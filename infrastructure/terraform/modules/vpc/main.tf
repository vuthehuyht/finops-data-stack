# 1. VPC
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

# 2. Internet Gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

# 3. Subnets
# 2 Public Subnets
resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24"
  availability_zone       = var.availability_zones[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name                     = "${var.project_name}-public-subnet-${count.index + 1}"
    "kubernetes.io/role/elb" = "1" # For AWS Load Balancer Controller
  }
}

# 2 Private App Subnets (cho EKS Worker Nodes)
resource "aws_subnet" "private_app" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = var.availability_zones[count.index]

  tags = {
    Name                              = "${var.project_name}-private-app-${count.index + 1}"
    "kubernetes.io/role/internal-elb" = "1" # For internal load balancer
  }
}

# 2 Private DB Subnets (cho Redshift Serverless)
resource "aws_subnet" "private_db" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 20}.0/24"
  availability_zone = var.availability_zones[count.index]

  tags = {
    Name = "${var.project_name}-private-db-${count.index + 1}"
  }
}

# 4. NAT Instance (Single On-Demand t4g.nano, located in the first Public Subnet)
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.project_name}-nat-eip"
  }
}

data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# IAM Role and Profile for NAT Instance (Enables SSH-less debugging via AWS Systems Manager)
resource "aws_iam_role" "nat" {
  name = "${var.project_name}-nat-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "nat_ssm" {
  role       = aws_iam_role.nat.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "nat" {
  name = "${var.project_name}-nat-instance-profile"
  role = aws_iam_role.nat.name
}

resource "aws_instance" "nat" {
  ami                  = data.aws_ami.amazon_linux_2023.id
  instance_type        = "t4g.nano" # Cheap ARM64 On-Demand (~$3.35/month)
  subnet_id            = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.nat.id]
  iam_instance_profile = aws_iam_instance_profile.nat.name
  
  associate_public_ip_address = true
  source_dest_check           = false # Required for NAT router

  root_block_device {
    volume_size           = 8
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  # Script configuration with persistent state across reboots
  user_data = <<-EOF
              #!/bin/bash
              # 1. Enable IP forwarding permanently
              echo "net.ipv4.ip_forward = 1" | sudo tee -a /etc/sysctl.d/custom-ip-forward.conf
              sudo sysctl -p /etc/sysctl.d/custom-ip-forward.conf

              # 2. Install iptables-services to save configurations permanently
              sudo dnf install iptables-services -y
              sudo systemctl enable iptables
              sudo systemctl start iptables

              # 3. Setup NAT Masquerade on the default network interface
              DEFAULT_IFACE=$(ip -o -4 route show to default | awk '{print $5}')
              sudo iptables -t nat -A POSTROUTING -o $DEFAULT_IFACE -j MASQUERADE
              
              # 4. Save iptables rule so it reloads on boot
              sudo service iptables save
              EOF

  tags = {
    Name = "${var.project_name}-nat-instance"
  }
}

resource "aws_eip_association" "nat" {
  instance_id   = aws_instance.nat.id
  allocation_id = aws_eip.nat.id
}

# 5. Route Tables & Associations
# Public Route Table
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Private App Route Table (Routes Internet-bound traffic through the single NAT Instance)
resource "aws_route_table" "private_app" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block           = "0.0.0.0/0"
    network_interface_id = aws_instance.nat.primary_network_interface_id
  }

  tags = {
    Name = "${var.project_name}-private-app-rt"
  }
}

resource "aws_route_table_association" "private_app" {
  count          = 2
  subnet_id      = aws_subnet.private_app[count.index].id
  route_table_id = aws_route_table.private_app.id
}

# Private DB Route Table (No internet route - Completely isolated)
resource "aws_route_table" "private_db" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-private-db-rt"
  }
}

resource "aws_route_table_association" "private_db" {
  count          = 2
  subnet_id      = aws_subnet.private_db[count.index].id
  route_table_id = aws_route_table.private_db.id
}

# 6. VPC Gateway Endpoint for S3 (Saves NAT cost & accelerates transfers)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"

  # Associate S3 Endpoint with Public and Private App Route Tables
  route_table_ids = [
    aws_route_table.public.id,
    aws_route_table.private_app.id
  ]

  tags = {
    Name = "${var.project_name}-s3-endpoint"
  }
}

# Data source to fetch current region
data "aws_region" "current" {}

# 7. Security Groups
# SG cho EKS Worker Nodes
resource "aws_security_group" "eks_nodes" {
  name        = "${var.project_name}-eks-nodes-sg"
  description = "Security group for EKS worker nodes"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "${var.project_name}-eks-nodes-sg"
  }
}

# SG cho Redshift Serverless
resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-redshift-sg"
  description = "Security group for Redshift Serverless DWH"
  vpc_id      = aws_vpc.main.id

  # Only accept inbound port 5439 from EKS nodes
  ingress {
    description     = "Allow Redshift port 5439 from EKS nodes"
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_nodes.id]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "${var.project_name}-redshift-sg"
  }
}

# SG cho NAT Instance
resource "aws_security_group" "nat" {
  name        = "${var.project_name}-nat-sg"
  description = "Security group for NAT Instance router"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Allow inbound traffic from VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "${var.project_name}-nat-sg"
  }
}
