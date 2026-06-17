output "vpc_id" {
  value       = aws_vpc.main.id
  description = "The ID of the VPC"
}

output "public_subnet_ids" {
  value       = aws_subnet.public[*].id
  description = "List of IDs of public subnets"
}

output "private_app_subnet_ids" {
  value       = aws_subnet.private_app[*].id
  description = "List of IDs of private app subnets"
}

output "private_db_subnet_ids" {
  value       = aws_subnet.private_db[*].id
  description = "List of IDs of private database subnets"
}

output "eks_node_sg_id" {
  value       = aws_security_group.eks_nodes.id
  description = "The ID of the EKS nodes Security Group"
}

output "redshift_sg_id" {
  value       = aws_security_group.redshift.id
  description = "The ID of the Redshift Security Group"
}
