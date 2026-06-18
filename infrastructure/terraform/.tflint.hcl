config {
  format = "compact"
}

# Kích hoạt plugin kiểm tra các quy tắc thiết kế hạ tầng trên AWS
plugin "aws" {
  enabled = true
  version = "0.47.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
