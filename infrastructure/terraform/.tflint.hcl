config {
  format = "compact"
}

# Enable AWS ruleset plugin to check infrastructure design rules on AWS
plugin "aws" {
  enabled = true
  version = "0.47.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
