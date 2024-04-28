# Terraform script to provision an s3 bucket in aws

resource "aws_s3_bucket" "random_user_bucket" {
  bucket = "random-user-extraction"

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}
