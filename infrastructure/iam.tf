# create a dedicated user for airflow instance
resource "aws_iam_user" "airflow_user" {
  name = "airflow"

  tags = {
    service     = "airflow"
    environment = "dev"
  }
}

# Create aws access key and secret key for airflow to programatically interact with AWS
resource "aws_iam_access_key" "airflow_credentials" {
  user = aws_iam_user.airflow_user.name
}

# saving the output of the access key into AWS SSM parameters store
resource "aws_ssm_parameter" "airflow_access_key" {
  name  = "/dev/airflow/access_key"
  type  = "String"
  value = aws_iam_access_key.airflow_credentials.id
}

# saving the output of the secret key into AWS SSM parameters store
resource "aws_ssm_parameter" "airflow_secret_key" {
  name  = "/dev/airflow/secret_key"
  type  = "String"
  value = aws_iam_access_key.airflow_credentials.secret
}

# defining AWS IAM policy which highlight what airflow user can do and cant in AWS
resource "aws_iam_policy" "airflow_policy" {
  name        = "airflow-policy"
  description = "Dedicated policy for airflow instance "

  # read and write permission 
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:*Object*",
        ]
        Resource = [
          "arn:aws:s3:::random-user-extraction",
          "arn:aws:s3:::random-user-extraction/*"
        ]
      },
    ]
  })
}

resource "aws_iam_user_policy_attachment" "test-attach" {
  user       = aws_iam_user.airflow_user.name
  policy_arn = aws_iam_policy.airflow_policy.arn
}
