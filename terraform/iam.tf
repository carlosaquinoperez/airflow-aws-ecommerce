# terraform/iam.tf

# Creamos un usuario de sistema para Airflow
resource "aws_iam_user" "airflow_service_user" {
  name = "airflow_ecommerce_bot"
  tags = { Purpose = "Airflow Automation" }
}

# Le damos permisos para leer/escribir en S3 y ejecutar trabajos de Glue
resource "aws_iam_user_policy" "airflow_aws_access" {
  name = "airflow_aws_access_policy"
  user = aws_iam_user.airflow_service_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns"
        ]
        Effect   = "Allow"
        Resource = "*" # En prod esto se restringe a ARNs específicos
      }
    ]
  })
}

# Generamos las llaves de acceso para Airflow
resource "aws_iam_access_key" "airflow_keys" {
  user = aws_iam_user.airflow_service_user.name
}