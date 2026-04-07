# terraform/outputs.tf

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.bucket
}

output "airflow_access_key" {
  value = aws_iam_access_key.airflow_keys.id
  description = "Access Key for Airflow to connect to AWS"
}

output "airflow_secret_key" {
  value = aws_iam_access_key.airflow_keys.secret
  sensitive = true # Terraform oculta esto en la terminal por seguridad
  description = "Secret Key for Airflow to connect to AWS"
}