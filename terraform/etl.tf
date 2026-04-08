# terraform/etl.tf

# 1. Upload the PySpark script to S3
# The 'etag' ensures Terraform updates the file in S3 if you modify the code
resource "aws_s3_object" "pyspark_script" {
  bucket = aws_s3_bucket.bronze.bucket
  key    = "scripts/transform_sales_data.py"
  source = "../scripts/transform_sales_data.py"
  etag   = filemd5("../scripts/transform_sales_data.py")
}

# 2. IAM Role for AWS Glue (The Badge)
resource "aws_iam_role" "glue_role" {
  name = "ecommerce_glue_role_${random_pet.bucket_suffix.id}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Grant AWS Glue basic permissions and S3 full access (for portfolio scope)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# 3. Create the AWS Glue Job
resource "aws_glue_job" "bronze_to_silver" {
  name     = "ecommerce_bronze_to_silver_job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/transform_sales_data.py"
    python_version  = "3"
  }

  # These parameters are passed dynamically to our PySpark script
  default_arguments = {
    "--JOB_NAME"      = "ecommerce_bronze_to_silver_job"
    "--BRONZE_BUCKET" = aws_s3_bucket.bronze.bucket
    "--SILVER_BUCKET" = aws_s3_bucket.silver.bucket
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2 # Using 2 nodes for fast distributed processing
}