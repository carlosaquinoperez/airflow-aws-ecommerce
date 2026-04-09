# terraform/catalog.tf

# 1. The Logical Database (A folder in the Catalog)
resource "aws_glue_catalog_database" "ecommerce_catalog" {
  name = "ecommerce_db"
}

# 2. The Crawler (The robot that scans S3)
resource "aws_glue_crawler" "silver_crawler" {
  database_name = aws_glue_catalog_database.ecommerce_catalog.name
  name          = "ecommerce_silver_crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.silver.bucket}/cleaned/orders/"
  }

  # This ensures the crawler only adds/updates metadata
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}