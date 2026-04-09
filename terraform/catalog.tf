# terraform/catalog.tf

# 1. The Logical Database (A folder in the Catalog)
resource "aws_glue_catalog_database" "ecommerce_catalog" {
  name = "ecommerce_db"
}

# 2. The Delta Table Definition
resource "aws_glue_catalog_table" "silver_orders" {
  name          = "silver_orders"
  database_name = aws_glue_catalog_database.ecommerce_catalog.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type" = "DELTA"
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.silver.bucket}/cleaned/orders/"
  }
}