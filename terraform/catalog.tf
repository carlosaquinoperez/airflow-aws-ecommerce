# terraform/catalog.tf

# 1. The Logical Database (A folder in the Catalog)
resource "aws_glue_catalog_database" "ecommerce_catalog" {
  name = "ecommerce_db"
}