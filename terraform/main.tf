# terraform/main.tf

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# Genera un sufijo aleatorio para evitar colisiones de nombres
resource "random_pet" "bucket_suffix" {
  length    = 2
  separator = "-"
}

# ------------------------------------------------------------------------------
# MEDALLION ARCHITECTURE BUCKETS
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket = "ecommerce-bronze-${random_pet.bucket_suffix.id}"
  force_destroy = true # Solo para desarrollo: permite borrar el bucket aunque tenga datos
  tags = { Layer = "Bronze", Environment = "Dev" }
}

resource "aws_s3_bucket" "silver" {
  bucket = "ecommerce-silver-${random_pet.bucket_suffix.id}"
  force_destroy = true
  tags = { Layer = "Silver", Environment = "Dev" }
}

resource "aws_s3_bucket" "gold" {
  bucket = "ecommerce-gold-${random_pet.bucket_suffix.id}"
  force_destroy = true
  tags = { Layer = "Gold", Environment = "Dev" }
}