terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~>4.5.0"
    }
  }
}
 
# Configure the AWS Provider
provider "aws" {
  region = "us-east-1"
}

variable "s3_bucket_regions" {
  type = list
  default = ["us-east-1", "us-east-2", "ap-south-1"]
}

resource "aws_s3_bucket" "dshahnaz_buckets" {
  count         = 10 //count will be 3
  bucket        = "dshahnaz-s3-bucket-${count.index}"
#   acl           = "public"
#   region        = var.s3_bucket_regions[count.index]
  force_destroy = true
}
