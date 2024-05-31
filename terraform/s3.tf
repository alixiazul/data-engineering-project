resource "aws_s3_bucket" "extract_bucket" {
    bucket_prefix = var.injestion_bucket_prefix
}

resource "aws_s3_bucket" "test_extract_bucket" {
    bucket_prefix = var.test_extract_bucket_prefix
}

resource "aws_s3_object" "extract_lambda_code" {
    bucket = aws_s3_bucket.test_extract_bucket.id
    key = "code/extract_lambda.zip"
    source = "${path.module}/../src/extract_lambda/extract_lambda.zip"
}

resource "aws_s3_object" "layer_code" {
  bucket = aws_s3_bucket.test_extract_bucket.id
  key = "code/layer.zip"
  source = "${path.module}/../layer.zip"
}