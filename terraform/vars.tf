variable "injestion_bucket_prefix" {
  type = string
  default = "extract-bucket-"
}

variable "test_extract_bucket_prefix" {
  type = string
  default = "test-extract-bucket-"
}

variable "lambda_ingestion_handler" {
  type = string
  default = "extract_lambda_handler"
}

variable "lambda_processed_handler" {
  type = string
  
  default = "lambda-processed-handler"
}

variable "lambda_load_handler" {
  type = string
  default = "lambda-load-handler"
}

variable "processed_bucket_prefix" {
  type = string
  default = "processed-bucket"
}

variable "lambda_code_bucket_prefix" {
  type = string
  default = "lambda_code-bucket"
}

variable "region"{
    type=string
    default = "eu-west-2"
}

variable "python_version" {
    type = string
    default = "python3.11"
}