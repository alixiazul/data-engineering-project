data "archive_file" "extract_lambda" {
  type        = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/extract_lambda/extract_lambda.py"
  output_path = "${path.module}/../src/extract_lambda/extract_lambda.zip"
}

data "archive_file" "extract_layer" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "${path.module}/../layer/"
  output_path = "${path.module}/../layer.zip"
}


resource "aws_lambda_function" "extract_lambda" {
    function_name = "${var.lambda_ingestion_handler}"
    #s3_bucket = aws_s3_bucket.extract_bucket.bucket
    #s3_bucket = aws_s3_bucket.test_extract_bucket.bucket
    #s3_key = "code/extract_lambda.zip"
    role = aws_iam_role.iam_for_lambda.arn
    handler = "extract_lambda.extract_lambda_handler"
    runtime = var.python_version
    layers = [aws_lambda_layer_version.libraries_layer.arn]
    filename = data.archive_file.extract_lambda.output_path
    source_code_hash = data.archive_file.extract_lambda.output_base64sha256

    environment {
      variables = {
        # S3_BUCKET_NAME = aws_s3_bucket.extract_bucket.bucket
        S3_BUCKET_NAME = aws_s3_bucket.test_extract_bucket.bucket
      }
    }
}

# resource "aws_lambda_permission" "allow_eventbridge" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.extract_lambda.function_name
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.scheduler.arn
#   source_account = data.aws_caller_identity.current.account_id
# }

resource "aws_lambda_layer_version" "libraries_layer" {
  layer_name = "libraries_layer"
  compatible_runtimes = [var.python_version]
  #s3_bucket = aws_s3_bucket.test_extract_bucket.bucket
  #s3_bucket = aws_s3_bucket.extract_bucket.bucket
  #s3_key = "code/layer.zip"
  depends_on = [ aws_s3_object.layer_code ]
  filename = data.archive_file.extract_layer.output_path

}
