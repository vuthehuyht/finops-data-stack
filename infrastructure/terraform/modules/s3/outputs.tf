output "raw_bucket_id" {
  value       = aws_s3_bucket.raw.id
  description = "The ID/Name of the Raw S3 bucket"
}

output "processed_bucket_id" {
  value       = aws_s3_bucket.processed.id
  description = "The ID/Name of the Processed S3 bucket"
}

output "model_artifacts_bucket_id" {
  value       = aws_s3_bucket.model_artifacts.id
  description = "The ID/Name of the Model Artifacts S3 bucket"
}
