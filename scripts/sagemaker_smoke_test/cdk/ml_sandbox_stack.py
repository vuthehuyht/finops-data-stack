"""Disposable CDK sandbox for the SageMaker training smoke test.

Provisions exactly one S3 bucket and one IAM role — nothing else. Separate
from infrastructure/terraform/ on purpose: this is a throwaway sandbox for
a manual smoke test, not part of the production stack.
"""

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct


class MlSandboxStack(Stack):
    """S3 bucket + SageMaker execution role for the training smoke test."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(
            self,
            "MlSandboxArtifactsBucket",
            bucket_name="finops-ml-sandbox-artifacts",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        execution_role = iam.Role(
            self,
            "SageMakerExecutionRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
        )
        bucket.grant_read_write(execution_role)
        execution_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess")
        )
        execution_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonEC2ContainerRegistryReadOnly"
            )
        )

        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "ExecutionRoleArn", value=execution_role.role_arn)
