import dagster

print("DAGSTER FILE IS:", dagster.__file__)
import dagster_aws.s3  # noqa: F401
