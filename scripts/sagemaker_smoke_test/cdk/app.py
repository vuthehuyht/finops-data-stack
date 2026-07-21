"""CDK app entrypoint for the SageMaker training smoke-test sandbox."""

import aws_cdk as cdk
from ml_sandbox_stack import MlSandboxStack

app = cdk.App()
MlSandboxStack(app, "FinopsMlSandboxStack")
app.synth()
