import aws_cdk as core
import aws_cdk.assertions as assertions

from emr_serverless_with_mwaa.emr_serverless_with_mwaa_stack import EmrServerlessWithMwaaStack

# example tests. To run these tests, uncomment this file along with the example
# resource in emr_serverless_with_mwaa/emr_serverless_with_mwaa_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EmrServerlessWithMwaaStack(app, "emr-serverless-with-mwaa")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
