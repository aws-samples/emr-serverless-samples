import aws_cdk as core
import aws_cdk.assertions as assertions

from stacks import EmrServerlessWithSfnStack

# example tests. To run these tests, uncomment this file along with the example
# resource in emr_serverless_with_sfn/emr_serverless_with_sfn_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EmrServerlessWithSfnStack(app, "emr-serverless-with-sfn")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
