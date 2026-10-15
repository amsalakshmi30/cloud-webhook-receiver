"""Thin SQS wrapper so the handlers can be tested with a fake queue."""


class SqsQueue:
    def __init__(self, queue_url, client=None):
        if client is None:
            import boto3  # imported lazily; boto3 ships with the Lambda Python runtime

            client = boto3.client("sqs")
        self.client = client
        self.queue_url = queue_url

    def send(self, body):
        self.client.send_message(QueueUrl=self.queue_url, MessageBody=body)
