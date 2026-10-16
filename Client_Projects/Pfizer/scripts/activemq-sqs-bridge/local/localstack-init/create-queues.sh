#!/bin/sh
# LocalStack init hook: creates the SQS queues used by the local test stack.
awslocal sqs create-queue --queue-name exception-in-dlq
awslocal sqs create-queue --queue-name exception-in \
  --attributes '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:exception-in-dlq\",\"maxReceiveCount\":\"5\"}"}'
awslocal sqs create-queue --queue-name exception-out-dlq
awslocal sqs create-queue --queue-name exception-out \
  --attributes '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:exception-out-dlq\",\"maxReceiveCount\":\"5\"}"}'
