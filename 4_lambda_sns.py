import json
import boto3
import os

topic_arn = "arn:aws:sns:us-east-1:421741497942:sns_lambda_topic"
session = boto3.Session()
sns_client = session.client('sns')


def lambda_handler(event, context):
    print(event)
    
    body= json.loads(event['body'])
    
    endpoint_url = "https://" + event["requestContext"]["domainName"] + "/" + event["requestContext"]["stage"] 
    connectionId = event['requestContext']['connectionId']
    action = event.get('requestContext', {}).get("routeKey")
    
    message = json.dumps({
    "character": body['character'],
    "endpoint_url": endpoint_url,
    "connectionId": connectionId,
    "action": action
    
    }, ensure_ascii=False)
    
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message
    )

    return { 
            'statusCode': 200,    
            'body': json.dumps('Succeed')
            }
