import json
import pdfkit
import boto3
import os
from boto3.dynamodb.conditions import Key, Attr

client = boto3.client('s3')
sqs = boto3.resource('sqs')
dynamodb = boto3.resource('dynamodb')
# Get the bucket name environment variable
sqs = boto3.client('sqs')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
queue_url = 'https://sqs.eu-west-1.amazonaws.com/396320213893/ArticlesQueue'
table = dynamodb.Table('articles')


def pdf_generate(event, context):

    res = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
    )

    article = []
    for message in res.get("Messages", []):
        message_body = message["Body"]
        article = json.loads(message_body)
    id_article = article['id']
    title = article['title']
    content = article['content']
    url = article['url']

    # Defaults
    key = id_article
    html = f"<html><head></head><body><h1>{title}</h1><br><p>{content}</p><br><a>{url}</a></body></html>"

    # Decode json and set values for pdf from API gateway event
    # if 'body' in event:
    #     data = json.loads(event['body'])
    #     key = data['filename']
    #     html = data['html']

    # Set file path to save pdf on lambda first (temporary storage)
    filepath = '/tmp/{key}'.format(key=key)

    # Create PDF
    config = pdfkit.configuration(wkhtmltopdf="binary/wkhtmltopdf")
    pdfkit.from_string(html, filepath, configuration=config, options={})

    # Upload to S3 Bucket
    r = client.put_object(
        ACL='public-read',
        Body=open(filepath, 'rb'),
        ContentType='application/pdf',
        Bucket=S3_BUCKET_NAME,
        Key=key
    )

    # # Format the PDF URI
    object_url = "https://{0}.s3.amazonaws.com/{1}".format(S3_BUCKET_NAME, key)

    # # Response with result
    response = {
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": True,
        },
        "statusCode": 200,
        "body": object_url
    }
    table.update_item(Key={'id': key},  UpdateExpression='SET title = :val1',
                      ExpressionAttributeValues={':val1': object_url})

    return response
