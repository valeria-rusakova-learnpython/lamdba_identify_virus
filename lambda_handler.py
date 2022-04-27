import json
import boto3
import urllib.parse
import re


s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
virus_bucket = s3.Bucket('vr-virus-bucket')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    copy_source = {
        'Bucket': bucket,
        'Key': key
    }

    file_obj = s3.Object(bucket, key)
    file_data = file_obj.get()['Body'].read().decode('utf-8')
    file_data_list = re.split(', |\n', file_data)

    table = dynamodb.Table('test_db')
    table_items_values = table.scan(ProjectionExpression='stop_word')["Items"]
    stop_word_list = [list(y.values()) for y in table_items_values]

    search_stop_word = []
    for item in stop_word_list:
        search_stop_word.append(' '.join(item))

    if any(x in file_data_list for x in search_stop_word):
        virus_bucket.copy(copy_source, key)

    return {
        'statusCode': 200,
        'body': json.dumps(search_stop_word)
    }
