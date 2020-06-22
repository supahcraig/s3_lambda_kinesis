import boto3
import json
import logging
from ratelimiter import RateLimiter

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_RECORDS_PER_REQUEST = 500
MAX_REQUESTS_PER_SECOND = 2

def lambda_handler(event, context):
    bucket = 'cnelson-lambda-autoload'
    key = 'bat-splitaa'

    s3_client = boto3.client('s3')
    kinesis_client = boto3.client('kinesis')

    logging.info('fetching data from the bucket')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    rows = (line.decode('utf-8') for line in obj['Body'].iter_lines())
    logging.info('SUCCESS:  found the bucket.')

    column_names = ['playerID','yearID','stint','teamID','lgID',
                    'G','AB','R','H','2B','3B','HR','RBI','SB','CS',
                    'BB','SO','IBB','HBP','SH','SF','GIDP']

    recordset = []
    row_count = 0
    batch_count = 0
    for i, row in enumerate(rows):
        # turn raw csv data into json for future consumers
        message = {'Data': json.dumps(dict(zip(column_names, row))), 'PartitionKey': key}
        recordset.append(message)
        row_count += 1
        
        # batch put requests up 
        if i % MAX_RECORDS_PER_REQUEST == 0:
            batch_count += 1
            put_records(kinesis_client=kinesis_client, stream_name='test_kinesis_stream', records=recordset)
            logging.info(f'SUCCESS:  published {i} records to kinesis')
            recordset = []

    logging.info(f'{row_count} records processed in {batch_count} batches.')

# 1 kinesis shard can ingest at most 1000 records per second
# we ratelimit to ensure we do not go over that rate
@RateLimiter(max_calls=MAX_REQUESTS_PER_SECOND, period=1)
def put_records(kinesis_client, stream_name, records):
    response = kinesis_client.put_records(StreamName=stream_name, Records=records)
    
    if response['FailedRecordCount'] > 0:
        logging.info(response)
    else:
        logging.info("Batch put successful.")

