import json
import boto3
import os
from datetime import datetime
from urllib.parse import unquote_plus
from requests_aws4auth import AWS4Auth
import requests

# in index-photos folder run and upload to console zip -r deployment-package.zip .

s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition')

OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT', '')
OPENSEARCH_INDEX = 'photos'
AWS_REGION = 'us-east-1'
SERVICE = 'es'

def lambda_handler(event, context):
    """
    Lambda function to index photos uploaded to S3 OR query OpenSearch.
    
    For S3 events: Index photos uploaded to S3
    For manual invocation: Query OpenSearch (use event['action'] = 'query')
    """
    
    # Check if this is a query request (manual invocation)
    if event.get('action') == 'query':
        return handle_query(event, context)
    
    # Otherwise, handle S3 indexing (original functionality)
    return handle_s3_indexing(event, context)

def handle_query(event, context):
    """Handle manual query requests to OpenSearch"""
    if not OPENSEARCH_ENDPOINT:
        return {
            'statusCode': 400,
            'body': json.dumps('OpenSearch endpoint not configured')
        }
    
    try:
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            AWS_REGION,
            SERVICE,
            session_token=credentials.token
        )
        
        query_type = event.get('queryType', 'count')
        
        if query_type == 'count':
            url = f"{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_count"
            response = requests.get(url, auth=awsauth)
        elif query_type == 'all':
            url = f"{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_search"
            query = {"query": {"match_all": {}}, "size": 10}
            response = requests.post(url, auth=awsauth, json=query, headers={'Content-Type': 'application/json'})
        else:
            return {'statusCode': 400, 'body': json.dumps('Invalid queryType')}
        
        return {
            'statusCode': 200,
            'body': json.dumps(response.json() if response.status_code == 200 else response.text)
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f'Query error: {str(e)}')}

def handle_s3_indexing(event, context):
    """
    Handle S3 PUT events for photo indexing.
    
    Process:
    1. Extract S3 event details (bucket, key)
    2. Use Rekognition to detect labels
    3. Retrieve custom labels from S3 object metadata
    4. Index photo metadata in OpenSearch
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Parse S3 event
        for record in event['Records']:
            # Get bucket and object key from event
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            
            print(f"Processing image: s3://{bucket}/{key}")
            
            # Step 1: Detect labels using Rekognition
            labels = detect_labels(bucket, key)
            print(f"Detected labels from Rekognition: {labels}")
            
            # Step 2: Get custom labels from S3 metadata
            custom_labels = get_custom_labels(bucket, key)
            print(f"Custom labels from metadata: {custom_labels}")
            
            # Step 3: Combine all labels
            all_labels = labels + custom_labels
            print(f"Combined labels: {all_labels}")
            
            # Step 4: Create photo document for indexing
            photo_document = {
                'objectKey': key,
                'bucket': bucket,
                'createdTimestamp': datetime.now().isoformat(),
                'labels': all_labels
            }
            
            print(f"Photo document prepared: {json.dumps(photo_document)}")
            
            # Step 5: Index in OpenSearch
            if OPENSEARCH_ENDPOINT:
                index_photo(photo_document)
                print(f"Photo indexed in OpenSearch: {key}")
            else:
                print("OpenSearch endpoint not configured.")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Photo(s) processed successfully')
        }
        
    except Exception as e:
        print(f"Error processing image: {str(e)}")
        raise e


def detect_labels(bucket, key):
    """
    Use AWS Rekognition to detect labels in the image.
    
    Args:
        bucket (str): S3 bucket name
        key (str): S3 object key
        
    Returns:
        list: List of detected label names with confidence > 70%
    """
    try:
        response = rekognition_client.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            MaxLabels=10,
            MinConfidence=70.0
        )
        
        # Extract label names from response
        labels = [label['Name'] for label in response['Labels']]
        return labels
        
    except Exception as e:
        print(f"Error detecting labels with Rekognition: {str(e)}")
        return []


def get_custom_labels(bucket, key):
    """
    Retrieve custom labels from S3 object metadata.
    
    Args:
        bucket (str): S3 bucket name
        key (str): S3 object key
        
    Returns:
        list: List of custom labels from metadata
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        metadata = response.get('Metadata', {})
        
        # Look for x-amz-meta-customLabels in metadata
        custom_labels_str = metadata.get('customlabels', '')
        
        if custom_labels_str:
            # Split comma-separated labels and strip whitespace
            custom_labels = [label.strip() for label in custom_labels_str.split(',')]
            return custom_labels
        
        return []
        
    except Exception as e:
        print(f"Error retrieving custom labels from S3 metadata: {str(e)}")
        return []


def get_photo_hash(bucket, key):
    """
    Generate content hash for duplicate detection.
    Uses first 1KB of file for performance.
    """
    import hashlib
    try:
        # Read first 1KB for hash (faster than full file)
        response = s3_client.get_object(
            Bucket=bucket, 
            Key=key, 
            Range='bytes=0-1023'
        )
        content = response['Body'].read()
        return hashlib.md5(content).hexdigest()[:12]  # 12-char hash
    except Exception as e:
        print(f"Error generating hash: {str(e)}")
        # Fallback to filename-based ID if hash fails
        return key.replace('/', '_').replace('.', '_')


def index_photo(photo_document):
    """
    Index photo document in OpenSearch.
    Uses content hash to prevent duplicate photos.
    
    Args:
        photo_document (dict): Photo metadata to index
    """
    if not OPENSEARCH_ENDPOINT:
        print("OpenSearch endpoint not configured")
        return
    
    try:
        # Get AWS credentials from Lambda execution role
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            AWS_REGION,
            SERVICE,
            session_token=credentials.token
        )
        
        # Use content hash as document ID to prevent duplicate photos
        content_hash = get_photo_hash(photo_document['bucket'], photo_document['objectKey'])
        document_id = f"photo_{content_hash}"
        
        # Check if this photo content already exists
        check_url = f"{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_doc/{document_id}"
        check_response = requests.head(check_url, auth=awsauth)
        
        if check_response.status_code == 200:
            print(f"Duplicate photo detected! Content hash: {content_hash}")
            print(f"Photo {photo_document['objectKey']} is identical to existing photo")
            return False  # Don't index duplicates
        
        url = f"{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_doc/{document_id}"
        
        headers = {'Content-Type': 'application/json'}
        
        # Index the document (will update if exists)
        response = requests.put(
            url,
            auth=awsauth,
            json=photo_document,
            headers=headers
        )
        
        print(f"OpenSearch response: {response.status_code}")
        print(f"Response body: {response.text}")
        
        if response.status_code in [200, 201]:
            print(f"New photo indexed with content hash: {content_hash}")
            return True
        else:
            print(f"Failed to index photo: {response.status_code}")
            return False
        
    except Exception as e:
        print(f"Error indexing photo in OpenSearch: {str(e)}")
        return False
