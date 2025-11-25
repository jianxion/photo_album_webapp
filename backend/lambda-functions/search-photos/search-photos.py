import json
import boto3
import os
import requests
from requests_aws4auth import AWS4Auth


OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT', '')
OPENSEARCH_INDEX = 'photos'
SERVICE = 'es'
AWS_REGION = 'us-east-1'
STOP_WORDS = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'show', 'me', 'find', 'search', 'photos', 'pictures', 'images'}

def normalize_plural(word):
    """Convert plural words to singular form"""
    if len(word) <= 3:
        return word
    
    # Handle common plural patterns
    if word.endswith('ies') and len(word) > 4:
        return word[:-3] + 'y'  # puppies -> puppy, berries -> berry
    elif word.endswith('es') and len(word) > 3:
        if word.endswith(('ches', 'shes', 'xes', 'zes', 'sses')):
            return word[:-2]  # beaches -> beach, boxes -> box
    elif word.endswith('s') and len(word) > 3:
        return word[:-1]  # dogs -> dog, cats -> cat, beasts -> beast
    
    return word

def extract_keywords_from_text(text):
    """Extract keywords from text by removing stop words and normalizing plurals"""
    words = text.lower().split()
    keywords = [word.strip('.,!?') for word in words if word not in STOP_WORDS and len(word) > 2]
    # Normalize plurals to singular
    return [normalize_plural(kw) for kw in keywords]


def search_photos_in_opensearch(keywords):
    """
    Search OpenSearch for photos with matching labels
    
    Args:
        keywords: List of keywords to search for
        
    Returns:
        List of photo objects with url and labels
    """
    if not keywords:
        return []
    
    if not OPENSEARCH_ENDPOINT:
        print("OpenSearch endpoint not configured")
        return []
    
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            AWS_REGION,
            SERVICE,
            session_token=credentials.token
        )
        
        # Build OpenSearch query
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"labels": keyword}} for keyword in keywords
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 50  # Maximum number of results
        }
        
        # Make request to OpenSearch
        url = f"{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_search"
        headers = {"Content-Type": "application/json"}
        
        print(f"Searching OpenSearch with query: {json.dumps(query)}")
        response = requests.post(url, auth=awsauth, headers=headers, json=query)
        response.raise_for_status()
        
        results_data = response.json()
        print(f"OpenSearch response: {json.dumps(results_data)}")
        
        # Parse results
        results = []
        hits = results_data.get('hits', {}).get('hits', [])
        
        for hit in hits:
            source = hit['_source']
            bucket = source.get('bucket', '')
            object_key = source.get('objectKey', '')
            
            s3_url = f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{object_key}"
            
            results.append({
                'url': s3_url,
                'labels': source.get('labels', [])
            })
        
        print(f"Found {len(results)} photos")
        return results
        
    except Exception as e:
        print(f"Error searching OpenSearch: {e}")
        return []
    

def lambda_handler(event, context):
    """
    LF2: Search photos Lambda function
    
    This function can be called:
    1. From API Gateway (GET /search?q=query) - calls Lex then searches
    2. From Lex fulfillment (processes Lex response)
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    # Check if this is coming from Lex (fulfillment)
    if 'sessionState' in event and 'intent' in event.get('sessionState', {}):
        return handle_lex_fulfillment(event, context)
    
    # Otherwise, this is from API Gateway - process user query
    return handle_api_search(event, context)


def handle_api_search(event, context):
    """Handle search request from API Gateway"""
    try:
        # Extract query from API Gateway event
        query_params = event.get('queryStringParameters') or {}
        user_query = query_params.get('q', '').strip()
        
        if not user_query:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'error': 'Query parameter q is required'
                })
            }
        
        print(f"User query: {user_query}")
        
        # Send query to Lex for processing
        lex_response = query_lex_bot(user_query)
        
        if not lex_response:
            # Fallback: extract keywords directly from query
            print("Lex response is empty, using direct keyword extraction")
            keywords = extract_keywords_from_text(user_query)
        else:
            # Extract keywords from Lex response
            keywords = extract_keywords_from_lex_response(lex_response)
            print(f"Extracted keywords from Lex: {keywords}")
        
        # If still no keywords, return empty results
        if not keywords:
            print("No keywords extracted from query")
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({
                    'query': user_query,
                    'keywords': [],
                    'results': []
                })
            }

        search_results = search_photos_in_opensearch(keywords)


        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'query': user_query,
                'keywords': keywords,
                'results': search_results
            })
        }
    except Exception as e:
        print(f"Error in handle_api_search: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': str(e)
            })
        }


def extract_keywords_from_lex_response(lex_response):
    """Extract keywords from Lex response"""
    try:
        if 'sessionState' in lex_response and 'intent' in lex_response['sessionState']:
            slots = lex_response['sessionState']['intent'].get('slots', {})
            keywords = extract_keywords_from_slots(slots)
            
            if keywords:
                return keywords
            
    except Exception as e:
        print(f"Error extracting keywords from Lex response: {str(e)}")
    
    return []



def query_lex_bot(user_input):
    """Send user query to Lex bot"""
    try:
        lex_client = boto3.client('lexv2-runtime', region_name=AWS_REGION)
        
        bot_id = os.environ.get('LEX_BOT_ID', '')
        bot_alias_id = os.environ.get('LEX_BOT_ALIAS_ID', 'TSTALIASID')
        locale_id = 'en_US'
        session_id = 'search-session'
        
        if not bot_id:
            print("LEX_BOT_ID not configured, skipping Lex")
            return None
        
        response = lex_client.recognize_text(
            botId=bot_id,
            botAliasId=bot_alias_id,
            localeId=locale_id,
            sessionId=session_id,
            text=user_input
        )
        
        print(f"Lex response: {json.dumps(response)}")
        return response
        
    except Exception as e:
        print(f"Error querying Lex: {str(e)}")
        return None

def text_message(content):
    return {"contentType": "PlainText", "content": content}

def response(session_state, messages=None, session_attributes=None):
    body = {"sessionState": session_state}
    if session_attributes:
        body["sessionState"]["sessionAttributes"] = session_attributes
    if messages:
        body["messages"] = messages
    return body

def close(event, fulfillment_state, message, session_attributes=None):
    intent = event["sessionState"]["intent"]
    intent["state"] = fulfillment_state  # "Fulfilled" | "Failed"
    return response(
        session_state={
            "dialogAction": {"type": "Close"},
            "intent": intent
        },
        messages=[text_message(message)],
        session_attributes=session_attributes
    )

def handle_lex_fulfillment(event, context):
    print(f"Received event: {json.dumps(event)}")
    try:
        # Extract keywords from Lex response
        intent_name = event['sessionState']['intent']['name']
        slots = event['sessionState']['intent']['slots']
        input_transcript = event.get('inputTranscript', '')

        if intent_name == 'FallbackIntent':
            return close(event, "Fulfilled", "Sorry, I didn’t quite get that.")
        
        print(f"Intent: {intent_name}, Input: {input_transcript}, Slots: {slots}")

        keywords = extract_keywords_from_slots(slots)

        print(f"Extracted keywords: {keywords}")

        return close(event, "Fulfilled", f"Searching for photos with keywords: {', '.join(keywords)}", session_attributes={
            'results': json.dumps(keywords)
        })
    except Exception as e:
        return close(event, "Failed", f"Error processing request: {str(e)}")

def extract_keywords_from_slots(slots):
    """Extract keywords from Lex slots"""
    keywords = []
    for slot_name, slot_value in slots.items():
        if slot_value and 'value' in slot_value:
            interpreted_value = slot_value['value'].get('interpretedValue', '')
            if interpreted_value:
                keywords.extend(extract_keywords_from_text(interpreted_value))
    return keywords