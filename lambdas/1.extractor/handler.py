import requests
import boto3
import json
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    current_time = datetime.now().strftime('%Y-%m-%dT%H%M%S')
    bucket= "rodrigo-products-data"
    key= f"rodrigo-products-data-raw/products-{current_time}.json"
    
    try:
        url = "https://dummyjson.com/products"
        response = requests.get(url)
        data = response.json()

        client = boto3.client('s3')
        client.put_object(
            Bucket= bucket,
            Key= key,
            Body= json.dumps(data)  
        )

        logger.info(json.dumps({
            "status": "success",
            "products_count": len(data['products']),
            "timestamp": current_time
        }))

        return {
            "statusCode": 200,
            "message": "success",
            "products_count": len(data['products']),
            "bucket": bucket,
            "key": key,
            "timestamp": current_time
            }
    except Exception as e:
        logger.error(json.dumps({
            "status": "error",
            "error": str(e), 
            "timestamp": current_time
        }))
        raise

if __name__ == "__main__":
    result = handler({}, None)
    print(result)