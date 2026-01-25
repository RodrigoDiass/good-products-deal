import boto3
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
s3 = boto3.client('s3')

def analyze_product(product: dict) -> dict:
    current_time = datetime.now().strftime('%Y-%m-%dT%H%M%S')
    try:    
        prompt = f"""Analyze this product and return ONLY a JSON object, no other text:
                    {json.dumps(product)}

                    Return exactly this format:
                    {{"is_good_deal": true or false, "price_category": "budget" or "mid-range" or "premium", "confidence": number 0-1}}"""


        response = bedrock.invoke_model(
            modelId="amazon.nova-lite-v1:0",
            body=json.dumps({
                "messages": [
                    {"role": "user", "content": [{"text": prompt}]}
                ],
                "inferenceConfig": {
                    "temperature": 0.7,
                    "maxTokens": 100
                }
            })
        )

        result = json.loads(response['body'].read())
        text = result['output']['message']['content'][0]['text']
        text_clean = text.replace("```json", "").replace("```", "").strip()
        result_json = json.loads(text_clean)
            
        logger.info(json.dumps({
            "status": "success",
            "response": result,
            "timestamp": current_time
        }))
        return result_json   
    except Exception as e:
        logger.error(json.dumps({
            "status": "error",
            "error": str(e), 
            "timestamp": current_time
        }))
        raise
    
def handler(event, context):
    bucket = event.get("bucket")
    key = event.get("key")
    

    response = s3.get_object(Bucket=bucket, Key=key)
    raw_data = json.loads(response['Body'].read())
    products = raw_data.get('products', [])

    output = []
    for product in products:
        try:
            analysis = analyze_product(product)  # chama a função auxiliar
            logger.info(json.dumps({
            "event": "product_analyzed",
            "id": product.get("id"),
            "result": "success"
            }))
            output.append({**product, **analysis})
        except Exception as e:
            logger.info(json.dumps({
                "event": "product_analysis_failed",
                "id": product.get("id"),
                "result": "failed",
                "error": str(e)
                }))
            output.append({**product, "error": "analysis_failed"})
    s3.put_object( 
        Bucket=bucket,
        Key=f'rodrigo-products-data-analyzed/analyzed/product-{datetime.now().strftime("%Y-%m-%dT%H%M%S")}.json',
        Body=json.dumps({"products": output})
        )
    
    return {"statusCode": 200, "body": json.dumps({"message": "Analysis completed."})}

if __name__ == "__main__": 
    sample_event = {
        "bucket": "rodrigo-products-data",
        "key": "rodrigo-products-data-structured/processed/products-2026-01-25T022611.json"
    }
    handler(sample_event, None)