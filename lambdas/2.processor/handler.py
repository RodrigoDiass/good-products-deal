from pydantic import BaseModel, Field, ValidationError
import boto3
import json
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Product(BaseModel): #Data Contract
    id: int
    title: str
    description: str
    price: float = Field(gt=0)  # price must be greater than 0
    discountPercentage: float = Field(ge=0, le=100)  # discount between 0 and 100
    rating: float = Field(ge=0, le=5)  # rating between 0 and 5
    stock: int = Field(ge=0)  # stock cannot be negative
    brand: str # brand can be optional? Understand with business team, coming null from item 16 from beyond.
    category: str
    thumbnail: str

class ProductsResponse(BaseModel):  
    products: list[Product]


def handler(event, context):
    current_time = datetime.now().strftime('%Y-%m-%dT%H%M%S')
    s3 = boto3.client('s3')

    
    try:
        try:
            bucket = event.get('bucket')
            key = event.get('key')
            filename = key.split('/')[-1]
            key_validate=f"rodrigo-products-data-structured/processed/{filename}"
            key_failed=f"rodrigo-products-data-structured/failed/{filename}"
            response = s3.get_object(Bucket=bucket, Key=key)
            raw_data = json.loads(response['Body'].read())
        except Exception as e:
            logger.error(json.dumps({
                "status": "error",
                "error": f"Failed to parse event: {str(e)}",
                "timestamp": current_time
            }))
            raise

        validated_products = []
        failed_products = []

        for product_data in raw_data.get('products', []):
            try:
                validated_product = Product(**product_data)
                validated_products.append(validated_product.model_dump())
            except ValidationError as e:
                logger.warning(json.dumps({
                    "status": "validation_failed",
                    "product_id": product_data.get('id', 'unknown'),
                    "reason": e.errors() 
                }))
                failed_products.append({
                    'data': product_data, 
                    'error': e.errors()})

        if validated_products:
            s3.put_object(
                Bucket=bucket,
                Key=key_validate,
                Body=json.dumps({'products': validated_products})
            )

        if failed_products:
            s3.put_object(
                Bucket=bucket,
                Key=key_failed,
                Body=json.dumps({'failed_products': failed_products})
            )
        
        total = len(raw_data.get('products', []))
        validated_count = len(validated_products)
        failed_count = len(failed_products)
        failure_rate_calc = (failed_count / total * 100) if total > 0 else 0
        failure_rate = round(failure_rate_calc, 2)


        logger.info(json.dumps({
            "status": "success",
            "source_key": key_validate,  
            "total": total,
            "validated": validated_count,
            "failed": failed_count,
            "failure_rate": failure_rate,
            "timestamp": current_time
            }))
        
        return {
            "statusCode": 200,
            "message": "Data processed successfully",
            "bucket": bucket,
            "key": key_validate,
            "validated_count": validated_count,
            "failed_count": failed_count,
            "failure_rate": failure_rate,
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
    test_event = {
        "bucket": "rodrigo-products-data",
        "key": "rodrigo-products-data-raw/products-2026-01-24T223451.json",
        "timestamp": "2026-01-24T223451"}
    result = handler(test_event, None)
    print(result)