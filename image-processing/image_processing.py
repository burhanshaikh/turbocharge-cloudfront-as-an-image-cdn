import os
import time
import io
import base64
import logging
import boto3
from botocore.config import Config
from PIL import Image, ImageOps # Pillow library for image processing. 11.3.0 supports AVIF natively.

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the S3 clients with MRAP support
s3_control_client = boto3.client('s3control')
# Default S3 client for non-MRAP operations
s3Client = boto3.client('s3')
# Configuration for MRAP clients with SigV4
mrap_config = Config(
    signature_version='s3v4',
    s3={'use_arn_region': False}  # Required for MRAP
)

# Get bucket names and MRAP ARNs from environment variables
S3_ORIGINAL_IMAGE_BUCKET = os.environ.get('originalImageBucketName')
S3_TRANSFORMED_IMAGE_BUCKET = os.environ.get('transformedImageBucketName')
S3_ORIGINAL_MRAP_ARN = os.environ.get('originalBucketMRAPArn')
S3_TRANSFORMED_MRAP_ARN = os.environ.get('transformedBucketMRAPArn')
TRANSFORMED_IMAGE_CACHE_TTL = os.environ.get('transformedImageCacheTTL')
TRANSFORM_REGION = os.environ.get('transformedRegion')
DEFAULT_IMAGE_QUALITY = int(os.environ.get('defaultImageQuality', 75))


def get_mrap_alias(mrap_arn):
    """Extract the MRAP alias from the ARN"""
    return mrap_arn.split('/')[-1]

def get_mrap_connection_string(mrap_arn):
    """Generate the MRAP connection string"""
    mrap_alias = get_mrap_alias(mrap_arn)
    return f'{mrap_alias}.accesspoint.s3-global'


# Initialize S3 clients with MRAP endpoints if MRAP ARNs are provided
if S3_ORIGINAL_MRAP_ARN and S3_TRANSFORMED_MRAP_ARN:
    # Use the current region of the lambda function
    current_region = os.environ.get('AWS_REGION')

    # Create separate clients for original and transformed buckets
    original_bucket_s3_client = boto3.client('s3',
        endpoint_url=f'https://{get_mrap_connection_string(S3_ORIGINAL_MRAP_ARN)}.amazonaws.com',
        region_name=current_region,
        config=mrap_config
    )

    transformed_bucket_s3_client = boto3.client('s3',
        endpoint_url=f'https://{get_mrap_connection_string(S3_TRANSFORMED_MRAP_ARN)}.amazonaws.com',
        region_name=current_region,
        config=mrap_config
    )
    
    logger.info('Using MRAP endpoints with SigV4 authentication')
else:
    # Fallback to regular S3 clients if MRAP ARNs are not provided
    original_bucket_s3_client = s3Client
    transformed_bucket_s3_client = s3Client
    logger.info('Using standard S3 endpoints (non-MRAP)')

def handler(event, context=None):
    # Validate if this is a GET request
    if not event.get("requestContext") or not event["requestContext"].get("http") or not (event["requestContext"]["http"].get("method") == 'GET'):
        return sendError(400, 'Only GET method is supported', event)
    
    # Example expected path:
    # /images/rio/1.jpeg/format=jpeg,width=100 or /images/rio/1.png/format=jpeg,width=100
    # where /images/rio/1.jpeg is the key of the original image in S3.
    imagePathArray = event["requestContext"]["http"]["path"].split('/')
    # The last element is the operations (e.g., format, width, etc.)
    operationsPrefix = imagePathArray.pop()
    # Remove the leading empty element (if the path starts with a slash)
    if imagePathArray[0] == "":
        imagePathArray.pop(0)
    # The remaining elements form the original image path
    originalImagePath = '/'.join(imagePathArray)
    
    startTime = time.perf_counter() * 1000
    # Download the original image from S3
    try:
        # Determine the bucket/ARN to use for fetching
        fetch_bucket = S3_ORIGINAL_MRAP_ARN if S3_ORIGINAL_MRAP_ARN else S3_ORIGINAL_IMAGE_BUCKET
        logger.info(f"Fetching original image '{originalImagePath}' from bucket/ARN '{fetch_bucket}' using endpoint '{original_bucket_s3_client.meta.endpoint_url}'")

        getOriginalImageCommandOutput = original_bucket_s3_client.get_object(Bucket=fetch_bucket, Key=originalImagePath)
        logger.info(f"Successfully downloaded {originalImagePath}")
        originalImageBody = getOriginalImageCommandOutput["Body"].read()
        contentType = getOriginalImageCommandOutput.get("ContentType")
        
        # If the image is an SVG, return it as-is without any processing
        if contentType and 'svg' in contentType.lower():
            logger.info("SVG image detected, returning as-is")
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': contentType,
                    'Cache-Control': f'max-age={TRANSFORMED_IMAGE_CACHE_TTL}'
                },
                'body': base64.b64encode(originalImageBody).decode('utf-8'),
                'isBase64Encoded': True
            }

    except Exception as error:
        return sendError(500, 'Error downloading original image', error)
    
    # Open the image using Pillow
    try:
        transformedImage = Image.open(io.BytesIO(originalImageBody))
    except Exception as error:
        return sendError(500, 'Error opening original image', error)
    
    # Get image orientation from EXIF to auto-rotate if needed
    imageMetadata = transformedImage._getexif() if hasattr(transformedImage, "_getexif") else None
    
    # Process the requested operations
    operationsParts = operationsPrefix.split(',')
    operationsJSON = dict(op.split('=') for op in operationsParts if '=' in op)
    
    # Track timing for diagnostics
    timingLog = 'img-download;dur=' + str(int(time.perf_counter() * 1000 - startTime))
    startTime = time.perf_counter() * 1000
    
    try:
        # Resize image if width or height is provided
        resizingOptions = {}
        if 'width' in operationsJSON:
            resizingOptions['width'] = int(operationsJSON['width'])
        if 'height' in operationsJSON:
            resizingOptions['height'] = int(operationsJSON['height'])
        if resizingOptions:
            orig_width, orig_height = transformedImage.size
            new_width = resizingOptions.get('width')
            new_height = resizingOptions.get('height')
            # Calculate the missing dimension to maintain aspect ratio if only one is provided
            if new_width and not new_height:
                new_height = int((orig_height * new_width) / orig_width)
            elif new_height and not new_width:
                new_width = int((orig_width * new_height) / orig_height)
            transformedImage = transformedImage.resize((new_width, new_height))
        
        # Auto-rotate the image based on EXIF data if available
        if imageMetadata and 274 in imageMetadata:
            transformedImage = ImageOps.exif_transpose(transformedImage)
        
        # Check if formatting is requested
        if 'format' in operationsJSON:
            fmt = operationsJSON['format']
            isLossy = False
            if fmt == 'jpeg':
                contentType = 'image/jpeg'
                isLossy = True
                # Convert image to RGB if it has transparency (alpha channel)
                if transformedImage.mode in ('RGBA', 'LA') or (transformedImage.mode == 'P' and 'transparency' in transformedImage.info):
                    transformedImage = transformedImage.convert('RGB')
            elif fmt == 'gif':
                contentType = 'image/gif'
            elif fmt == 'webp':
                contentType = 'image/webp'
                isLossy = True
            elif fmt == 'png':
                contentType = 'image/png'
            elif fmt == 'avif':
                contentType = 'image/avif'
                isLossy = True
            else:
                # Default to JPEG if an unsupported format is specified
                contentType = 'image/jpeg'
                isLossy = True
                if transformedImage.mode in ('RGBA', 'LA') or (transformedImage.mode == 'P' and 'transparency' in transformedImage.info):
                    transformedImage = transformedImage.convert('RGB')
        
            # Set the output format accordingly
            output_format = fmt.upper() if fmt != 'jpeg' else 'JPEG'
            # Prepare any save parameters (such as quality for lossy formats)
            save_kwargs = {}
            quality = int(operationsJSON.get('quality', DEFAULT_IMAGE_QUALITY))

            # Save the transformed image to a buffer
            if contentType:
                buffer = io.BytesIO()
                # Use quality setting for all formats
                transformedImage.save(buffer, format=fmt.upper(), quality=quality)
                transformedImageBytes = buffer.getvalue()
                logger.info(f"Successfully transformed image to format: {fmt}")
        else:
            # If no explicit format is requested, maintain the original format.
            # For example, if the image is an SVG, convert it to PNG. Since Pillow does not support saving SVGs directly,
            # we need to convert it to a raster format first.
            if contentType == 'image/svg+xml':
                contentType = 'image/png'
            buffer = io.BytesIO()
            # Save using the original image format if available, otherwise default to PNG.
            transformedImage.save(buffer, format=transformedImage.format if transformedImage.format else 'PNG')
            transformedImageBytes = buffer.getvalue()
    except Exception as error:
        return sendError(500, 'Error transforming image', error)
    
    timingLog = timingLog + ',img-transform;dur=' + str(int(time.perf_counter() * 1000 - startTime))
    
    # Upload the transformed image back to S3 if a bucket is specified
    if S3_TRANSFORMED_IMAGE_BUCKET:
        startTime = time.perf_counter() * 1000
        try:
            # Determine the bucket/ARN to use for uploading
            upload_bucket = S3_TRANSFORMED_MRAP_ARN if S3_TRANSFORMED_MRAP_ARN else S3_TRANSFORMED_IMAGE_BUCKET
            upload_key = f"{originalImagePath}/{operationsPrefix}"
            logger.info(f"Uploading transformed image to bucket/ARN '{upload_bucket}' with key '{upload_key}' using endpoint '{transformed_bucket_s3_client.meta.endpoint_url}'")

            transformed_bucket_s3_client.put_object(
                Bucket=upload_bucket,
                Key=upload_key,
                Body=transformedImageBytes,
                ContentType=contentType,
                CacheControl=f'max-age={TRANSFORMED_IMAGE_CACHE_TTL}',
                Tagging=f'transformedIn={TRANSFORM_REGION}',
                Metadata={
                    'original-image-key': originalImagePath,
                    'transformations': operationsPrefix,
                    'transformedIn': TRANSFORM_REGION
                }
            )
            timingLog = timingLog + ',img-upload;dur=' + str(int(time.perf_counter() * 1000 - startTime))
        except Exception as error:
            logError('Could not upload transformed image to S3', error)

    response_headers = {
        'Content-Type': contentType,
        'Cache-Control': f'max-age={TRANSFORMED_IMAGE_CACHE_TTL}',
        'x-transformed-in': TRANSFORM_REGION,
        'Server-Timing': timingLog
    }
    response_headers['Content-Type'] = contentType
    return {
        "statusCode": 200,
        "headers": response_headers,
        "body": base64.b64encode(transformedImageBytes).decode('utf-8'),
        "isBase64Encoded": True
    }

def sendError(statusCode, body, error):
    logError(body, error)
    return { "statusCode": statusCode, "body": body, "headers": {"x-transformed-in": TRANSFORM_REGION} }

def logError(message, error):
    logger.error(f"{message} - Error: {error}", exc_info=True)

if __name__ == "__main__":
    # Example event for local testing purposes.
    test_event = {
        "requestContext": {
            "http": {
                "method": "GET",
                # Example: convert a PNG image to JPEG with a width of 100 pixels.
                "path": "/images/rio/1.png/format=jpeg,width=100"
            }
        }
    }
    response = handler(test_event)
    logger.info(f"Local test response: {response}")
