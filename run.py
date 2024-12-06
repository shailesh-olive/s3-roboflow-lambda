import os
import time
import json
import asyncio
import aiohttp

import urllib.parse
from s3_utils import generate_presigned_url, get_s3_objects
# from dotenv import load_dotenv
# load_dotenv()

# Environment variables for configuration
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION')
ROBOFLOW_API_KEY = os.getenv('ROBOFLOW_API_KEY')
ROBOFLOW_PROJECT_NAME = os.getenv('ROBOFLOW_PROJECT_NAME')


async def upload_to_roboflow(
    presigned_url: str, 
    batch_name : str ,
    api_key: str = ROBOFLOW_API_KEY, 
    project_name: str = ROBOFLOW_PROJECT_NAME, 
    img_name='', 
    split="train"
):
    """Upload an image to Roboflow."""
    
    # Construct upload URL with batch name
    API_URL = f"https://api.roboflow.com/dataset/{project_name}/upload"
    upload_url = f"{API_URL}?api_key={api_key}&name={img_name}&split={split}&batch={batch_name}&image={urllib.parse.quote_plus(presigned_url)}"
    
    try:
        # Use aiohttp for asynchronous HTTP requests
        async with aiohttp.ClientSession() as session:
            async with session.post(upload_url) as response:
                if response.status == 200:
                    json_response = await response.json()
                    if 'duplicate' in json_response.keys():
                        print(f"Trying to upload duplicate image. Skipped!")
                        
                    print(f"Successfully uploaded {img_name} to {project_name} in batch {batch_name}")
                else:
                    error_content = await response.text()
                    print(f"Failed to upload {img_name}. Error: {error_content}")
                                
    except Exception as e:
        print(f"Error with image upload : {e}")
        
        
async def run_upload_process(s3_urls, project_name):
    # Upload images to Roboflow concurrently
    # with concurrent.futures.ThreadPoolExecutor(max_workers=min(20, len(s3_urls))) as executor:
    #     futures = [executor.submit(
    #         upload_to_roboflow, 
    #         presigned_url, 
    #         f"batch-{project_name}") for presigned_url in s3_urls
    #     ]
    
    tasks = [
        upload_to_roboflow(url, f"batch-{project_name}")
        for url in s3_urls
    ]
    await asyncio.gather(*tasks)


def lambda_handler(event, context):
    """
    Lambda function to generate presigned URLs for S3 objects and upload to Roboflow.
    Triggered via API Gateway.
    """
    start = time.time()
    
    # Parse input parameters from API Gateway event
    if event.get('body') is None:
        raise Exception("Json Body is not added")

    body = json.loads(event['body'])
    folder_prefix = body.get('s3_base_path', '')
    project_name = folder_prefix.split('/')[-1].replace(" ","_")
    
    if not folder_prefix:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'folder_prefix is required'})
        }

    # Fetch all images in the specified folder
    available_images = get_s3_objects(folder_prefix, S3_BUCKET_NAME)
    presigned_urls = [generate_presigned_url(image_key, S3_BUCKET_NAME) for image_key in available_images]
    print(f"Uploading {len(available_images)} images from {project_name}.")

    asyncio.run(
        run_upload_process(presigned_urls, project_name)
    )

    end_time = time.time() - start
    print(f"Total time taken to upload {len(available_images)} images is {end_time} seconds.")

    return {
        'statusCode': 200,
        'body': json.dumps({'message': f'Uploaded {len(available_images)} images to roboflow in {end_time} seconds.'})
    }
    
    
# if __name__ == '__main__':
#     res = lambda_handler(
# 		event = {
#             "body":json.dumps({
#                "s3_base_path": "NFL/localTest_39-test-xx_b8fd64cb-101a-4313-a8be-ae36d76f4e77",
#             })
#         },
# 		context=None
# 	)
#     print(res)
