import asyncio
import concurrent.futures
from datetime import datetime
import json
import os
import time

import boto3

aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_region = os.environ["AWS_DEFAULT_REGION"]
bucket_name = os.environ["BUCKET_NAME"]

local_folder_path = f"{bucket_name}-local"
print(bucket_name)


class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


s3_client = boto3.client('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key,
                         region_name=aws_region)


async def download_file(key, local_file_path):
    if not os.path.isfile(local_file_path):
        if os.path.dirname(local_file_path):
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        await asyncio.get_running_loop().run_in_executor(
            None, s3_client.download_file, bucket_name, key, local_file_path)


async def main():
    paginator = s3_client.get_paginator('list_objects_v2')
    objects = []
    for response in paginator.paginate(Bucket=bucket_name):
        current_objects = response.get('Contents', [])
        objects.extend(current_objects)
    print(len(objects))
    with open(f"{bucket_name}.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(objects, indent=4, cls=DateTimeEncoder))
    objects = list(filter(lambda x: os.path.basename(x['Key']), objects))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            asyncio.create_task(
                download_file(obj['Key'],
                              os.path.join(local_folder_path, obj['Key'])))
            for obj in objects
        ]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    os.makedirs(local_folder_path, exist_ok=True)
    start_time = time.time()

    asyncio.run(main())

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time: {execution_time} seconds")
