import os
import asyncio

from dotenv import load_dotenv
# Baidu BCE
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.bos.bos_client import BosClient
# Quality of life
from tqdm import tqdm

load_dotenv()

#设置BosClient的Host，Access Key ID和Secret Access Key
bos_host = "gz.bcebos.com"
access_key_id = os.getenv("QIANFAN_ACCESS_KEY")
secret_access_key = os.getenv("QIANFAN_SECRET_KEY")

#创建BceClientConfiguration
config = BceClientConfiguration(credentials=BceCredentials(access_key_id, secret_access_key), endpoint = bos_host)


#新建BosClient
bos_client = BosClient(config)

bucket_name = "qwq114514"

async def upload_batch_file(file_name: str, key: str):
    bos_client.put_object_from_file(bucket_name, key, file_name)
    

async def upload(start: int, end: int):
    with tqdm(total=end-start) as pbar:
        tasks = []
        for i in range(start, end):
            file_name = f"src/batch{i}.jsonl"
            key = f"src/{i}/{file_name}"
            t = asyncio.create_task(upload_batch_file(file_name, key))
            t.add_done_callback(lambda _: pbar.update(1))
            tasks.append(t)
        await asyncio.gather(*tasks)
            
def main(start, end):
    asyncio.run(upload(start, end))
    
if __name__ == '__main__':
    import fire
    fire.Fire(main)    