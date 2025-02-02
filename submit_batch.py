import os

from dotenv import load_dotenv
from zhipuai import ZhipuAI

load_dotenv()

client = ZhipuAI(api_key=os.getenv('ZHIPU_API_KEY'))

with open('batch.jsonl', 'rb') as f:
    result = client.files.create(
        file=f,
        purpose='batch'
    )
    print(result)
