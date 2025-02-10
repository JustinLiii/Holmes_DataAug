import asyncio

from dotenv import load_dotenv
from qianfan import resources

from tqdm import tqdm
# 通过环境变量初始化认证信息
load_dotenv()

async def creat_task(i):
     resources.console.utils.call_action(
        # 调用本文API，该参数值为固定值，无需修改；对应API调用文档-请求结构-请求地址的后缀
        "/v2/batchinference", 
        # 调用本文API，该参数值为固定值，无需修改；对应API调用文档-请求参数-Query参数的Action
        "CreateBatchInferenceTask", 
        # 请查看本文请求参数说明，根据实际使用选择参数；对应API调用文档-请求参数-Body参数
        {
            "name":f"batch{i}",
            "description":f"batch{i}",
            "modelId": "amv-sb5kfqie51z1", # ernie-tiny-8k
            "inferenceParams":{
                "top_p": 0.5, 
                "temperature": 0.9, 
                "penalty_score": 1.1
            },
            "inputBosUri":f"bos:/qwq114514/src/{i}",
            "outputBosUri":"bos:/qwq114514/desc"
        }
     )

async def create(start: int, end: int):
    with tqdm(total=end-start) as pbar:
        tasks = []
        for i in range(start, end):
            t = asyncio.create_task(creat_task(i))
            t.add_done_callback(lambda _: pbar.update(1))
            tasks.append(t)
        await asyncio.gather(*tasks)

def main(start, end):
    asyncio.run(create(start, end))

if __name__ == '__main__':
    import fire
    fire.Fire(main)