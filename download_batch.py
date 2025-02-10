import os
import time
import asyncio

from dotenv import load_dotenv
# Baidu BCE
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.services.bos.bos_client import BosClient
from qianfan import resources
from tqdm import tqdm
from fire import Fire
from windows_toasts import Toast, WindowsToaster

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

toaster = WindowsToaster('Task Downloader')

def make_toast(text: str):
    newToast = Toast()
    newToast.text_fields = [text]
    toaster.show_toast(newToast)
    

def check_task(start, end):
    print("Checking task status")
    task_names = [f"batch{i}" for i in range(start, end)]
    resp = resources.console.utils.call_action( # type: ignore
        # 调用本文API，该参数值为固定值，无需修改；对应API调用文档-请求结构-请求地址的后缀
        "/v2/batchinference", 
        # 调用本文API，该参数值为固定值，无需修改；对应API调用文档-请求参数-Query参数的Action 
        "DescribeBatchInferenceTasks", 
        # 请查看本文请求参数说明，根据实际使用选择参数；对应API调用文档-请求参数-Body参数
        {}
    )
    tasks = [task for task in resp.body["result"]["taskList"] if task["name"] in task_names]
    task_output_uri = [f"{task["outputBosUri"]}/{task["outputDir"]}" for task in tasks]
    
    done = True
    for task in tasks:
        status = task["runStatus"]
        if status == 'Done':
            continue
        
        if status in ['Stopped', 'Failed', 'Expired']:
            make_toast(f"Task {task["name"]} failed in status {status}")
            raise RuntimeError(f"Task {task["name"]} failed in status {status}")
        
        done = False
        break
        
    print(f"Task status: {'Done' if done else 'Running'}")
    return done, task_output_uri

def download_output(task_output_uri):
    task_output_uri = [uri.lstrip(f'bos:/{bucket_name}/') for uri in task_output_uri]
    object_keys = []
    for uri in task_output_uri:
        object_keys.extend([obj.key for obj in bos_client.list_all_objects(bucket_name, prefix=uri)])
    with tqdm(total=len(object_keys)) as pbar:
        tasks = []
        for key in object_keys:
            file_name = key.split('/')[-1]
            file_name = os.path.join('desc', file_name)
            t = asyncio.create_task(bos_client.get_object_to_file(bucket_name, key, file_name))
            t.add_done_callback(lambda _: pbar.update(1))
            tasks.append(t)
        asyncio.gather(*tasks)
        

def main(start, end):
    # check status every 2 minutes
    while True:
        done, task_output_uri = check_task(start, end)
        if done:
            break
        time.sleep(120)
    
    make_toast("Task Done")
    download_output(task_output_uri)
    make_toast("Download Complete")
    
if __name__ == '__main__':
    Fire(main)