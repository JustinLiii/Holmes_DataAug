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
    

def check_task():
    print("Checking task status")
    resp = resources.console.utils.call_action(
        # 调用本文API，该参数值为固定值，无需修改；对应API调用文档-请求结构-请求地址的后缀
        "/v2/batchinference", 
        # 调用本文API，该参数值为固定值，无需修改；对应API调用文档-请求参数-Query参数的Action 
        "DescribeBatchInferenceTasks", 
        # 请查看本文请求参数说明，根据实际使用选择参数；对应API调用文档-请求参数-Body参数
        {}
    )
    task_output_uri = [task["outputBosUri"] for task in resp.body["result"]["taskList"]]
    
    done = True
    for task in resp.body["result"]["taskList"]:
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
    task_output_uri = task_output_uri.lstrip(f'bos:/{bucket_name}/')
    object_keys = [obj.key for obj in bos_client.list_all_objects(bucket_name, prefix=task_output_uri)]
    with tqdm(total=len(object_keys)) as pbar:
        for key in object_keys:
            file_name = key.split('/')[-1]
            file_name = os.path.join('desc', file_name)
            t = asyncio.create_task(bos_client.get_object_to_file(bucket_name, key, file_name))
            t.add_done_callback(lambda _: pbar.update(1))
        

def main():
    # check status every 2 minutes
    while True:
        done, task_output_uri = check_task()
        if done:
            break
        time.sleep(120)
        
    download_output(task_output_uri)
    make_toast("Task Done")
    
if __name__ == '__main__':
    main()