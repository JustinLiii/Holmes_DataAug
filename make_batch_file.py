import json
import random
import uuid
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor

from datasets import load_dataset
from tqdm import tqdm

MAX_BATCH_NUM=50000
# MAX_BATCH_NUM=10
MAX_BATCH_SIZE=100*1024*1024

DATASET_SIZE = 55353453 # fineweb-cn(Insdustrial+Tele)

@dataclass
class PromptTemplate:
    SYSTEM_PROMPT: str
    PROMPT: str

@dataclass
class CorrectQA(PromptTemplate):
    SYSTEM_PROMPT='你是一个乐于解答各种问题的助手，你的任务是为用户提供专业、准确、有见地的建议。'
    PROMPT='请仔细阅读以下文段，提出10个问题和对应的答案\n\n{{文段}}\n\n格式\n问题1：XXXX\n答案1：XXXX\n\nn问题2：XXXX\n答案2：XXXX\n\n......\n\n问题10：XXXX\n答案10：XXXX\n\n'

# Wrong QA
@dataclass
class WrongQA(PromptTemplate):
    SYSTEM_PROMPT='你是一个乐于解答各种问题的助手，你的任务是为用户提供专业、准确、有见地的建议。'
    PROMPT='请仔细阅读以下文段，提出10个问题并给出错误的答案\n\n{{文段}}\n\n格式\n问题1：XXXX\n答案1：XXXX\n\n问题2：XXXX\n答案2：XXXX\n\n......\n\n问题10：XXXX\n答案10：XXXX\n\n'

# Translation
@dataclass
class Translation(PromptTemplate):
    SYSTEM_PROMPT='你是一个中文到英文的翻译器，请将以下中文文段翻译成英文。'
    PROMPT='请将以下中文文段翻译成英文\n\n{{文段}}\n\n'
    
PROB = [0.5, 0.25, 0.25]

def enhance(template: type[PromptTemplate], text: str) -> str:
    system_prompt = template.SYSTEM_PROMPT
    prompt = template.PROMPT.replace('{{文段}}', text)
    
    # glm
    # request = {
    #     'custom_id': str(uuid.uuid4()),
    #     'method': 'POST',
    #     'url': '/v4/chat/completions',
    #     'body': {
    #         'model': 'glm-4-flash',
    #         'messages': [
    #             {'role': 'system', 'content': system_prompt},
    #             {'role': 'user', 'content': prompt}
    #         ],
    #         'temperature': 0.95, # glm体验中心默认参数
    #         'top_p': 0.7, # glm体验中心默认参数
    #         'max_tokens': 4096, # 考虑到一些文段可能较长
    #     }
    # }
    
    # baidu
    request = {
        'id': str(uuid.uuid4()),
        'request_body': {
            'system': system_prompt,
            'messages': [
                {'role':'user', 'content': prompt}
            ],
            "top_p": 0.5, 
            "temperature": 0.9, 
            "penalty_score": 1.1
        }
    }
    
    return json.dumps(request, ensure_ascii=False)
    
def main(start = MAX_BATCH_NUM):
    dataset = load_dataset(
        'opencsg/chinese-fineweb-edu', 
        split='train', 
        streaming=True,
        cache_dir='cache')
    
    template_choices = random.choices(
        [CorrectQA, WrongQA, Translation],
        weights=PROB,
        k=MAX_BATCH_NUM
    )
    
    dataset_iter = iter(dataset)
    
    for i in range(start):
        next(dataset_iter)
    
    with ProcessPoolExecutor(max_workers=6) as executor:
        with open('batch1.jsonl', 'w', encoding='utf-8') as f:
            futures = []
            for i in tqdm(range(MAX_BATCH_NUM), desc='Submitting Tasks'):
                text = next(dataset_iter)['text']
                template = template_choices[i]
                future = executor.submit(enhance, template, text)
                futures.append(future)
            for future in tqdm(futures, desc='Getting Results'):
                f.write(future.result()+'\n')
                
if __name__ == '__main__':
    main()