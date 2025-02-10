import json
import random
import uuid
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor

import pyarrow.dataset as ds
from tqdm import tqdm
from fire import Fire

MAX_BATCH_NUM=30000
MAX_JOB_NUM=5

# DATASET_SIZE = 55353453 # fineweb-cn(Insdustrial+Tele)

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
    
def make_one_file(start: int, target_dir: str, dataset_dir: str="E:\\Projects\\HolmesLM\\dataset\\4_5"):
    dataset = ds.dataset(dataset_dir, format='parquet')
    scanner = ds.Scanner.from_dataset(
        dataset, 
        columns=['text'], 
        filter=ds.field('source') != 'SkyPile')
    lines = scanner.take(range(start, start+MAX_BATCH_NUM)).column('text')

    template_choices = random.choices(
        [CorrectQA, WrongQA, Translation],
        weights=PROB,
        k=MAX_BATCH_NUM
    )

    with ProcessPoolExecutor(max_workers=6) as executor:
        with open(target_dir, 'w', encoding='utf-8') as f:
            futures = []
            for i in tqdm(range(MAX_BATCH_NUM), desc='Submitting Tasks'):
                text = str(lines[i])
                template = template_choices[i]
                future = executor.submit(enhance, template, text)
                futures.append(future)
            for future in tqdm(futures, desc='Getting Results'):
                f.write(future.result()+'\n')
                
def main(start = 0, end = 0 + MAX_JOB_NUM):
    for i in range(start, end):
        make_one_file(i*MAX_BATCH_NUM, f'src/batch{i}.jsonl')

if __name__ == '__main__':
    Fire(main)