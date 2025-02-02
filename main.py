import os
import openai
import re
import json
import asyncio

import backoff
from tqdm import tqdm
from datasets import load_dataset

from dotenv import load_dotenv
load_dotenv()

# fineweb-cn(Insdustrial+Tele), total 55353453
# Correct QA
SYSTEM_PROMPT='你是一个乐于解答各种问题的助手，你的任务是为用户提供专业、准确、有见地的建议。'
PROMPT='请仔细阅读以下文段，提出10个问题和对应的答案\n\n{{文段}}\n\n格式\n问题1：XXXX\n答案1：XXXX\n\nn问题2：XXXX\n答案2：XXXX\n\n......\n\n问题10：XXXX\n答案10：XXXX\n\n'

# Wrong QA
SYSTEM_PROMPT='你是一个乐于解答各种问题的助手，你的任务是为用户提供专业、准确、有见地的建议。'
PROMPT='请仔细阅读以下文段，提出10个问题并给出错误的答案\n\n{{文段}}\n\n格式\n问题1：XXXX\n答案1：XXXX\n\n问题2：XXXX\n答案2：XXXX\n\n......\n\n问题10：XXXX\n答案10：XXXX\n\n'

# Translation
SYSTEM_PROMPT='你是一个中文到英文的翻译器，请将以下中文文段翻译成英文。'
PROMPT='请将以下中文文段翻译成英文\n\n{{文段}}\n\n'
# open-web-math, total 6315233
# SYSTEM_PROMPT='You are an helpful assistant who is eager to answer all kinds of questions. Your task is to provide professional, accurate, and insightful advice to users.'
# PROMPT='Carefully read the following text, repharse the content into a organized and coherent text.\n\n{{文段}}\n\n'

CLIENT = openai.AsyncClient(
        api_key=os.getenv('ZHIPU_API_KEY'),
        # api_key='ollama',
        # base_url='http://127.0.0.1:11434/v1/'
        base_url="https://open.bigmodel.cn/api/paas/v4/"
    )

def exceed_limit(e) -> bool:
    if not isinstance(e, openai.BadRequestError):
        return False
    return e.code in (1302, 1303)

@backoff.on_predicate(backoff.expo, exceed_limit, max_tries=3, on_backoff=lambda x: print(f'Backoff: {x}'))
async def query(prompt) -> str:
    response = await CLIENT.chat.completions.create(
        messages=[{'role': 'system', 'content': SYSTEM_PROMPT},
                    {'role': 'user', 'content': prompt}],
        # model='qwen2.5:3b',
        model='glm-4-flash'
    )
    assert response.choices[0].message.content is not None
    return response.choices[0].message.content

async def augment_text(text: str) -> str | openai.BadRequestError:
    prompt = PROMPT.replace('{{文段}}', text)
    try:
        ret = await query(prompt)
    except openai.BadRequestError as e:
        if e.code == 1301:
            return e
        raise e
    if isinstance(ret, openai.BadRequestError):
        return ret
    return re.sub(r'问题\d+：|答案\d+：', '', ret) # type: ignore

async def get_and_write(text, f):
    ret = await augment_text(text)
    if isinstance(ret, openai.BadRequestError):
        return 1 if ret.code == 1301 else 0
    json.dump({'augmented_text': ret}, f, ensure_ascii=False)
    f.write('\n')
    return 0

async def main(output_dir='output.jsonl'):
    dataset = load_dataset(
        'open-web-math/open-web-math', 
        split='train', 
        streaming=True,
        cache_dir='cache')
    cnt = 0
    async with asyncio.Semaphore(1):
        with tqdm(total=6315233) as pbar:
            tasks = []
            with open(output_dir, 'a+', encoding='utf-8') as f:
                for example in dataset:
                    text = example['text']
                    task = asyncio.create_task(get_and_write(text, f))
                    task.add_done_callback(lambda _: pbar.update(1))
                    tasks.append(task)
                    cnt += 1
                    if cnt >= 1:
                        break
                results = await asyncio.gather(*tasks)
    bad_text_cnt = sum(results)
    print(f'Bad text count: {bad_text_cnt}')
        
if __name__ == '__main__':
    asyncio.run(main())
