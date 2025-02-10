import json
import uuid

class PromptTemplate:
    def __init__(self, SYSTEM_PROMPT, PROMPT):
        self.SYSTEM_PROMPT = SYSTEM_PROMPT
        self.PROMPT = PROMPT

def enhance_qa_pairs(data, template):
    system_prompt = template.SYSTEM_PROMPT
    prompt = template.PROMPT.replace('{{问题}}', data['question']).replace('{{答案}}', data['answer'])
    
    request = {
        'custom_id': str(uuid.uuid4()),
        'method': 'POST',
        'url': '/v4/chat/completions',
        'body': {
            'model': 'glm-4-flash',
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': prompt}
            ],
            'temperature': 0.95,
            'top_p': 0.7,
            'max_tokens': 4096
        }
    }
    
    return json.dumps(request, ensure_ascii=False)

def main():
    with open('data.jsonl', 'r', encoding='utf-8') as f:
        data = []
        for line in f:
            data.append(json.loads(line.strip()))
        
    translation_template = PromptTemplate(
        SYSTEM_PROMPT='你是一个英文到中文的翻译器，请将以下英文文段翻译成中文。输出格式：\n### 问题\n...\n### 答案\n...',
        PROMPT='请将以下英文数学题问答翻译成中文，注意用中式的人名和本地化的例子代替英文名和非中国生活中常见的例子。注意要保留数字和数学逻辑关系\n\n### 问题\n{{问题}}\n### 答案\n{{答案}}\n\n输出格式：\n### 问题\n...\n### 答案\n...\n\n'
    )
    
    with open('translation_answers.jsonl', 'w', encoding='utf-8') as f:
        for item in data:
            request = enhance_qa_pairs(item, translation_template)
            f.write(request + '\n')

if __name__ == '__main__':
    main()
