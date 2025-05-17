import json
import os
import types
from enum import Enum
from typing import Literal, Union

import dotenv
import openai
from pydantic import BaseModel, Field

dotenv.load_dotenv()

一级领域 = Literal[
    "经济与产业",
    "人文与社会",
    "医疗与健康",
    "科学与智能",
    "综合",
]

二级领域 = Literal[
    "生物医药",
    "政府决策",
    "公共卫生",
    "社会治理",
    "智慧医疗",
    "地缘环境",
    "经济金融",
    "绿色能源",
    "智能制造",
    "人文与社会",
    "社会治理_政府决策_地缘环境",
    "公共卫生_经济金融_其他",
    "其他",
    "其他_其他_其他",
    "经济金融_宏观经济数据",
]


class Data(BaseModel):
    """
    一条数据线索，与科学研究相关，可能是一个数据集，也可能是一个数据源。
    """

    record_id: str = Field(description="数据的ID，无需补全")
    data_name: str = Field(description="数据名称")
    web_link: str = Field(description="数据来源的网页链接")

    data_description: str | None = Field(description="数据简介，300字以内", default=None)
    level_one_category: 一级领域 | None = Field(description="一级领域", default=None)
    level_two_category: 二级领域 | None = Field(description="二级领域", default=None)
    level_three_category: str | None = Field(description="三级领域", default=None)

    data_type: str | None = Field(description="数据类型", default=None)
    country_or_district: str | None = Field(description="数据所属国家/地区", default=None)
    data_language: str | None = Field(description="数据中涉及的自然语言类型", default=None)
    data_modality: str | None = Field(description="数据模态", default=None)

    data_organization: str | None = Field(description="数据提供者", default=None)
    data_publisher: str | None = Field(description="数据发布者", default=None)
    data_update_date: str | None = Field(description="数据更新时间", default=None)
    data_estimated_size: float | None = Field(description="数据大小(GB)", default=None)


def is_union_type(anno):
    if anno is None:
        return False
    if isinstance(anno, types.UnionType):
        return True
    if hasattr(anno, "__origin__") and anno.__origin__ is Union:
        return True
    return False


def is_enum_type(anno):
    if anno is None:
        return False
    if isinstance(anno, type) and issubclass(anno, Enum):
        return True
    return False


def get_enum_values(anno):
    if is_enum_type(anno):
        return [member.value for member in anno]
    return []


def is_literal_type(anno):
    if anno is None:
        return False
    if hasattr(anno, "__origin__") and anno.__origin__ is Literal:
        return True
    return False


def get_literal_values(anno):
    if is_literal_type(anno):
        return list(anno.__args__)
    return []


def get_fields(data):
    assert isinstance(data, BaseModel)
    fields = []
    for name, field in type(data).model_fields.items():
        anno, desc = field.annotation, field.description
        if anno is not None and is_union_type(anno) and len(anno.__args__) == 2:
            if anno.__args__[0] == types.NoneType:
                anno = anno.__args__[1]
            elif anno.__args__[1] == types.NoneType:
                anno = anno.__args__[0]
        options = []
        if is_enum_type(anno):
            options = get_enum_values(anno)
        elif is_literal_type(anno):
            options = get_literal_values(anno)
        if options:
            anno = type(options[0])
        if anno is not None and hasattr(anno, "__name__"):
            anno = anno.__name__
        fields.append({"name": name, "desc": desc, "type": anno, "options": options})
    return fields


def get_prompt(data):
    fields = get_fields(data)
    data_doc = (type(data).__doc__ or "").strip()
    filled_data = {k: v for k, v in data.__dict__.items() if v is not None}
    unfilled_fields = [k for k, v in data.__dict__.items() if v is None]
    prompt = "你是一名数据领域的专家，现在有一条数据需要你来补充完整。\n"
    if data_doc:
        prompt += f"这条数据表示：{data_doc}\n"
    prompt += "这条数据包括如下字段：\n\n"
    prompt += "```\n"
    for field in fields:
        prompt += f"字段名: {field['name']}\n"
        prompt += f"描述: {field['desc']}\n"
        prompt += f"类型: {field['type']}\n"
        if field["options"]:
            prompt += "可选值:\n"
            for val in field["options"]:
                prompt += f"  - {val}\n"
        prompt += "---\n"
    prompt += "```\n\n"
    prompt += "在这条数据中，部分字段是已经填充好的，你只需要补充未填充的字段。\n"
    prompt += "以下为已经填充好的字段：\n\n"
    prompt += "```json\n"
    prompt += json.dumps(filled_data, indent=2, ensure_ascii=False) + "\n"
    prompt += "```\n\n"
    prompt += "请根据以上信息，补充完整这条数据。已经填充的字段无需包含在输出中。\n"
    prompt += "未填充的字段包括：(" + ", ".join(unfilled_fields) + ")\n"
    prompt += "请注意，输出需要符合JSON格式，不要输出其他内容。\n\n"
    prompt += "```json\n"
    return prompt


openai_api_key = os.getenv("OPENAI_API_KEY")
openai_base_url = os.getenv("OPENAI_BASE_URL")
openai_client = openai.OpenAI(api_key=openai_api_key, base_url=openai_base_url)


def complete_data(data, model="gpt-4o-mini", use_json_schema=False):
    assert isinstance(data, BaseModel)
    unfilled_fields = [k for k, v in data.__dict__.items() if v is None]
    if not unfilled_fields:
        return data
    prompt = get_prompt(data)
    if use_json_schema:
        json_schema = type(data).model_json_schema()
        response = openai_client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "Data",
                    "description": "数据",
                    "schema": json_schema,
                    "strict": False,
                },
            },
        )
    else:
        response = openai_client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
    if not response.choices:
        raise ValueError("No response from OpenAI")
    content = response.choices[0].message.content
    if not content:
        raise ValueError("No content from OpenAI, response: %s", response)
    try:
        completed = json.loads(content)
    except json.JSONDecodeError:
        raise ValueError(
            "Invalid JSON content from OpenAI, response: %s, content: %s",
            response,
            content,
        )
    for field in unfilled_fields:
        setattr(data, field, completed[field])
    return data


data = Data(
    record_id="recuI2Uqv258Q1",
    data_name="先进制造伙伴计划",
    web_link="https://obamawhitehouse.archives.gov/the-press-office/2011/06/24/president-obama-launches-advanced-manufacturing-partnership",
    level_one_category="经济与产业",
    level_two_category="智能制造",
    level_three_category="智能制造装备（智能机床、机器人等）",
    data_type="政策文件",
    country_or_district="美国",
)

print("Before completion:")
print(json.dumps(data.model_dump(), indent=2, ensure_ascii=False))

data = complete_data(data)

print("After completion:")
print(json.dumps(data.model_dump(), indent=2, ensure_ascii=False))


# print(search_and_fetch("test"))

# print(json.dumps(google_search("What is the capital of France?"), indent=2))
