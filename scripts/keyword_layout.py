#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: keyword_layout.py
@time: 2020/12/18 11:29
@desc:
"""
from core.core import logger
from algos.formula import CustomSentence
from algos.castrated_cpni.extract_location import ExtractLocation
from tools.baixing_elasticsearch import BXElasticSearch
import re
import numpy as np
from retrying import retry
import os
import json
from tools.utils import list_merge, custom_replace_word, get_paragraphs

et = ExtractLocation()  # 地名实例
es = BXElasticSearch()  # 百姓es实例
cs = CustomSentence()   # 套话实例
"""
业务需求：
?补充一个前置项，对于B2C行业，优先在其子类目中检索；后期补上检索词映射，查es库部分代码可以再精简一下
1、地名识别接口识别标题中的地名，标题中所有地名删除，正文中地名统一替换为地域词A。（与需求有些出入）。
2、业务关键词BCD从ES中精确匹配标题
返回的结果不为空时，取第一条数据；正则匹配BCD，原数据标题在第一个BCD前加上A作为新标题；添加字段 layout_tag:标题匹配BCD词。
3、步骤二满足则跳过，以核心词C到ES中精确匹配标题
返回的结果不为空时，取第一条数据；正则匹配C词，匹配到的第一个C词替换为目标关键词；添加字段 layout_tag:标题匹配C词
则标题生成结束。
4、BCD词匹配正文，随机一个位置BCD词替换为目标关键词。
5、若步骤四满足则跳过，C词匹配正文，随机一个位置C词替换为目标关键词。
6、若步骤四或五满足则跳过，C词未匹配到，则在第一段后加上包含目标关键词的套话。
自此，标题和正文布词结束。
"""
def get_es_data(query_id, paras):
    """
    获取ES数据库数据
    :param query_id: es 模板
    :param paras: 参数列表
    :return:
    """
    es_back = es.search_pro(
        query_id=query_id,
        paras=paras,
        indexs="dw_article"
    )
    es_response = es_back.get("restResponse")
    datas = {}
    if es_response:
        count = es_response["hits"]["total"]["value"]
        if count > 0:
            datas = es_response["hits"]["hits"][0]["_source"]  # 取匹配的第一条数据
    return datas

def get_data_match_title(w_A, w_B, w_C, w_D, industry_l2, business_category):
    # 通过关键词匹配标题得到的原始数据
    datas = {}  # 返回匹配到的数据
    if business_category == "B2C" and industry_l2 != "":
        # 执行额外的优先检索操作
        datas = optional_match_title(w_A, w_B, w_C, w_D, industry_l2, business_category)
    if not datas:
        flag_tag = ["标题匹配BCD词"]  # 标记分析tag
        # TODO 标题精确匹配
        query_param_BCD = " && ".join([i for i in [w_B, w_C, w_D] if i])  # 短语逻辑匹配，不一定连续
        datas = get_es_data(query_id="01000", paras=[query_param_BCD, business_category])  # 业务词标题查询
        if not datas:
            query_param_C = w_C
            datas = get_es_data(query_id="01000", paras=[query_param_C, business_category])  # 核心词标题查询
            flag_tag = ["标题匹配C词"]
        # 可能标题匹配不到数据[]
        if datas:
            datas["keyword_layout_tag"] = flag_tag
            if datas.get("lastUpdatedTime"):
                del datas["lastUpdatedTime"] # 删除字段lastUpdatedTime
     # TODO 导入新的dw_ai_article中
    return datas

def optional_match_title(w_A, w_B, w_C, w_D, industry_l2, business_category):
    """
    补充需求匹配标题
    :param w_A: 词根A
    :param w_B: 词根B
    :param w_C: 词根C
    :param w_D: 词根D
    :param industry_l2: 百姓网二级行业类目
    :param business_category: B2B/B2C 行业类别
    :return:
    """
    # TODO 后期要加检索词映射字典，同时支持热更新
    flag_tag = ["标题匹配BCD词"]  # 标记分析tag
    query_param_BCD = " && ".join([i for i in [w_B, w_C, w_D] if i])  # 短语逻辑匹配，不一定连续
    datas = get_es_data(query_id="01001", paras=[query_param_BCD, industry_l2, business_category])  # 业务词标题查询
    if not datas:
        query_param_C = w_C
        datas = get_es_data(query_id="01001", paras=[query_param_C, industry_l2, business_category])  # 核心词标题查询
        flag_tag = ["标题匹配C词"]
    # 可能标题匹配不到数据[]
    if datas:
        datas["keyword_layout_tag"] = flag_tag
        if datas.get("lastUpdatedTime"):
            del datas["lastUpdatedTime"] # 删除字段lastUpdatedTime # 外层删除该字段
    return datas

def clean_location(text):
    """
    文本清洗所有地名，不替换
    :param text:
    :return:
    """
    loc_list = et.find_loc(text=text)
    for _, loc in enumerate(loc_list):
        text = re.sub(loc, "", text)
    return text

def replace_location(text, target_word=""):
    """
    文本替换所有地域词为目标地域词
    :param text: 文本
    :param target_word: 目标地域词
    :return:
    """
    loc_list = et.find_loc(text=text)
    loc_match_start_end_list = []
    for loc in loc_list:
        print([[substr.start(), substr.end()] for substr in re.finditer(loc, text)])
        loc_match_start_end_list.extend([[substr.start(), substr.end()] for substr in re.finditer(loc, text)])
    # 合并地名列表，小于一个字符差异的也会被合并
    loc_match_start_end_list = list_merge(loc_match_start_end_list)
    print(loc_match_start_end_list)
    # 地名合并后替换
    text = custom_replace_word(text=text, match_start_end_list=loc_match_start_end_list, target_word=target_word)
    return text

def modify_title(data, w_A, w_B, w_C, w_D):
    """
    标题布词
    :param data: es数据体
    :param w_A: 词根A
    :param w_B: 词根B
    :param w_C: 词根C
    :param w_D: 词根D
    :return:
    """
    # 清洗地域词
    data["title"] = clean_location(text=data["title"])
    # 标题布词，布设第一个位置
    if data.get("keyword_layout_tag") and "标题匹配BCD词" in data.get("keyword_layout_tag"):
        # TODO 待测试标题匹配结果
        temp_word = w_B if w_B else w_C
        new_title = re.sub(
            temp_word, w_A + temp_word, data["title"], count=1
        )  # 仅替换匹配到的第一个词
    elif data.get("keyword_layout_tag") and "标题匹配C词" in data.get("keyword_layout_tag"):
        # new_title = (w_A + w_B + w_C + w_D) + "，" + data["title"] # 需求调整，目标关键词，原标题
        new_title = re.sub(
            w_C, (w_A + w_B + w_C + w_D), data["title"], count=1
        )  # 匹配到的第一个C词替换为目标关键词
    else:
        # 这一步理论上不会遇到，除非前词语有歧义，被清洗了
        logger.warning("[关键词布词] [标题未匹配到C词]")
        new_title = data["title"]
    data["title"] = new_title
    print(data["title"])
    return data

def modify_content(data, w_A, w_B, w_C, w_D):
    """
    正文布词
    :param data: es数据体
    :param w_A: 词根A
    :param w_B: 词根B
    :param w_C: 词根C
    :param w_D: 词根D
    :return:
    """
    # 通过地域词A替换所有不连续的地域词
    # print(data["content"])
    data["content"] = replace_location(text=data["content"], target_word=w_A)
    # print(data["content"])
    # 正文布词
    query_word_BCD = w_B + w_C + w_D
    query_word_C = w_C
    target_word = w_A + w_B + w_C + w_D
    text = data["content"]
    keyword_layout_tag_list = data.get("keyword_layout_tag", [])
    res_BCD = re.search(query_word_BCD, text)
    res_C = re.search(query_word_C, text)

    if res_BCD:
        # 所有BCD位置
        query_match_start_end_list = [[substr.start(), substr.end()] for substr in
                                      re.finditer(query_word_BCD, text)]
        # 随机选取一个位置，不允许重复的随机值
        random_indexs = sorted(np.random.choice(len(query_match_start_end_list), 1, replace=False))
        # 随机位置映射到原列表
        query_match_start_end_list = [query_match_start_end_list[index] for index in random_indexs]
        data["content"] = custom_replace_word(text=text, match_start_end_list=query_match_start_end_list,
                                              target_word=target_word)

        keyword_layout_tag_list.append("正文匹配BCD词")
        data["keyword_layout_tag"] = keyword_layout_tag_list
    elif not res_BCD and res_C:
        # 所有C位置
        query_match_start_end_list = [[substr.start(), substr.end()] for substr in re.finditer(query_word_C, text)]
        # 随机选取一个位置，不允许重复的随机值
        random_indexs = sorted(np.random.choice(len(query_match_start_end_list), 1, replace=False))
        # 随机位置映射到原列表
        query_match_start_end_list = [query_match_start_end_list[index] for index in random_indexs]
        data["content"] = custom_replace_word(text=text, match_start_end_list=query_match_start_end_list,
                                              target_word=target_word)

        keyword_layout_tag_list.append("正文匹配C词")
        data["keyword_layout_tag"] = keyword_layout_tag_list
    else:
        # print(text)
        # print(get_paragraphs(text))

        # 人工段落创建一个随机模板句子，并埋入目标关键词
        random_sentence = cs.create_manual_sentence(target_word=target_word)
        # 获取段落结构
        origin_paragraphs = []
        try:
            origin_paragraphs = get_paragraphs(text)
        except Exception as e:
            logger.error('获取段落结构异常 {}'.format(e))
        # 第一段后加上随机模板句子
        new_paragraphs = origin_paragraphs
        # print(new_paragraphs)
        new_paragraphs[0] = new_paragraphs[0] + random_sentence
        # 恢复新的段落
        new_text = ""
        for i, paragraph in enumerate(new_paragraphs):
            if paragraph.strip():
                # print(paragraph)
                new_text += paragraph + "\n"
        # 新的段落为new_text
        data["content"] = new_text
        keyword_layout_tag_list.append("套话模板添加")
        data["keyword_layout_tag"] = keyword_layout_tag_list
    return data

def put_es_data(data, task_data):
    """
    数据匹配传入草稿箱
    :param data: 与dw_article中document相比，多了keyword_layout_tag字段
    :param task_data: 多了店铺传过来的任务字段
    :return:
    """
    data.update(task_data)
    print(data)

    # 插入es
    @retry(stop_max_attempt_number=3)
    def es_retry():
        try:
            es.put_pro(indexs="dw_ai_article", data=[data], id_field="rowkey")
        except Exception as e:
            logger.exception(msg=e)
    es_retry()



def run():
    pass
    w_A = "上海"
    w_B = ""
    w_C = "盾牌"
    w_D = "哪家好"
    business_category = "B2B"
    industry_l2 = ""
    task_data = {
        "task_id": "123456",
        "task_create_time": "2020-12-21 20:16:15",
        "compound_words_id": "abc123",
        "compound_words_type": "ACD",
        "compound_words": "上海盾牌哪家好",
        "root_A": "上海",
        "root_B": "",
        "root_C": "盾牌",
        "root_D": "哪家好"
    }
    w_A = w_A.strip()
    w_B = w_B.strip()
    w_C = w_C.strip()
    w_D = w_D.strip()
    # 检索到符合的内容
    data = get_data_match_title(w_A=w_A, w_B=w_B, w_C=w_C, w_D=w_D, industry_l2=industry_l2, business_category=business_category)
    # print(data)
    # 清洗和替换地名，标题和正文布词
    if data:
        data = modify_title(data=data, w_A=w_A, w_B=w_B, w_C=w_C, w_D=w_D)
        data = modify_content(data=data, w_A=w_A, w_B=w_B, w_C=w_C, w_D=w_D)
    print(data)
    # 数据重新入库，增加任务相关字段
    put_es_data(data=data, task_data=task_data)


if __name__ == "__main__":
    # es_back = es.search('')
    search_first_words = ["1", "2", "3"]
    print("-".join(search_first_words))
    print(cs.create_manual_sentence(target_word="上海黄金回收"))

    text1 = "上海南京黄金回收北京哪家好？"
    text2 = clean_location(text1)
    print(text2)

    print(replace_location(text=text1, target_word=""))
    print(cs.create_manual_sentence("盾牌"))
    run()