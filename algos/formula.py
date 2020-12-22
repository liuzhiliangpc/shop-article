#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: algo_keyword_layout.py
@time: 2020/12/21 11:18
@desc:
"""
import os
import numpy as np

class CustomSentence(object):
    def __init__(self):
        self.sentence_list = self.load_origin_sentence_file()

    def create_manual_sentence(self, target_word):
        """
        随机一个套话模板替换为目标关键词
        :param target_word:
        :return:
        """
        # 随机产生一个待改造的句子索引
        random_indexs = sorted(np.random.choice(len(self.sentence_list), 1, replace=False))
        sentence_choice = self.sentence_list[random_indexs[0]]%target_word # 占位符替换
        return sentence_choice

    # 抽象时改为初始化变量
    def load_origin_sentence_file(self):
        """
        加载套话模板
        :return:
        """
        file_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        manual_sentence_path = os.path.join(file_path, "algos/manual_sentences")
        with open(os.path.join(manual_sentence_path, 'general_sentence'), mode="r", encoding="utf-8") as fr:
            ret = [line.strip().strip('\n') for line in fr.readlines()]
        return ret

    # def clean_location(text):
    #     """
    #     文本清洗所有地名，不替换
    #     :param text:
    #     :return:
    #     """
    #     loc_list = ET.find_loc(text=text)
    #     for _, loc in enumerate(loc_list):
    #         text = re.sub(loc, "", text)
    #     return text
    #
    # def replace_location(text, target_word=""):
    #     """
    #     文本替换所有地域词为目标地域词
    #     :param text: 文本
    #     :param target_word: 目标地域词
    #     :return:
    #     """
    #     loc_list = ET.find_loc(text=text)
    #     loc_match_start_end_list = []
    #     for loc in loc_list:
    #         print([[substr.start(), substr.end()] for substr in re.finditer(loc, text)])
    #         loc_match_start_end_list.extend([[substr.start(), substr.end()] for substr in re.finditer(loc, text)])
    #     # 合并地名列表，小于一个字符差异的也会被合并
    #     loc_match_start_end_list = list_merge(loc_match_start_end_list)
    #     print(loc_match_start_end_list)
    #     # 地名合并后替换
    #     text = custom_replace_word(text=text, match_start_end_list=loc_match_start_end_list, target_word=target_word)
    #     return text
    #
    #
    # def list_merge(intervals):
    #     """
    #     合并区间，公共函数
    #     :param intervals: 列表[[0,1], [3,6]]
    #     :return:
    #     """
    #     intervals.sort(key=lambda x: x[0])
    #     merged = []
    #     for interval in intervals:
    #         # 如果列表为空，或者当前区间与上一区间不重合，直接添加
    #         # if not merged or merged[-1][1] < interval[0]:
    #         if not merged or (interval[0] - merged[-1][1]) > 1:
    #             merged.append(interval)
    #         else:
    #             # 否则的话，我们就可以与上一区间进行合并，合并中间一个字符的
    #             merged[-1][1] = max(merged[-1][1], interval[1])
    #     return merged
    #
    #
    # def custom_replace_word(text, match_start_end_list, target_word):
    #     """
    #     自定义替换指定位置的词语，正文替换地名和正文布词公共函数
    #     :param text: 文本
    #     :param match_start_end_list: 需要替换的位置，格式:[[a,b]]嵌套列表
    #     :param target_word: 替换后目标值
    #     :return:
    #     """
    #     # 尝试简化方案，仅一个list[list]
    #     text_split = []
    #     for index, item in enumerate(match_start_end_list):
    #         # 首字符处理
    #         if index == 0 and item[0] != 0:
    #             text_split.append(text[:item[0]])
    #         # 中间列表
    #         if index > 0:
    #             text_split.append(text[match_start_end_list[index - 1][1]:item[0]])
    #         # 替换：text[item[0]:item[1]]位置替换为目标关键词
    #         text_split.append(target_word)
    #         # 尾字符处理
    #         if index + 1 == len(match_start_end_list):
    #             if item[1] < len(text):
    #                 text_split.append(text[item[1]:])
    #     text = "".join(text_split)  # 随机换词后的正文
    #     return text
    #
    #
    # def modify_title(data, w_A, w_B, w_C, w_D):
    #     """
    #     标题布词
    #     :param data: es数据体
    #     :param w_A: 词根A
    #     :param w_B: 词根B
    #     :param w_C: 词根C
    #     :param w_D: 词根D
    #     :return:
    #     """
    #     # 清洗地域词
    #     data["title"] = clean_location(text=data["title"])
    #     # 标题布词，布设第一个位置
    #     if data.get("keyword_layout_tag") and "标题匹配BCD词" in data.get("keyword_layout_tag"):
    #         # TODO 待测试标题匹配结果
    #         temp_word = w_B if w_B else w_C
    #         new_title = re.sub(
    #             temp_word, w_A + temp_word, data["title"], count=1
    #         )  # 仅替换匹配到的第一个词
    #     elif data.get("keyword_layout_tag") and "标题匹配C词" in data.get("keyword_layout_tag"):
    #         # new_title = (w_A + w_B + w_C + w_D) + "，" + data["title"] # 需求调整，目标关键词，原标题
    #         new_title = re.sub(
    #             w_C, (w_A + w_B + w_C + w_D), data["title"], count=1
    #         ) # 匹配到的第一个C词替换为目标关键词
    #     else:
    #         # 这一步理论上不会遇到，除非前词语有歧义，被清洗了
    #         logger.warning("[关键词布词] [标题未匹配到C词]")
    #         new_title = data["title"]
    #     data["title"] = new_title
    #     return data
    #
    #
    # def modify_content(data, w_A, w_B, w_C, w_D):
    #     """
    #     正文布词
    #     :param data: es数据体
    #     :param w_A: 词根A
    #     :param w_B: 词根B
    #     :param w_C: 词根C
    #     :param w_D: 词根D
    #     :return:
    #     """
    #     # 通过地域词A替换所有不连续的地域词
    #     data["content"] = replace_location(text=data["content"], target_word=w_A)
    #     # 正文布词
    #     query_word_BCD = w_B + w_C + w_D
    #     query_word_C = w_C
    #     target_word = w_A + w_B + w_C + w_D
    #     res_BCD = re.search(query_word_BCD, data)
    #     res_C = re.search(query_word_C, data)
    #     text = data["content"]
    #     if res_BCD:
    #         # 所有BCD位置
    #         query_match_start_end_list = [[substr.start(),substr.end()] for substr in re.finditer(query_word_BCD, text)]
    #         # 随机选取一个位置，不允许重复的随机值
    #         random_indexs = sorted(np.random.choice(len(query_match_start_end_list), 1, replace=False))
    #         # 随机位置映射到原列表
    #         query_match_start_end_list = [query_match_start_end_list[index] for index in random_indexs]
    #         data["content"] = custom_replace_word(text=text, match_start_end_list=query_match_start_end_list, target_word=target_word)
    #         data["keyword_layout_tag"] = data["keyword_layout_tag"].append("正文匹配BCD词")
    #     elif not res_BCD and res_C:
    #         # 所有C位置
    #         query_match_start_end_list = [[substr.start(),substr.end()] for substr in re.finditer(query_word_C, text)]
    #         # 随机选取一个位置，不允许重复的随机值
    #         random_indexs = sorted(np.random.choice(len(query_match_start_end_list), 1, replace=False))
    #         # 随机位置映射到原列表
    #         query_match_start_end_list = [query_match_start_end_list[index] for index in random_indexs]
    #         data["content"] = custom_replace_word(text=text, match_start_end_list=query_match_start_end_list, target_word=target_word)
    #         data["keyword_layout_tag"] = data["keyword_layout_tag"].append("正文匹配C词")
    #     else:
    #         # 人工段落创建一个随机模板句子，并埋入目标关键词
    #         random_sentence = create_manual_sentence(target_word=target_word)
    #         # 获取段落结构
    #         origin_paragraphs = []
    #         try:
    #             origin_paragraphs = get_paragraphs(text)
    #         except Exception as e:
    #             logger.error('获取段落结构异常 {}'.format(e))
    #         # 第一段后加上随机模板句子
    #         new_paragraphs = origin_paragraphs
    #         new_paragraphs[0] = new_paragraphs[0] + random_sentence
    #         # 恢复新的段落
    #         new_text = ""
    #         for i, paragraph in enumerate(new_paragraphs):
    #             if paragraph.strip():
    #                 # print(paragraph)
    #                 new_text += paragraph + "\n"
    #         # 新的段落为new_text
    #         data["content"] = new_text
    #         data["keyword_layout_tag"] = data["keyword_layout_tag"].append("套话模板添加")
    #         return data

