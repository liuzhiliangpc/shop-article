#coding=utf8

import re
import json
import copy
import datetime
import numpy as np
from datetime import datetime, timedelta
from retrying import retry
from milvus import IndexType, MetricType, Status

def filter_text(text):
    if not text :
        return None

    # 去除\x01这类乱码数据
    text = text.replace("\x01","").replace("\x02","").replace("\x03","").replace("\x04","").replace("\x05","").replace("\x06","").replace("\x07","").replace("\x08","").replace("\x09","")
    # 去除不间断空白符 &nbsp \xa0
    text = text.replace(u'\xa0', u' ')
    # 去除全角空白符 \u3000
    text = text.replace("\u3000", "")
    # 自动忽略诸如<200b>排版编码（实际看不见，vim下可以看见）
    text = text.encode("utf-8", "ignore")
    text = text.decode("utf-8", "replace")
    text = text.replace('\u200b', '')  # 一种方式是gbk转换，但表情会丢失
    text = text.replace('\n', "").replace("\r", "").replace("\t", "").strip()

    data_line = "{}".format(text)
    return data_line

def get_mid_date(date_pre):
    # 返回起始时间到当前时间之间的所有日期
    date_start = datetime.strptime(date_pre, "%Y-%m-%d")
    date_end = datetime.now()
    date_list = []
    while 1:
        if date_start <= date_end:
            date_list.append(date_start.strftime("%Y-%m-%d"))
            date_start += timedelta(days=1)
        else:
            break
    return date_list

def url_extract(text):
    regex = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        )
    urls = re.findall(regex, text)
    return urls

def two_list_join(l1, l2, level=1, sort=True):
    if level == 1:
        helper_dict = {}
        for l in l1:
            key = l[0]
            value = l[1]
            if not key in helper_dict:
                helper_dict[key] = 0
            helper_dict[key] += value
        for l in l2:
            key = l[0]
            value = l[1]
            if not key in helper_dict:
                helper_dict[key] = 0
            helper_dict[key] += value
        ret = sorted(list(helper_dict.items()), key=lambda kv:kv[1], reverse=True)
        return ret

    if level == 2:
        helper_dict = {}
        for l in l1:
            key = l[0]
            value = l[1]
            if not key in helper_dict:
                helper_dict[key] = value
            else:
                new = two_list_join(helper_dict[key], value)
                helper_dict[key] = new

        for l in l2:
            key = l[0]
            value = l[1]

            if not key in helper_dict:
                helper_dict[key] = value
            else:
                new = two_list_join(helper_dict[key], value)
                helper_dict[key] = new

        ret = list(helper_dict.items())
        return ret
    return None

@retry(stop_max_attempt_number=3)
def get_bert_client(bert_client, logger,texts=None):
    sentence_vecs = None
    try:
        # sentence_vec = bert_client.get_sentence_vec_batch(sentence=text).tolist()
        sentence_vecs = bert_client.get_sentence_vec_batch(sentences=texts).tolist()
        logger.info("[重复度检测] [获取bert句向量成功]")
    except Exception as e:
        logger.error("[重复度检测] [获取bert句向量失败：{}]".format(e))
    return sentence_vecs

@retry(stop_max_attempt_number=3)
def calculate_simhash(cu_sim, logger,text=None):
    sentence_vec = None
    try:
        sentence_vec = cu_sim.custom_simhash(text=text)
        logger.info("[重复度检测] [计算simhash句向量成功]")
    except Exception as e:
        logger.error("[重复度检测] [计算simhash句向量失败：{}]".format(e))
    return sentence_vec

@retry(stop_max_attempt_number=3)
def search_milvus(mymilvus, vectors, logger, collection_name=None):
    results = None
    try:
        results = mymilvus.search(vectors=vectors, collection_name=collection_name)
        # print(results)
        logger.info("[重复度检测] [查询milvus数据库成功]")
    except Exception as e:
        logger.error("[重复度检测] [查询milvus数据库失败：{}]".format(e))
    return results

@retry(stop_max_attempt_number=3)
def insert_milvus(mymilvus, vectors, ids, partition_tag, logger, collection_name=None, type=None):
    status = False
    try:
        # status = mymilvus.insert(vectors=vectors, ids=ids, partition_tag=partition_tag,
        #                 collection_name=collection_name)  # mymilvus.insert有单次插入大小的限制
        if type == 'float':
            status = mymilvus.insert(vectors=vectors, ids=ids, partition_tag=partition_tag, \
                                     indextype=IndexType.IVF_PQ, \
                                     index_param={'nlist': 16384, 'm':16}, collection_name=collection_name)
        elif type == 'binary':
            status = mymilvus.insert(vectors=vectors, ids=ids, partition_tag=partition_tag, \
                                     indextype=IndexType.IVF_FLAT, index_param={'nlist': 16384}, \
                   collection_name=collection_name)

        if status:
            logger.info("[数据批量插入] [Add vectors successfully! Id are {}]".format(ids))
        else:
            raise Exception("insert vectors error")
    except Exception as e:
        logger.error("[数据批量插入] [Add vectors error! Id are {0}, error is {1}]".format(ids, e))
    return status

@retry(stop_max_attempt_number=3)
def delete_milvus(mymilvus, ids, logger, collection_name=None):
    status = False
    try:
        status = mymilvus.delete(id_array=ids, collection_name=collection_name)
        if status:
            logger.info("[数据批量删除] [Delete vectors successfully! Id are {}]".format(ids))
        else:
            raise Exception("delete vectors error")
    except Exception as e:
        logger.error("[数据批量删除] [Delete vectors error! Id are {0}, error is {1}]".format(ids, e))
    return status
# def calculate_similarity(cu_sim, hash_distance):
#     """Calculate how similar this distance.
#     Returns a float from 0.0 to 1.0
#     """
#     return float(cu_sim.f - hash_distance) / cu_sim.f

# MongoDB数据库id 由24个十六进制数组组成的字符串
# 十进制数值转换为16进制字符串
def decimal2hex(id):
    # return str(hex(int(id,10)))  # 字符串前会多出'0x'，replace掉也可以
    return "{:x}".format(id)

# MongoDB数据库id 由24个十六进制数组组成的字符串
# 一个是12字节的id，mysql是8字节id,存储时截断了机器码和进程码
# 16进制字符串转换为十进制整型
def hex2decimal(text):
    cut_text = text[0:8]+ text[16:24]
    return int(cut_text, 16)


if __name__ == "__main__":
    text = "Repost 这是一条#自带声音#的微博：土拨鼠的逃生新技能get，由#华为Mate20#友情提供 http://t.cn/EyAcRWr "
    urls = url_extract(text)
    print(urls)

    l1 = [(2,1), (5,1), (1,1)]
    l2 = [(1,1), (3,1), (9,1)]

    ll1 = [(2,[(1,1),(2,1)]), (1,[(1,1),(2,1)])]
    ll2 = [(1,[(1,1),(2,1)]), (3,[(1,1),(2,1)])]

    print(two_list_join(l1,l2))
    print(two_list_join(ll1,ll2, level=2))

