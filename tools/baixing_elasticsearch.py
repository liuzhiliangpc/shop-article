#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: baixing_elasticsearch.py
@time: 2020/12/14 20:52
@desc: 百姓POST方式读取ElasticSearch接口，最新版本时间
"""
from elasticsearch import Elasticsearch
import os
import copy
from tools.log import logInit
import json
import requests
from config import conf

mode = conf.getConf('elasticsearch', 'mode')
if mode == "dev":
    ip = conf.getConf('elasticsearch', 'dev_ip')
    search_port = conf.getConf('elasticsearch', 'dev_search_port')
    insert_port = conf.getConf('elasticsearch', 'dev_insert_port')
    search_host = "%s:%s" % (ip, search_port)
    insert_host = "%s:%s" % (ip, insert_port)
elif mode == "prod":
    HOST = conf.getConf('elasticsearch', 'prod_host')
    search_host = HOST
    insert_host = HOST
elif mode == "prod_dev":
    ip = conf.getConf('elasticsearch', 'prod_dev_host')
    search_port = conf.getConf('elasticsearch', 'dev_search_port')
    insert_port = conf.getConf('elasticsearch', 'dev_insert_port')
    search_host = "%s:%s" % (ip, search_port)
    insert_host = "%s:%s" % (ip, insert_port)


logger = logInit("ES_APP")
# 方案一：利用架构部分开发的post接口
class BXElasticSearch(object):
    def __init__(self):
        self.__index = "dw_article"  # 缺省索引
        self.__id_field = "rowkey"  # 缺省id字段名
        self.query_dir = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "../jsons/es_query/"
        )

    def search(self, data):
        params = data
        r = requests.post(
            url="http://%s/api/es-query/queryByDsl" % (search_host),
            # url="http://%s:%s/api/es-query/queryByDsl" % (ip, search_port),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def put(self, data):
        params = data
        r = requests.post(
            url="http://%s/api/es-write/add" % (insert_host),
            # url="http://%s:%s/api/es-write/add" % (ip, insert_port),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def update(self, data):
        params = data
        r = requests.post(
            url="http://%s/api/es-write/update" % (insert_host),
            # url="http://%s:%s/api/es-write/update" % (ip, insert_port),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def delete(self, data):
        params = data
        r = requests.post(
            url="http://%s/api/es-write/delete" % (insert_host),
            # url="http://%s:%s/api/es-write/delete" % (ip, insert_port),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def search_pro(self, query_id, paras=[], indexs=None):
        # 封装上层查询接口
        if not indexs:
            indexs = self.__index
        query_id = str(query_id)
        if paras and type(paras) == type([]):
            paras = tuple(paras)

        try:
            query = self.__loadQuery(index=indexs, query_id=query_id, paras=paras)
        except Exception as e:
            logger.warning(
                "Load query file error. Check file %s.json; %s" % (query_id, e)
            )
            return []
        ret = self.search(data=query)
        return ret

    def put_pro(self, indexs, data, id_field):
        if not indexs:
            indexs = self.__index
        if not id_field:
            id_field = self.__id_field

        query = {"index": indexs, "docs": data, "idField": id_field}
        status = self.put(data=query)
        print(status)

    def update_pro(self, indexs, data, id_field):
        if not indexs:
            indexs = self.__index
        if not id_field:
            id_field = self.__id_field

        query = {"index": indexs, "docs": data, "idField": id_field}
        status = self.update(data=query)
        print(status)

    def delete_pro(self, indexs, data, id_field):
        if not indexs:
            indexs = self.__index
        if not id_field:
            id_field = self.__id_field

        query = {"index": indexs, "docs": data, "idField": id_field}
        status = self.delete(data=query)
        print(status)

    def __loadQuery(self, index, query_id, paras=()):
        q_file = os.path.join(self.query_dir, "%s.json" % query_id)
        q = ""
        with open(q_file, "r", encoding="utf-8") as fin:
            for line in fin:
                q = q + line.strip()
        q = q % paras  # 核心语句占位符替换
        query_json = json.dumps(q, ensure_ascii=False)  # dsl部分转换为json字符串
        query = """{"index": "%s","dsl": %s, "routes":"?timeout=120s"}""" % (
            index,
            query_json,
        )  # 替换占位符
        query = json.loads(query)
        return query


if __name__ == "__main__":
    pass
    # 官方接口，查询测试一：可再封装一层
    es = Elasticsearch(hosts="172.30.1.98:9200")
    query = """{"query":{
        "match_all": {}
    }
    }"""
    ret = es.search(index="cat_material_test_20200618", body=query, request_timeout=120)
    query_insert = {
        "category": "fangwuweixiu",
        "content": "lzl换气扇是家里很重要的一个设施",
        "id": "5f72eb8602ec996bc67d6000",
        "title": "lzl换气扇的种类有哪些 排气扇怎么安装",
    }
    # 官方执行插入成功
    # es.index(index="cat_material_test_20200618", id="5f72eb8602ec996bc67d6000", body=query_insert, request_timeout=120)

    del es
    # print(ret)
    # 百姓接口，查询测试二：
    es = BXElasticSearch()
    # query_dsl = """{"query":{
    #     "match_all": {}
    # }
    # }"""  # dsl部分后续也用占位符
    # query_json = json.dumps(query_dsl, ensure_ascii=False) # dsl部分转换为json字符串
    # print(query_json)
    # query = """{"index": "ad_uid","dsl": %s, "routes":"?timeout=120s"}"""%(query_json) # 替换占位符
    # print(query)
    # query = json.loads(query)
    # print(query)
    # print(es.search(query))

    # 上述为百姓接口search_pro功能
    # print(es.search_pro(query_id="00100", paras=["match_all"]))

    def get_es_data(query_str):
        # 获取ES数据库数据
        es_back = es.search_pro(
            query_id="00102",
            paras=[
                query_str,
            ],
        )
        # print(es_back)
        es_response = es_back.get("restResponse")
        datas = {}
        if es_response:
            count = es_response["hits"]["total"]["value"]
            if count > 0:
                datas = es_response["hits"]["hits"][0]["_source"]  # 取匹配的第一条数据
        return datas
    data_back = get_es_data("01006499790470d104384280346b1d")
    print(data_back.get("content"))
    print(type(data_back))


    # 百姓接口数据插入
    data1 = [{"id": "test_5"}]
    idField = "id"
    query = {"index": "dw_article", "docs": data1, "idField": idField}
    print(query)
    # status = es.put(data=query)
    # print(status)
    # 上述为百姓接口put_pro功能
    # data2 = [{"id": "test_6"}]
    # es.put_pro(indexs="api_test", data=data2, id_field=idField)
    data2 = [{
        "category": "fangwuweixiu",
        "content": "lzl换气扇是家里很重要的一个设施",
        "id": "5f72eb8602ec996bc67d6000",
        "title": "lzl换气扇的种类有哪些 排气扇怎么安装",
    }]

    idField = "rowkey"
    data2 = [{
	"vector_id": None,
	"site_name": "四通搬家",
	"language": "zh",
	"category": "专业资讯",
	"source_url": "https://www.stbj.cn/1729.html",
	"published_time": None,
	"crawler_time": "2020-12-17 16:34:23",
	"spider_time": "2020-12-18 19:48:36",
	"rowkey": "0200539191123059d608efa1eb4332",
	"crawler_rowkey": "",
	"crawler_keywords": [],
	"description": "",
	"keywords": [],
	"title": "小型搬家与居民搬家的区别",
	"content": "搬家是我们在日常当中经常能遇到的事，我们生活种常见的搬家方式可以分为：小型搬家、居民搬家。那么小型搬家啊和居民搬家的区别是什么。小型搬家和地区居民搬家的区别主要集中体现在所搬运东西的多少。小型搬家，是指搬家的过程中需要搬运东西少的一种方式。在一般情况下，学生搬家，临时租就是小型搬家，由于较少的物品，移动小，相对来说，整个搬家过程要简单一点，所需的时间会相对少一些。居民搬家，则更显复杂一些。因为很多人平时生活中的各种电器，家具，日用品，家具拆装，最后再把所有的东西都搬走，我们需要更多的时间和精力。尤其是一些家用电器和一些名贵的家具用品等有其特殊的搬运、拆装方法，相对于小型搬家，居民搬家更为复杂。小搬家与居民搬家的区别也体现在注意事项上。因为在小型搬家的过程中相对较小的东西比较多，会遗漏的东西也多。因此，公司在进行搬运的物品前都会做一个信息列表，搬家的时候可以参照列表上的东西进行分类搬运，就可以避免在搬家过程中，会有很多东西遗漏。居民搬家，因为东西太多，会较为复杂，居民搬家，你需要注意多一点：可以提前告知搬家的公司，自己所在的位置、小区出入口、停车位等信息，便于搬家的公司可以及时找到你的家里面，从而进行物品的搬运。提前做好搬家的准备，对所有家电，生活用品等进行分类，收集，打包零散物品，并告知搬家公司是否需要特别安排，哪些家具属于贵重物品，在搬运过程中需要特别注意，特别是要着重保护贵重物品，如果可能的话，在纸箱上做一些标记或警示标志等..合理安排搬家的时间和行程，避免在上下班的高峰发展时期搬家，那样会浪费搬家的时间。\n",
	"author": "",
	"editor": "",
	"pv": 0,
	"comment": 0,
	"forward": 0,
	"like": 0,
	"crawler_tags": [],
	"navigation": [],
	"level": "A",
	"industry_l1": "",
	"industry_l2": "",
	"industry_l3": "",
	"crawler_industry_l1": "",
	"crawler_industry_l2": "",
	"crawler_industry_l3": "居民搬家",
	"crawler_batch_tag": "V20201218",
	"business_category": "B2C",
	"image_urls": [],
	"image_oss_urls": [],
	"domain": "stbj.cn",
	"hostname": "www.stbj.cn",
	"nearest_vec_id": None,
	"nearest_vec_sim": 0,
	"status": 0,
	"download_count": 0,
	"is_used": 0
}]
    # es.put_pro(indexs="dw_article", data=data2, id_field=idField)

    # 百姓接口数据更新
    # data3 = [{"download_count": 1, "rowkey": "01008581367470ff5507afdc28ac77"}]
    # es.update_pro(indexs="dw_ai_article", data=data3, id_field="rowkey")

    # 百姓接口数据删除
    # data4 = [{"id": "test_6"}]
    # es.delete_pro(indexs="api_test", data=data4, id_field=idField)