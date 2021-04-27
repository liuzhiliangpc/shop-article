#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: baixing_elasticsearch.py
@time: 2020/12/14 20:52
@desc: 百姓POST方式读取ElasticSearch接口，最新版本时间2021-04-19
"""
# from elasticsearch import Elasticsearch
import os
# import copy
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
    # print(insert_host)
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
        """查询操作基础函数""" # TODO 基于选项合并为一个基础函数
        params = data
        r = requests.post(
            url="http://%s/api/es-query/queryByDsl" % (search_host),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def put(self, data):
        """插入操作基础函数"""
        params = data
        r = requests.post(
            url="http://%s/api/es-write/add" % (insert_host),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def update(self, data):
        """更新操作基础函数"""
        params = data
        r = requests.post(
            url="http://%s/api/es-write/update" % (insert_host),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def delete(self, data):
        """删除操作基础函数"""
        params = data
        r = requests.post(
            url="http://%s/api/es-write/delete" % (insert_host),
            json=params,
            verify=False,
        )
        ret = json.loads(r.text)
        return ret

    def update_by_query(self, data):
        """查询更新操作基础函数"""
        params = data
        r = requests.post(
            url="http://%s/api/es-write/updateByQuery" % (insert_host),
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
        logger.info(status)
        return status

    def update_pro(self, indexs, data, id_field):
        if not indexs:
            indexs = self.__index
        if not id_field:
            id_field = self.__id_field

        query = {"index": indexs, "docs": data, "idField": id_field}
        status = self.update(data=query)
        logger.info(status)
        return status

    def update_version(self, indexs, data,id_field, _primary_term, _seq_no):
        if not indexs:
            indexs = self.__index
        if not id_field:
            id_field = self.__id_field

        query = {"index": indexs, "docs": data, "idField": id_field, "primaryTerm": _primary_term, "seqNo": _seq_no}
        status = self.update(data=query)
        logger.info(status)
        return status

    def delete_pro(self, indexs, data, id_field):
        if not indexs:
            indexs = self.__index
        if not id_field:
            id_field = self.__id_field

        query = {"index": indexs, "docs": data, "idField": id_field}
        status = self.delete(data=query)
        logger.info(status)
        return status

    def __loadQuery(self, index, query_id, paras=()):
        """支持search_pro使用，后续需和__loadQuery_scroll合并"""
        q_file = os.path.join(self.query_dir, "%s.json" % query_id)
        q = ""
        with open(q_file, "r", encoding="utf-8") as fin:
            for line in fin:
                q = q + line.strip()
        q = q % paras  # 核心语句占位符替换
        query_json = json.dumps(q, ensure_ascii=False)  # dsl部分转换为json字符串
        # print(query_json)
        query = """{"index": "%s","dsl": %s, "routes":"?timeout=120s"}""" % (
            index,
            query_json,
        )  # 替换占位符
        # print(query)
        query = json.loads(query)
        # print(query)
        return query

    def update_by_query_pro(self, query_id, paras=[], indexs=None):
        """查询更新高级接口"""
        if not indexs:
            indexs = self.__index
        query_id = str(query_id)
        if paras and type(paras) == type([]):
            paras = tuple(paras)

        try:
            query_json = self.__loadQuery_scroll(query_id=query_id, paras=paras)
        except Exception as e:
            logger.warning("Load update_by_query file error. Check file %s.json; %s" % (query_id, e))
            return []
        # 复杂的还支持scroll默认5min、scroll_size默认1000滚动大小等,timeout不设置默认是1min。
        query = """{"index": "%s","dsl": %s, "routes":"?timeout=120s"}""" % (
            indexs,
            query_json,
        )  # 替换占位符
        query = json.loads(query)
        ret = self.update_by_query(data=query)
        return ret

    def search_scroll_pro(self, query_id, paras=[], indexs=None, scroll=False):
        # 带游标es查询接口，默认不带游标
        if not indexs:
            indexs = self.__index
        query_id = str(query_id)
        if paras and type(paras) == type([]):
            paras = tuple(paras)

        try:
            query_json = self.__loadQuery_scroll(query_id=query_id, paras=paras)
        except Exception as e:
            logger.warning(
                "Load query file error. Check file %s.json; %s" % (query_id, e)
            )
            return []
        # 不使用游标方式
        if not scroll:
            query = """{"index": "%s","dsl": %s, "routes":"?timeout=120s"}""" % (
                indexs,
                query_json,
            )  # 替换占位符
        # 使用游标方式
        else:
            query = """{"index": "%s","dsl": %s, "routes":"?timeout=120s&scroll=10m"}""" % (
                indexs,
                query_json,
            )  # 替换占位符
        query = json.loads(query)
        ret = self.search(data=query)
        return ret

    def scroll(self, indexs, scroll_id):
        # 通过游标方式查询，返回的数据中需解析scroll_id，以便后面使用，首次使用先用search_scroll_pro，后用scroll命令
        query = """{"index": "%s","dsl": "{}", "routes":"?scroll=5m", "scrollId":"%s"}""" % (
            indexs,
            scroll_id,
        )  # 替换占位符
        query = json.loads(query)
        ret = self.search(data=query)
        return ret

    def __loadQuery_scroll(self, query_id, paras=()):
        """供search_scroll_pro使用，后面补充对update_by_query支持"""
        q_file = os.path.join(self.query_dir, "%s.json" % query_id)
        q = ""
        with open(q_file, "r", encoding="utf-8") as fin:
            for line in fin:
                q = q + line.strip()
        q = q % paras  # 核心语句占位符替换
        query_json = json.dumps(q, ensure_ascii=False)  # dsl部分转换为json字符串
        return query_json



if __name__ == "__main__":
    pass
    # # 官方接口，查询测试一：可再封装一层
    # es = Elasticsearch(hosts="172.30.1.98:9200")
    # query = """{"query":{
    #     "match_all": {}
    # }
    # }"""
    # ret = es.search(index="cat_material_test_20200618", body=query, request_timeout=120)
    # query_insert = {
    #     "category": "fangwuweixiu",
    #     "content": "lzl换气扇是家里很重要的一个设施",
    #     "id": "5f72eb8602ec996bc67d6000",
    #     "title": "lzl换气扇的种类有哪些 排气扇怎么安装",
    # }
    # # 官方执行插入成功
    # # es.index(index="cat_material_test_20200618", id="5f72eb8602ec996bc67d6000", body=query_insert, request_timeout=120)
    #
    # del es
    # print(ret)
    # 百姓接口，查询测试二：
    es = BXElasticSearch()
    query_dsl = """{"query":{
        "match_all": {}
    }
    }"""  # dsl部分后续也用占位符
    query_json = json.dumps(query_dsl, ensure_ascii=False) # dsl部分转换为json字符串

    # print(query_json)
    query = """{"index": "dw_article","dsl": %s, "routes":"?timeout=120s"}"""%(query_json) # 替换占位符
    query = json.loads(query)
    # print(query)
    # print(es.search(query))

    # 上述为百姓接口search_pro功能
    # print(es.search_pro(query_id="00100", paras=["match_all"]))
    res_scroll = es.search_scroll_pro(query_id="test", paras=["KTV"], indexs="dw_article", scroll=True)
    print(res_scroll)
    scroll_flag = res_scroll.get("scrollId")
    print("-------------------------------------------")
    print(scroll_flag)
    res_scroll_batch2 = es.scroll(indexs="dw_article", scroll_id=scroll_flag)
    # query = """{"index": "dw_article","dsl": "{}", "routes":"?scroll=5m", "scrollId":"%s"}""" % (
    #     scroll_flag,
    # )  # 替换占位符
    # query = json.loads(query)
    # ret = es.search(data=query)
    print("-------------------------------------------")
    # print(ret)
    print(res_scroll_batch2)


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
    data_back = get_es_data("021053866831407d4b23e37a62fbff")
    # print(data_back.get("content"))

    # 数据插入（旧数据完全替换方式）
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

    # 不带版本更新
    data3 = [{"download_count": 1, "rowkey": "01008581367470ff5507afdc28ac77"}]
    # es.update_pro(indexs="dw_article", data=data3, id_field="rowkey")

    # 带版本更新
    data4 = [{"download_count": 1, "rowkey": "0100719438947046f899062c8cd395"}]
    # es.update_version(indexs="dw_article", data=data5, id_field="rowkey", _primary_term=13, _seq_no=90607)

    # 百姓接口数据删除
    data5= [{"article_id": "01105387945260d6ec9079e2d62d8f"}, {"article_id": "011053879452606b45362717378058"}, {"article_id": "011053879452603f8b095b7d978bc8"}]
    # es.delete_pro(indexs="dw_ai_article", data=data4, id_field="article_id")

    # 查询更新操作
    rowkey_list_str = json.dumps(["200"], ensure_ascii=False)
    data6 = es.update_by_query_pro(query_id="update_by_query_test", paras=[16, rowkey_list_str], indexs="dw_ai_article")
    print(data6)
    # curl --location --request POST 'http://api.baixing.com.cn/api/es-query/queryByDsl?timeout=3s' --header 'Content-Type: application/json' --data-raw '{    "index": "dw_article",    "dsl": "{\"query\":{\"term\":{\"rowkey\":\"02105296927100823e3b122b0ec457\"}},\"from\":0,\"size\":20}",    "routes": "?timeout=3s"}'