#!/usr/bin/env python3
# encoding: utf-8

import os
import pandas as pd
from tools.mypsycopg2 import Mypsycopg2
from core.core import logger
import json

class ShopTaskQuery:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.task_record_pkl = os.path.join(dir_path, 'tasks/task.pkl')
        self.db = 'presto'

    def request_format(self, data):
        """
        request_format 可实现对店铺组合词的接收与存储
        :param data:
        {
            "task_id": "202018",
            "task_create_time": "2020-12-08 10:03:21",
            "business_category": "B2B",
            "industry_l2": "休闲娱乐",
            "task_nums": 2,
            "data": [{
              "compound_words_id": "123",
              "compound_words_type": "ABC",
              "compound_words": "上海刑事律师",
              "root_A": "上海",
              "root_B": "刑事",
              "root_C": "律师",
              "root_D": ""
            },
            {
              "compound_words_id": "126",
              "compound_words_type": "ABC",
              "compound_words": "上海诉讼律师",
              "root_A": "上海",
              "root_B": "诉讼",
              "root_C": "律师",
              "root_D": ""
            }
            ]
        }
        task_id 表示发文任务编号，字符串类型；
        task_create_time表示发文任务创建时间，字符串类型，格式："%Y-%m-%d %H:%M:%S";
        business_category 表示文章类型，目前只能为"B2B"或"B2C"，字符串类型；
        industry_l2表示百姓网二级类目，字符串类型；
        task_nums表示所需文章额度， 当前最小为1，最大为200，超过200请拆成多个任务发送，整型。
        data列表内为精选组合词信息。其中，compound_words_id为组合词编号，字符串类型；
            compound_words_type为组合词类型，只能为："AC"、"ABC"、"ACD"、"ABCD"，即必须同时包含"A"和"C"，字符串类型；
            compound_words为组合词词根拼接后的字符串；
            root_A、root_B、root_C、root_D分别为词根A、词根B、词根C、词根D。
        :return:
        {
            "retcode": 0,
            "msg":"操作成功"
        }
        retcode 表示状态码信息，数值类型；
        msg表示说明信息，字符串类型。
        """
        # # 读取
        df_task = pd.read_pickle(self.task_record_pkl).reset_index(drop=True)
        df_task['task_id'] = df_task.apply(lambda r: str(r.task_id), axis=1)
        if df_task.shape[0] > 0:
            # 检查任务编号是否重复
            if data['task_id'] in list(df_task['task_id']):
                return 3
        # df_task.loc[len(df_task)] = [data['task_id'], data, data['task_create_time'],
        #                              data['business_category'], data['task_nums'],
        #                              data['data']]
        # 若无误则存入数据库中
        back_df = update_last_id(data, "shop_tasks")
        os.remove(self.task_record_pkl)
        # save
        back_df.to_pickle(self.task_record_pkl)
        return 0

    def status_tag(self, data):
        r = {}
        rr = self.request_format(data)
        if rr == 0:
            r['retcode'] = 0
            r['msg'] = '操作成功'
        else:
            r['retcode'] = rr
            r['msg'] = '任务编号重复 重写任务编号'
        return r

def update_last_id(params, source, last_id='20201222'):
    # 更新最新的时间到数据记录（会有重复数据，保留以验证流程完整）
    # if source == "ai":
    #     source_table = "ai_process_log"  # ai_process_log表
    # elif source == "manual":
    #     source_table = "manual_process_log"  # manual_process_log表
    # else:
    #     raise ValueError("未定义的数据来源")
    mypg = Mypsycopg2()
    sql = """select * from {}""".format(source)
    # df = mypg.execute(sql)
    params['shop_task_json'] = json.dumps(params, ensure_ascii=False)
    params['status'] = 1
    print(params)
    # insert_sql = """INSERT INTO {} (task_id, task_create_time, business_category, task_nums, shop_task_json, status,
    # industry_l2) VALUES ('798798', '', '', 2, '', 1, '休闲娱乐')""".format(source)
    # insert_sql = "INSERT INTO {} (task_id, task_create_time, business_category, task_nums, shop_task_json, status, " \
    #              "industry_l2) VALUES ({}, {}, {}, {}, {}, {}, {})". \
    #     format(source, params['task_id'], params['task_create_time'], params['business_category'], params['task_nums'],
    #            params['shop_task_json'], params['status'], params['industry_l2'])
    insert_sql = """INSERT INTO {} (task_id, task_create_time, business_category, task_nums, shop_task_json, status, 
    industry_l2) VALUES (%(task_id)s, %(task_create_time)s, %(business_category)s, %(task_nums)s, %(shop_task_json)s, %(status)s, 
    %(industry_l2)s)""".format(source)
    print(insert_sql)
    try:
        mypg.execute(insert_sql, params)
        logger.info("向表{}中更新最新爬虫数据id {}".format(source, last_id))
    except Exception as e:
        logger.error("执行sql语句,向表{}中更新最新爬虫数据id{}失败 {}".format(source, last_id, e))
    mypg.close()
    mypg = Mypsycopg2()
    df = mypg.execute(sql)
    mypg.close()
    return df

def get_shop_task_data(source_table, origin_id=1, last_id=10):
    mypg = Mypsycopg2()
    # query_sql = """SELECT * from %(source_table)s WHERE id between %(gte_id)s and %(lte_id)s"""
    query_sql = """SELECT * from {}""".format(source_table)
    # params = {"source_table": source_table, "gte_id": origin_id, "lte_id": last_id}
    try:
        data = mypg.execute(query_sql)  # 获取当前时间范围内的数据
        logger.info(
            "获取爬取id为{0}到{1}的数据，数据量为{2}".format(origin_id, last_id, data.shape[0])
        )
    except Exception as e:
        logger.error("执行sql语句,从表{}中获取爬取id为{}到{}的数据失败 {}".format(source_table, origin_id, last_id, e))
    # 关闭数据库连接
    mypg.close()
    return data

if __name__ == '__main__':
    shop = ShopTaskQuery()
    d = list([{
                "compound_words_id": "777",  # 组合词id
                "compound_words_type": "ABC",  # 组合词类型
                "compound_words": "上海刑事律师11",  # 组合词
                "root_A": "上海",  # 词根A,缺省为空字符串""
                "root_B": "刑事",  # 词根B
                "root_C": "律师11",  # 词根C
                "root_D": ""      # 词根D
         }, {
                "compound_words_id": "666",  # 组合词id
                "compound_words_type": "ABC",  # 组合词类型
                "compound_words": "上海刑事律师22",  # 组合词
                "root_A": "上海",  # 词根A,缺省为空字符串""
                "root_B": "刑事",  # 词根B
                "root_C": "律师22",  # 词根C
                "root_D": ""  # 词根D
            }])
    data = {
        "task_id": "999999",  # 任务编号id
        "task_create_time": "2020-12-08 10:03:21",  # 任务创建时间
        "business_category": "B2B",  # 任务为B2B类型还是B2C类型（本地服务）
        "task_nums": 2,  # 总的任务请求素材数
        "data": d,
        "industry_l2": "休闲娱乐"
    }
    print(shop.status_tag(data))
    #
    # data = get_shop_task_data("shop_tasks")
    # print(data)


