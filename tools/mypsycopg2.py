#!/usr/bin/env python3
# encoding: utf-8
'''
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: mypsycopg2.py
@time: 2020/11/26 14:27
@desc:当前版本更新到2020年11月26日
'''

import psycopg2
from config import conf
import pandas as pd

class Mypsycopg2():
    def __init__(self):
        self._host = conf.getConf('postgresql', 'HOST')
        self._port = conf.getConf('postgresql', 'PORT')
        # _host = 'bi.baixing.com'
        # _port = 15432
        self.conn = psycopg2.connect(database="dev", user="crawler", password="crawler!@#123", host=self._host, port=self._port)
        # 创建游标对象
        self.cursor = self.conn.cursor()

    def execute(self, sql=None, params=None):
        # 缺省sql时，打印version信息
        if not sql:
            sql = 'select version()'
        # 测试函数，另一种取数方法
        # if 'select' in sql or 'SELECT' in sql:
        #     df = pd.read_sql_query(sql=sql, con=self.conn)
        #     return df
        # 执行语句
        if params:
            self.cursor.execute(sql, params)
        else:
            self.cursor.execute(sql)
        # 事务提交
        self.conn.commit()
        # data = None
        if 'select' in sql or 'SELECT' in sql:
            # 拉取全部数据
            data = self.cursor.fetchall()

            columnDes = self.cursor.description #获取连接对象的描述信息
            columnNames = [columnDes[i][0] for i in range(len(columnDes))]
            df = pd.DataFrame([list(i) for i in data],columns=columnNames)
            return df

    def close(self):
        # 关闭数据库连接
        self.conn.close()


if __name__ == '__main__':
    mypg = Mypsycopg2()
    #
    # # 查询sql
    sql = """select * from shop_task"""
    # # sql = """SELECT id, title, crawler_time FROM "ai_article_liuzhiliang" \
    # #         WHERE to_timestamp(crawler_time, 'YYYY-MM-DD hh24:mi:ss')  \
    # #         between '2020-11-19 23:36:35' and '2020-11-19 23:36:59' LIMIT 1000"""
    # params = {'id': 3,
    #           'task_id': '123444',
    #           'task_create_time': '',
    #           'business_category': '',
    #           'task_nums': 3,
    #           'shop_task_json': '',
    #           'status': 0,
    #           'message': '',
    #           'industry_l2': ''}
    # sql1 = "INSERT INTO shop_task (id, task_id, task_create_time, business_category, task_nums, shop_task_json, status, " \
    #        "message, industry_l2)  VALUES (%s, '%s', '%s', '%s', %s, '%s', %s, '%s', '%s')" % \
    #        (params['id'], params['task_id'], params['task_create_time'], params['business_category'],
    #         params['task_nums'], params['shop_task_json'], params['status'], params['message'], params['industry_l2'])
    # sql1 = """INSERT INTO shop_task (id, task_id, task_create_time, business_category, task_nums, shop_task_json, status, message, industry_l2)  VALUES (4, '564815', '2020-12-08 10:03:21', 'B2B', 2, '', 0, '操作成功', '休闲娱乐')
    # """
    # mypg.execute(sql1)
    data = mypg.execute(sql)
    mypg.close()
    # data = mypg.execute()
    # print("the version of database is {}".format(data))
#     # 创建表格
#
#     sql = """CREATE Table test (id serial4 PRIMARY KEY, num int4,name text)"""
#     # data = mypg.execute(sql)
#
#     # 插入
    #     # \n符号插入
#     # mypg.execute(sql)
#
#     sql = "INSERT INTO test (num, name)  VALUES (%s, E'%s')" %(106, """
#
# <!DOCTYPE html>
#         <meta name="keywords" content="mysql 存储数据时遇到单'引号和双引号问题">
#         <meta name="csdn-baidu-search"  content='{"autorun":true,"install":true,"keyword":"mysql 存储数据时遇到单引号和双引号问题"}'>
#     <meta name="description" content="insert into tableName（id , name , sex）values (1,‘张‘三’ 丰’，‘男’ )这种情况会报错，对于不确定插入字段数据的是单引号还是双引号时，解决方案有2种：insert into tableName（id , name , sex）values (1,‘张\‘三\’ 丰’，‘男’ )----使用转义字符insert into tableName（...">
#     <script src='//g.csdnimg.cn/tingyun/1.8.3/blog.js' type='text/javascript'></script>
#
# """.replace("'","\\'"))
#     # E方式插入
#     # mypg.execute(sql)
#
#     sql = "INSERT INTO test (num, name)  VALUES (%s, '%s')" % (107, """
#
#     <!DOCTYPE html>
#             <meta name="keywords" content="mysql 存储数据时遇到单'引号和双引号问题">
#             <meta name="csdn-baidu-search"  content='{"autorun":true,"install":true,"keyword":"mysql 存储数据时遇到单引号和双引号问题"}'>
#         <meta name="description" content="insert into tableName（id , name , sex）values (1,‘张‘三’ 丰’，‘男’ )这种情况会报错，对于不确定插入字段数据的是单引号还是双引号时，解决方案有2种：insert into tableName（id , name , sex）values (1,‘张\‘三\’ 丰’，‘男’ )----使用转义字符insert into tableName（...">
#         <script src='//g.csdnimg.cn/tingyun/1.8.3/blog.js' type='text/javascript'></script>
#
#     """.replace("'","''"))
#     # 常规方式插入
#     # mypg.execute(sql)
#
# #     print(sql)
#     sql = """INSERT INTO test (num, name) VALUES (%(num)s, %(name)s)"""
#     params = {'num': 108, 'name': """
#
# <!DOCTYPE html>
#         <meta name="keywords" content="mysql 存储数据时遇到单'引号和双引号问题">
#         <meta name="csdn-baidu-search"  content='{"autorun":true,"install":true,"keyword":"mysql 存储数据时遇到单引号和双引号问题"}'>
#     <meta name="description" content="insert into tableName（id , name , sex）values (1,‘张‘三’ 丰’，‘男’ )这种情况会报错，对于不确定插入字段数据的是单引号还是双引号时，解决方案有2种：insert into tableName（id , name , sex）values (1,‘张\‘三\’ 丰’，‘男’ )----使用转义字符insert into tableName（...">
#     <script src='//g.csdnimg.cn/tingyun/1.8.3/blog.js' type='text/javascript'></script>
#
# """}
#     # 字典方式插入
#     # mypg.execute(sql, params)
#
#     sql = """INSERT INTO test (num, name) VALUES (%s, %s)"""
#     params = (109, """
#
# <!DOCTYPE html>
#     <meta name="description" content="insert into tableName（id , name , sex）values (1,‘张‘三’ 丰’，‘男’ )这种情况会报错，对于不确定插入字段数据的是单引号还是双引号时，解决方案有2种：insert into tableName（id , name , sex）values (1,‘张\‘三\’ 丰’，‘男’ )----使用转义字符insert into tableName（...">
#     <script src='//g.csdnimg.cn/tingyun/1.8.3/blog.js' type='text/javascript'></script>
#
# """)
#     # 以参数分离方式插入
#     # mypg.execute(sql, params)
#
#     sql = """select name from test where num=%(num1)s limit 10"""
#     params = {'num1': 900}
#     data = mypg.execute(sql, params)
#     print(data)
#     # sql = """INSERT INTO test (num, name) VALUES (%(num)s, %(name)s)"""
#     # params = {'num': 906, 'name': str(data.iloc[0]['name'])}
#     # mypg.execute(sql, params)
#     验证json格式校验
    import json
    sql = """select * from ai_article limit 10"""
    data = mypg.execute(sql)
    print(data['crawler_industry_l2'])
    if data.iloc[0]['crawler_industry_l2']:
        print('not None')
    else:
        print('is None')
    data = data.iloc[0].to_json(orient='columns', force_ascii=False)
    # print(data)
    data = json.loads(data)  # 字典对象
    print(data)

    # 整型字段的转换与校验
    def valid_int(text):
        if text == None:
            return 0
        if not isinstance(text, int):  # 非整型数据强制转换为整型
            if text != "":             # 空字符串默认为整数0
                text = int(text)
            else:
                text = 0
        return text

    # 字符串数据的转换与校验
    def valid_str(text):
        if text == None:
            return ""
        if not isinstance(text, str):
            return ""
        else:
            return text.strip()

    def valid_list(text):
        if text == None:
            return []
        if not isinstance(text, list):
            return []
        else:
            return text

    # data = data_df.iloc[i].to_json(orient='columns', force_ascii=False)
    # data = json.loads(data)  # json转换为字典对象
    # site_name = valid_str(data.get('site_name', ""))
    # language = valid_str(data.get('language_s', ""))
    # category = valid_str(data.get('category_s', ""))
    # source_url = valid_str(data.get('url', ""))  # 采集链接
    # published_time = valid_str(data.get('published_time', ""))
    # crawler_time = valid_str(data.get('crawler_time', ""))
    # # 补充统一的 rowkey
    # crawler_row_key = valid_str(data.get('row_key', ""))  # 原始的rowkey生成，该字段仅在应用库，不会进入草稿箱
    # crawler_keywords = valid_list(data.get('crawler_keywords', []))
    # title = valid_str(data.get('title', ""))
    # content = valid_str(data.get('content_s', ""))
    # author = valid_str(data.get('author', ""))
    # editor = valid_str(data.get('editor_s', ""))
    # pv = valid_int(data.get('pv', 0))  # None类型强制转化为0
    # comment = valid_int(data.get('comment_s', 0))
    # forward = valid_int(data.get('forward_s', 0))
    # like = valid_int(data.get('like_s', 0))
    # crawler_tags = valid_list(data.get('crawler_tags', []))
    # navigation = valid_list(data.get('navigation', []))
    # level = valid_str(data.get('level_s', ""))
    # industry_l1 = valid_str(data.get('industry_l1', ""))
    # industry_l2 = valid_str(data.get('industry_l2', ""))
    # industry_l3 = valid_str(data.get('industry_l3', ""))
    # crawler_industry_l1 = valid_str(data.get('crawler_industry_l1', ""))
    # crawler_industry_l2 = valid_str(data.get('crawler_industry_l2', ""))
    # crawler_industry_l3 = valid_str(data.get('crawler_industry_l3', ""))
    # crawler_batch_tag = valid_str(data.get('crawler_batch_tag', ""))
    # business_category = valid_str(data.get('business_category', ""))
    # image_urls = valid_list(data.get('image_urls', []))
    # image_oss_urls = valid_list(data.get('image_oss_urls', []))
    pass












