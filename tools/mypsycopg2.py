#!/usr/bin/env python3
# encoding: utf-8
'''
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: mypsycopg2.py
@time: 2020/11/26 14:27
@desc:当前版本更新到2021年01月18日
'''

import psycopg2
from config import conf
import pandas as pd

class Mypsycopg2():
    def __init__(self):
        self._host = conf.getConf('postgresql', 'HOST')
        self._port = conf.getConf('postgresql', 'PORT')
        self._database = conf.getConf("postgresql", "database")
        self._scheme = conf.getConf("postgresql", "scheme")
        self._user = conf.getConf("postgresql", "user")
        self._password = conf.getConf("postgresql", "password")
        # self.conn = psycopg2.connect(database="dev", user="crawler", password="crawler!@#123", host=self._host, port=self._port)
        self.conn = psycopg2.connect(database=self._database, user=self._user, password=self._password, host=self._host, port=self._port, options="-c search_path={}".format(self._scheme))
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

    data = mypg.execute()
    print("the version of database is {}".format(data))
    # 查询sql
    # sql = """select id, title from ai_article where crawler_time >  limit 10"""
    # sql = """SELECT id, title, crawler_time FROM "ai_article_liuzhiliang" \
    #         WHERE to_timestamp(crawler_time, 'YYYY-MM-DD hh24:mi:ss')  \
    #         between '2020-11-19 23:36:35' and '2020-11-19 23:36:59' LIMIT 1000"""
    # data = mypg.execute(sql)
    # data = mypg.execute()
    # print("the version of database is {}".format(data))
#     # 创建表格
#
#     sql = """CREATE Table test (id serial4 PRIMARY KEY, num int4,name text)"""
#     # data = mypg.execute(sql)
#
#     # 插入
#     sql ="INSERT INTO test (num, name)  VALUES (%s, '%s')" %(105, "(一)''明知''是伪造的信用卡而持有、运输的，或者明知是伪造的空白信用卡而持有、运输，数量较大的;\n\n(二)非法持有他人信用卡，数量较大的;\n\n(三)使用虚假的身份证明骗领信用卡的;")
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
