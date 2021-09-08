#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: flask_postgresql_service.py
@time: 2021/6/1 15:19
@desc: 基于flask_postgresql执行原生SQL的公共类
"""

from typing import Any, Tuple, List, Dict  # 泛型类型支持
from contextlib import contextmanager
from sqlalchemy import text
# from sqlalchemy.orm import sessionmaker # 未使用
from init import db
import pandas as pd

class DBUtil(object): # 后续优化，支持的db选项如mysql、pgsql多个数据源可以配置
    def __init__(self):
        pass

    def get_session(self):
        session = db.session
        return session

    @contextmanager
    def auto_commit(self):
        # 装饰器contextmanager，使用完自动释放
        try:
            self.session = self.get_session()
            yield
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise InterruptedError(f'sql执行出错, e:{str(e)}')
        finally:
            self.session.close()

    def update(self, sql: str='', params=None):
        """数据库执行一次操作"""
        session = self.session
        if sql:
            stmt = text(sql)
            if params:
                session.execute(stmt, params)
            else:
                session.execute(stmt)
        else:
            print("sql is none")

    def select(self, sql: str='', params=None):
        # resList = []
        session = self.session
        if sql:
            stmt = text(sql)
            if params:
                records = session.execute(stmt, params) # records为迭代器，要分别获取字段名和值，需要用fetchall
                # results = [dict(zip(result.keys(), result)) for result in records] # 可获取字段名信息，数据为空时该方式会报异常，且pandas.from_dict不支持该方式转。
                datas = records.fetchall() # 此处浪费了内存 可优化为直接用迭代器 TODO
                columnDes = records.context.cursor.description  # 获取连接对象的描述信息，支持查询结果为空的情况，只能用该方式。# 抛弃异常的获取方式 columnNames = list(datas[0].keys())
                columnNames = [columnDes[i][0] for i in range(len(columnDes))]
                df_res = pd.DataFrame([list(result) for result in datas], columns=columnNames)
                return df_res
            else:
                records = session.execute(stmt)
                datas = records.fetchall() # 此处浪费了内存 可优化为直接用迭代器 TODO
                columnDes = records.context.cursor.description  # 获取连接对象的描述信息，支持查询结果为空的情况，只能用该方式。
                columnNames = [columnDes[i][0] for i in range(len(columnDes))]
                df_res = pd.DataFrame([list(result) for result in datas], columns=columnNames)
                return df_res
        else:
            print("sql is none")
