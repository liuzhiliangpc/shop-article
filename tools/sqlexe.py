# coding=utf-8
'''需安装的包
pip install sasl

pip install thrift

pip install thrift-sasl

pip install PyHive
'''
# from pyhive import hive
from pyhive import presto
# import psycopg2
# from TCLIService.ttypes import TOperationState
import sys
import importlib
import pandas as pd
importlib.reload(sys)
'''python3不适用
reload(sys) 
sys.setdefaultencoding('utf-8')
'''
from config import conf

host = conf.getConf('presto_server', 'HOST')
port = conf.getConf('presto_server', 'PORT')
username = conf.getConf('presto_server', 'USERNAME')

def sqlexe(sql,type):
    try:
        # print sql
        # sql_plus = sql.replace('\n',' ').replace('\t',' ').replace('"','\'')
        sql_plus = sql.replace(')',') ').replace(') \'',')\'')
        # print ('执行的sql语句为 : {}{}'.format('\n',sql_plus))#加括号
        if type == 'presto':#封装成df
            conn = presto.connect(host,port=port,username=username)
            cursor = conn.cursor()
            cursor.execute(sql_plus)
            data = cursor.fetchall()

            # for i in data:
            #     print(i)
            columnDes = cursor.description #获取连接对象的描述信息    
            columnNames = [columnDes[i][0] for i in range(len(columnDes))]
            df = pd.DataFrame([list(i) for i in data],columns=columnNames)
            return df
    except Exception as e:
        print (e)#加括号
        sys.exit()#改exit(1)为sys.exit()

if __name__ == '__main__':
    sqltext = "select * from hive.ods.babel_topic_all where strpos(ATTRIBUTE_DATA, 'lastOp:拼接信息') > 0 And strpos(TITLE, '题库') > 0 limit 10"
    type = 'presto'
    df = sqlexe(sqltext,type)
    print(df.head(10))