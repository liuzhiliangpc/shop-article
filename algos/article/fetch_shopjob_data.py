#!/usr/bin/env python3
# encoding: utf-8

import os
import pandas as pd

class ShopTaskQuery:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.task_record_pkl = os.path.join(dir_path, 'tasks/task.pkl')
        self.db = 'presto'

    def request_format(self, data):
        # # 读取
        df_task = pd.read_pickle(self.task_record_pkl).reset_index(drop=True)
        df_task['task_id'] = df_task.apply(lambda r: str(r.task_id), axis=1)
        if df_task.shape[0] > 0:
            # 检查任务编号是否重复
            if data['task_id'] in list(df_task['task_id']):
                return 3
        df_task.loc[len(df_task)] = [data['task_id'], data, data['task_create_time'],
                                     data['business_category'], data['task_nums'],
                                     data['data']]
        os.remove(self.task_record_pkl)
        # save
        df_task.to_pickle(self.task_record_pkl)
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

if __name__ == '__main__':
    shop = ShopTaskQuery()
    d = list([{
                "compound_words_id": "123",  # 组合词id
                "compound_words_type": "ABC",  # 组合词类型
                "compound_words": "上海刑事律师",  # 组合词
                "root_A": "上海",  # 词根A,缺省为空字符串""
                "root_B": "刑事",  # 词根B
                "root_C": "律师",  # 词根C
                "root_D": ""      # 词根D
         }, {
                "compound_words_id": "123",  # 组合词id
                "compound_words_type": "ABC",  # 组合词类型
                "compound_words": "上海刑事律师",  # 组合词
                "root_A": "上海",  # 词根A,缺省为空字符串""
                "root_B": "刑事",  # 词根B
                "root_C": "律师",  # 词根C
                "root_D": ""  # 词根D
            }])
    data = {
        "task_id": "564805",  # 任务编号id
        "task_create_time": "2020-12-08 10:03:21",  # 任务创建时间
        "business_category": "B2B",  # 任务为B2B类型还是B2C类型（本地服务）
        "task_nums": 2,  # 总的任务请求素材数
        "data": d
    }
    print(shop.status_tag(data))



