#!/usr/bin/env python3
# encoding: utf-8
import os
import pandas as pd

class ConfirmTag:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.task_path = os.path.join(dir_path, 'tasks/task.pkl')
        # 素材库路径
        # 素材索引号和被引用次数的记录文件
        self.material_store = os.path.join(dir_path, 'materials/m_history.pkl')

    def material_recieve(self, query):
        df_task = pd.read_pickle(self.task_path)
        df_task['task_id'] = df_task.apply(lambda r: str(r.task_id), axis=1)
        # df_task = pd.read_pickle(r'C:\Users\baixing\Desktop\BX\build_request\algo_article\algos\cpni\tasks\task.pkl')
        df_material = pd.read_pickle(self.material_store)
        # df_material = pd.read_excel(r'C:\Users\baixing\Desktop\BX\build_request\algo_article\algos\cpni\materials\m_history.xlsx').reset_index(drop=True)
        ret = []
        # 检验确认接收的任务编号是否存在
        if query['task_id'] not in list(df_task['task_id']):
            back = {}
            back['retcode'] = 3
            back['msg'] = '任务编号不存在'
            return back
        task_query = list(df_task[df_task['task_id'] == query['task_id']]['query'])[-1]
        # 检验素材号是否存在
        for rowkey in [dic['rowkey'] for dic in query['data']]:
            back = {}
            if str(rowkey) in list(df_material['m_id']):
                p = task_query['data'].pop(0)
                # print(p)
                old_num = df_material.loc[df_material['m_id'].astype(str) == str(rowkey), 'times']
                df_material.loc[df_material['m_id'].astype(str) == str(rowkey), 'times'] = old_num + 1
                back['retcode'] = 0
                back['msg'] = '操作成功'
            else:
                back['retcode'] = 4
                back['msg'] = '文章id不存在'
                return back
            # 更新原始任务内容
            df_task['query'] = df_task.apply(lambda r: task_query if r.task_id == query['task_id'] else r.query, axis=1)
            if len(ret) == len(query['data']):
                back = {}
                back['retcode'] = 0
                back['msg'] = '操作成功'
        # 释放任务编号
        if len(task_query['data']) == 0:
            df_task = df_task.drop(index=(df_task.loc[(df_task['task_id'] ==
                                                       query['task_id'])].index)).reset_index(drop=True)
        try:
            df_material.to_pickle(self.material_store)
        except:
            os.remove(self.material_store)
            df_material.to_pickle(self.material_store)
        try:
            df_task.to_pickle(self.task_path)
        except:
            os.remove(self.task_path)
            df_task.to_pickle(self.task_path)
        return back

if __name__ == '__main__':
    q = data = {
        "task_id": "123456",
        "data": [{
                "compound_words_id":"123",
                "rowkey": "111111aadd"
        }]
}
    CR = ConfirmTag()
    r = CR.material_recieve(query=q)
    print(r)