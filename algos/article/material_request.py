#!/usr/bin/env python3
# encoding: utf-8
import os
import pandas as pd
from tools.mypsycopg2 import Mypsycopg2

class MaterialTag:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.task_path = os.path.join(dir_path, 'tasks/task.pkl')
        # 素材库路径
        # 素材索引号和被引用次数的记录文件
        self.material_store = os.path.join(dir_path, 'materials/m_history.pkl')

    def material_request(self, query):
        df_task = pd.read_pickle(self.task_path)
        df_task['task_id'] = df_task.apply(lambda r: str(r.task_id), axis=1)
        # df_task = pd.read_pickle(r'C:\Users\baixing\Desktop\BX\build_request\algo_engine\algos\cpni\tasks\task.pkl')
        df_material = pd.read_pickle(self.material_store).reset_index(drop=True)
        # df_material = pd.read_excel(r'C:\Users\baixing\Desktop\BX\shop-article\algos\article\materials\m_history.xlsx').reset_index(drop=True)


        ret = []
        if query['task_id'] not in list(df_task['task_id']):
            back = {}
            back['retcode'] = 4
            back['msg'] = '任务编号不存在'
            return back
        if len(df_material) == 0:
            back = {}
            back['retcode'] = 3
            back['msg'] = '文章准备中'
            return back
        else:
            task_query = list(df_task[df_task['task_id'] == query['task_id']]['query'])[-1]
            for i in range(1):
                back = {}
                back['task_id'] = query['task_id']
                back['data'] = []
                article_info = {
                "description": "",  # 摘要，暂时为空，P2需求算法补上
                "keywords": [],  # 关键词，暂时缺失，P2需求算法需求补上
                "image_urls": [],  # 图片oss地址
                "keyword_layout_tag": []  # 布词方式标记,字符串类型
                }
                # 草稿箱里的素材量足够
                if len(task_query['data']) >= query['batch_size']:
                    for j in range(query['batch_size']):
                        article_info = {
                            "description": "",  # 摘要，暂时为空，P2需求算法补上
                            "keywords": [],  # 关键词，暂时缺失，P2需求算法需求补上
                            "image_urls": [],  # 图片oss地址
                            "keyword_layout_tag": []  # 布词方式标记,字符串类型
                        }
                        article_info['rowkey'] = str(df_material['m_id'][i*query['batch_size'] + j])
                        p = task_query['data'].pop(0)
                        article_info['compound_words_id'] = str(p['compound_words_id'])
                        article_info['title'] = str(df_material['m_title'][i*query['batch_size'] + j])
                        article_info['content'] = str(df_material['m_contents'][i*query['batch_size'] + j])
                        # print(str(df_material['m_id'][i * query['batch_size'] + j]))
                        back['data'].append(article_info)
                    if len(task_query['data']) == 0:
                        back['retcode'] = 2
                        back['remain_nums'] = 0
                        back['msg'] = '操作成功，本次请求后，发文任务无剩余文章额度'
                    else:
                        back['retcode'] = 0
                        back['remain_nums'] = len(task_query['data'])
                        # response['msg'] = '草稿箱未取尽'.format()
                        back['msg'] = '操作成功，本次请求后，发文任务有剩余文章额度'
                # 若草稿箱里的素材量不足
                elif len(task_query['data']) > 0:
                    short_nums = query['batch_size'] - len(task_query['data'])
                    for j in range(len(task_query['data'])):
                        article_info = {
                            "description": "",  # 摘要，暂时为空，P2需求算法补上
                            "keywords": [],  # 关键词，暂时缺失，P2需求算法需求补上
                            "image_urls": [],  # 图片oss地址
                            "keyword_layout_tag": []  # 布词方式标记,字符串类型
                        }
                        article_info['rowkey'] = str(df_material['m_id'][i*query['batch_size'] + j])
                        p = task_query['data'].pop(0)
                        article_info['compound_words_id'] = str(p['compound_words_id'])
                        article_info['title'] = str(df_material['m_title'][i*query['batch_size'] + j])
                        article_info['content'] = str(df_material['m_contents'][i*query['batch_size'] + j])
                        back['data'].append(article_info)
                    back['retcode'] = 2
                    back['remain_nums'] = 0
                    back['msg'] = '操作成功，本次请求超过发文任务剩余文章额度'
                else:
                    back['retcode'] = 207
                    back['msg'] = '草稿箱已空'
        return back

    # mypg = Mypsycopg2()
    # # query_sql = """SELECT * from %(source_table)s WHERE id between %(gte_id)s and %(lte_id)s"""
    # query_sql = """SELECT * from {} WHERE id between %(gte_id)s and %(lte_id)s""".format(source_table)
    # # params = {"source_table": source_table, "gte_id": origin_id, "lte_id": last_id}
    # params = {"gte_id": origin_id, "lte_id": last_id}
    # try:
    #     data = mypg.execute(query_sql, params)  # 获取当前时间范围内的数据
    #     logger.info(
    #         "获取爬取id为{0}到{1}的数据，数据量为{2}".format(origin_id, last_id, data.shape[0])
    #     )
    # except Exception as e:
    #     logger.error("执行sql语句,从表{}中获取爬取id为{}到{}的数据失败 {}".format(source_table, origin_id, last_id, e))
    # # 关闭数据库连接
    # mypg.close()
if __name__ == '__main__':
    q = {
        "task_id": '202090',  # 任务编号id
        "batch_size": 2  # 单批次次请求1条数量
    }
    MR = MaterialTag()
    r = MR.material_request(query=q)
    print(r)