#!/usr/bin/env python3
# encoding: utf-8
import os
import pandas as pd

class MaterialTag:
    def __init__(self):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.task_path = os.path.join(dir_path, 'tasks/task.pkl')
        # 素材库路径
        # 素材索引号和被引用次数的记录文件
        self.material_store = os.path.join(dir_path, 'materials/m_history.pkl')

    def material_request(self, query):
        """
        material_request 根据已接收过店铺组合词的任务编号，返回指定数量的店铺文章
        :param query:
            {
              "task_id": "202090",
              "batch_size": 3
            }
        task_id 表示发文任务编号，字符串类型；batch_size表示请求文章数目，整型。
        batch_size不能超过任务剩余文章额度。
        :return:
        # 请求返回结果
        {
          "retcode": 0,
          "task_id": "665544",
          "remain_nums": 1,
          "data": [
            {
              "rowkey": "111111aadd",
              "compound_words_id": "123",
              "article_id": "123456",
              "title": "上海黄金回收哪家好？",
              "content": "7时25分，交警指挥中心的视频监控系统切换到南京北街与抚顺路交通岗。只见细雨中，几名交警指挥车辆，移动式信号灯也上了岗。",
              "description": "",
              "keywords": [],
              "image_urls": [],
              "keyword_layout_tag": [],
            }
          ],
          "msg": "操作成功，本次请求后，发文任务有剩余文章额度"
        }
        retcode 表示状态码信息，数值类型；
        remain_nums表示本次请求后剩余文章量，即任务额度已耗尽，整型；
        msg表示说明信息，字符串类型;
        data列表内为文章信息。其中rowkey为文章id，文章唯一键，十六进制字符串类型；
                                compound_words_id为组合词编号，字符串类型；
                                article_id为文章id，由于布词前文章rowkey和组合词id可能重复，article_id为新建草稿箱文档唯一键；
                                title为文章标题，字符串类型；
                                content为文章正文，字符串类型；
                                description为摘要，目前为空字符串；
                                keywords为关键词列表，目前为空列表；
                                image_urls为图片在oss上的url地址，列表类型；
                                keyword_layout_tag为布词标记，列表类型。
        """
        df_task = pd.read_pickle(self.task_path)
        df_task['task_id'] = df_task.apply(lambda r: str(r.task_id), axis=1)
        # df_task = pd.read_pickle(r'C:\Users\baixing\Desktop\BX\build_request\algo_engine\algos\cpni\tasks\task.pkl')
        df_material = pd.read_pickle(self.material_store).reset_index(drop=True)
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
                        article_info['article_id'] = '待定0000'
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
                        article_info['article_id'] = '待定0000'
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

if __name__ == '__main__':
    q = {
        "task_id": '202090',  # 任务编号id
        "batch_size": 2  # 单批次次请求1条数量
    }
    MR = MaterialTag()
    r = MR.material_request(query=q)
    print(r)