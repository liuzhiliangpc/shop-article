#!/usr/bin/env python3
# encoding: utf-8
'''
@author: liuzhiliang
@license:
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: a0015.py
@time: 2020/11/4 21:34
@desc: 素材j接收确认
'''
from core.core import logger
from algos.article.recieve_confirmtag import ConfirmTag

# LT = LocationTagger()
CT = ConfirmTag()
def run(request):
    """
    素材接收确认功能
    :param request: 请求参数
    :return: 校验结果
    """
    r = {}
    # 获取一条请求数据
    event = request
    # 检查task_id数据类型
    if not isinstance(event['task_id'], str):
        r['retcode'] = 6
        r['msg'] = '请检查数据类型'
        return r
    # 检查data字段里数据类型
    for dic in event['data']:
        r = {}
        if not isinstance(dic['compound_words_id'], str) or not isinstance(dic['rowkey'], str):
            r['retcode'] = 6
            r['msg'] = '请检查数据类型'
            return r
    # 检验素材号是否有重复
    if len(list(set([dic['rowkey'] for dic in event['data']]))) < len([dic['rowkey'] for dic in event['data']]):
        r['retcode'] = 2
        r['msg'] = '确认接收的文章id有重复'
        return r
    # 检验组合词id是否有重复
    if len(list(set([dic['compound_words_id'] for dic in event['data']]))) < len([dic['compound_words_id']
                                                                                  for dic in event['data']]):
        r['retcode'] = 2
        r['msg'] = '组合词编号有重复'
        return r
    logger.info('[素材确认接收] [任务编号:{}]'.format(str(event['task_id'])))
    # debug_option = event.get('debug', None)  # 调试模式默认为'dev'
    ret = CT.material_recieve(event)
    logger.info('[素材接收返回] [结果:{}]'.format(str(ret)))
    return ret

if __name__ == "__main__":
    data = {
        "task_id": "209678",
        "data": [{
                "compound_words_id":"123",
                "rowkey": "111111aadd"
        }, {
                "compound_words_id":"123",
                "rowkey": "111111222d"
        }]
}
    print(run(request=data))
    # import time
    # time1 = time.time()
    # for i in range(1000):
    #     run(request=data)
    # time_range = time.time() - time1
    # print('spend time is {} s'.format(time_range))