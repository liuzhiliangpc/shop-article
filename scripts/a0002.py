#!/usr/bin/env python3
# encoding: utf-8
'''
@author: liuzhiliang
@license:
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: a0015.py
@time: 2020/11/4 21:34
@desc: 素材请求
'''

from core.core import logger
from algos.article.material_request import MaterialTag

# LT = LocationTagger()
MR = MaterialTag()
def run(request):
    """
    素材请求功能
    :param request: 请求参数
    :return: 校验结果
    """
    r = {}
    # 获取一条请求数据
    event = request
    # 检查task_id数据类型
    if not isinstance(event['task_id'], str):
        r['retcode'] = 5
        r['msg'] = '检查数据类型'
        return r
    logger.info('[获取素材请求数据] [每批返回数据:{}条]'.format(str(event['batch_size'])))
    debug_option = event.get('debug', None)  # 调试模式默认为'dev'
    ret = MR.material_request(event)
    logger.info('[素材返回] [结果:{}]'.format(str(ret)))
    return ret

if __name__ == "__main__":
    data = {
        "task_id": '667788',  # 任务编号id
        "batch_size": 3,  # 单批次次请求1条数量
}
    print(run(request=data))