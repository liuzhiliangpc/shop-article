#!/usr/bin/env python3
# encoding: utf-8
# 算法核心服务资源

import sys
import importlib
from tools.log import logInit
logger = logInit('BackEndCore')

class BackEndCore(object):

    def __init__(self, web=False):
        pass
        # 预加载需要的实例
        self.gateways = (
            ['scripts.a0001'],
            ['scripts.a0002'],
            ['scripts.a0003'],
        )
        for gateway in self.gateways:
            lib = importlib.import_module(gateway[0])

    def handle(self, job_name, request):
        module_name = 'scripts.%s'%job_name

        if module_name in sys.modules.keys():
            logger.info('Module:%s has been loaded'%(module_name))
            _module = sys.modules[module_name]
        else:
            _module = importlib.import_module(module_name)

        logger.info('Run module')
        result = _module.run(request)
        return result

