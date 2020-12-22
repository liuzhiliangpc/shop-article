#coding=utf-8
# 日志文件

import os
import time
import logging
import logging.handlers
from config import conf

def logInit(TAGGER='COMMON'):
    """
    实例化日志对象
    """
    # 默认配置
    log_dir   = conf.getConf('common', 'LOG_PATH')
    log_level = conf.getConf('common', 'LOG_LEVEL')

    if not log_dir:
        handler = logging.StreamHandler()
    else:
        if not os.path.exists(log_dir):
            open(log_dir, 'w').close()
        handler = logging.handlers.TimedRotatingFileHandler(log_dir, when='D', encoding='utf-8')

    if not log_level:
        log_level = 'info'


    fmt = '[' + TAGGER + '] [%(process)d] [%(filename)s +%(lineno)d] %(asctime)s  %(levelname)s  %(message)s'
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)

    logger_loc = logging.getLogger(TAGGER)
    logger_loc.addHandler(handler)
    level_dict = {
        'CRITICAL': 50,
        'FATAL': 'CRITICAL',
        'ERROR': 40,
        'WARNING': 30,
        'WARN': 'WARNING',
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0,
    }
    level = log_level
    logger_loc.setLevel(level_dict[level.upper()])
    return logger_loc

logger = logInit('COMMON')
