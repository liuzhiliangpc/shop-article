#!/usr/bin/env python3
# encoding: utf-8
"""
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: jsonlog.py
@time: 2021/1/26 21:13
@desc: 日志文件，当前版本日期为2021-03-12
"""

from pythonjsonlogger import jsonlogger
import datetime
from config import conf
import logging
from tools.time_utils import get_current_date

log_dir = conf.getConf("common", "LOG_PATH").strip() # 日志路劲
log_level = conf.getConf("common", "LOG_LEVEL")


def logInit(logger_name):
    class CustomJsonFormatter(jsonlogger.JsonFormatter):
        def process_log_record(self, log_record):
            log_record["logger_name"] = logger_name
            return log_record

    if not log_level:  # 日志等级
        level = "info"
    else:
        level = log_level

    logger = logging.getLogger(logger_name)
    if not log_dir:
        log_handler = logging.StreamHandler()
    else:
        log_handler = logging.FileHandler(
            f"{log_dir}.{get_current_date()}.log", encoding="utf-8"
        )  # 中文显示

    fmt = "%(asctime)s %(levelname)s %(process)d %(filename)s %(funcName) %(lineno)d %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%SZ%z"  # 指定时间格式
    formatter = CustomJsonFormatter(
        fmt=fmt, datefmt=datefmt, json_ensure_ascii=False
    )  ### 支持中文，指定参数json_ensure_ascii=False

    log_handler.setFormatter(formatter)  # 设置格式
    level_dict = {
        "CRITICAL": 50,
        "FATAL": "CRITICAL",
        "ERROR": 40,
        "WARNING": 30,
        "WARN": "WARNING",
        "INFO": 20,
        "DEBUG": 10,
        "NOTSET": 0,
    }

    logger.setLevel(level_dict[level.upper()])  # 设置日志等级
    logger.addHandler(log_handler)  # 添加日志
    return logger


logger = logInit("Common")

if __name__ == "__main__":
    logger = logInit("test")
    logger.info("我是测试数据啦")

    def test():
        print("我是测试2号")
        logger.info("我是测试2号")

    test()

