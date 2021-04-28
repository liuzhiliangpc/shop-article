#!/usr/bin/env python3
# encoding: utf-8
import time
import datetime


def get_current_timestamp():
    return int(time.time())


def get_current_datetime():
    return datetime.datetime.now()


def get_current_datetime_str(format='%Y-%m-%d %H:%M:%S'):
    return get_current_datetime().strftime(format)


def get_current_date():
    return get_current_datetime_str(format='%Y-%m-%d')


def format_timestamp(ts: int, format='%Y-%m-%d %H:%M:%S'):
    """ 将时间戳转化为不同格式的时间字符串

    @param ts: 时间戳，例如：12345667
    @param format: 时间的格式
    @return: 时间字符串
    """
    return datetime.datetime.fromtimestamp(ts).strftime(format)
