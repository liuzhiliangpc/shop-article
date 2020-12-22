#!/usr/bin/env python3
# encoding: utf-8
'''
@author: liuzhiliang
@license: 
@contact: liuzhiliang_pc@163.com
@software: pycharm
@file: gunicorn_config.py
@time: 2020/10/10 15:08
@desc:
'''

# import multiprocessing
# from config import conf
#
# port = conf.getConf('http_server', 'PORT', 'int')
# bind = '127.0.0.1:{}'.format(port)
# workers = multiprocessing.cpu_count() * 2 + 1
backlog = 2048
timeout = 600
# 并行工作进程数
workers = 8
# 指定每个工作者的线程
threads = 2
# 端口 18001
bind = '0.0.0.0:18001'
# 设置守护进程,将进程交
daemon = 'false'
# 工作模式协程
# worker_class = "gevent" # 不支持，原因未知
worker_class = 'eventlet'
# 设置最大并发量
worker_connections = 200
# worker重启之前处理的最大requests数， 缺省值为0表示自动重启disabled。主要是防止内存泄露。
max_requests = 1000
# 设置进程文件目录
# pidfile = '/var/run/gunicorn.pid'
# 设置访问日志和错误信息
# accesslog = "log/access.log"
# errorlog = "log/debug.log"
# loglevel = "debug"
# 设置日志记录水平
loglevel = 'warning'