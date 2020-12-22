#!/usr/bin/env python3
# encoding: utf-8

from server.web import app
from config import conf

host = conf.getConf('shop_article_server', 'HOST')
port = conf.getConf('shop_article_server', 'PORT', 'int')
app.run(host=host, port=port, threaded=True)

