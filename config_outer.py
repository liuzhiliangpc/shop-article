#!/usr/bin/env python3
# encoding: utf-8

from config import conf


class BaseConfig:
    pass


class LocalConfig(BaseConfig):
    REDIS_URL = "redis://localhost:6379/1"
    CELERY_BROKER_URL = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND = "redis://localhost:6379/1"
    CELERY_DEFAULT_QUEUE = "celery_shop_article"
    REDIS_LOCK_HOST = "localhost"
    REDIS_LOCK_PORT = 6379
    REDIS_LOCK_DB = 0


class ProdConfig(BaseConfig):
    REDIS_URL = "redis://r-data-yxt.redis.zhangbei.rds.aliyuncs.com:6379/1"
    CELERY_BROKER_URL = "redis://r-data-yxt.redis.zhangbei.rds.aliyuncs.com:6379/1"
    CELERY_RESULT_BACKEND = "redis://r-data-yxt.redis.zhangbei.rds.aliyuncs.com:6379/1"
    CELERY_DEFAULT_QUEUE = "celery_shop_article"
    REDIS_LOCK_HOST = "r-data-yxt.redis.zhangbei.rds.aliyuncs.com"
    REDIS_LOCK_PORT = 6379
    REDIS_LOCK_DB = 0


class DevConfig(BaseConfig):
    REDIS_URL = 'redis://10.62.34.231:6379/1'
    CELERY_BROKER_URL='redis://10.62.34.231:6379/1'
    CELERY_RESULT_BACKEND='redis://10.62.34.231:6379/1'
    CELERY_DEFAULT_QUEUE = 'celery_shop_article'
    REDIS_LOCK_HOST = '10.62.34.231'
    REDIS_LOCK_PORT = 6379
    REDIS_LOCK_DB = 0


configs_outer = {
    "default": BaseConfig,
    "local": LocalConfig,
    "dev": DevConfig,
    "prod": ProdConfig,
}

mode = conf.getConf("common", "MODE")
ConfigOuter = configs_outer[mode]
