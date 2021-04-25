#!/usr/bin/env python3
# encoding: utf-8

class BaseConfig:
    pass

class LocalConfig(BaseConfig):
    REDIS_URL = 'redis://localhost:6379/1'
    CELERY_BROKER_URL='redis://localhost:6379/1'
    CELERY_RESULT_BACKEND='redis://localhost:6379/1'
    CELERY_DEFAULT_QUEUE = 'celery_456'
    REDIS_LOCK_HOST = 'localhost'
    REDIS_LOCK_PORT = 6379
    REDIS_LOCK_DB = 0

configs_outer = {
    'default': BaseConfig,
    'local': LocalConfig,
}

ConfigOuter = configs_outer['local']
