#coding=utf-8
"""
    读取配置模块：默认读取该目录下的 configure.ini 文件，若该文件不存在则读取default_config.ini文件
"""
import os
import configparser


class Config(object):

    def __init__(self):
        dir_home = os.path.abspath(os.path.dirname(__file__))
        self.conf = configparser.ConfigParser()

        if os.path.exists(os.path.join(dir_home, 'conf.d', 'configure.ini')):
            print('load succeed')
            self.conf.read(os.path.join(dir_home, 'conf.d', 'configure.ini'), encoding='UTF-8')
        else:
            self.conf.read(os.path.join(dir_home, 'default_config.ini'), encoding='UTF-8')

    def getConf(self, section, option, type='str', default=None):
        if not self.conf.has_section(section):
            return default
    
        if not self.conf.has_option(section, option):
            return default
    
        if type is 'str':
            value = self.conf.get(section, option)
        elif type is 'int':
            value = self.conf.getint(section, option)
        elif type is 'float':
            value = self.conf.getfloat(section, option)
        elif type is 'bool':
            value = self.conf.getboolean(section, option)
        elif type is 'list':
            value = self.conf.get(section, option).split(',')
        else:
            value = None

        if value == None:
            return default

        return value

conf = Config()

if __name__ == '__main__':
    conf = Config()
    ret = conf.getConf('common', 'VERSION')
    print(ret)

