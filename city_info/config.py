# encoding: utf-8
"""
@author: han.li
@file  : config.py
@time  : 3/19/19 7:17 PM
@dec   : 配置相关
"""
import os
from urllib.parse import quote_plus


class Config(object):
    """Base configuration"""

    APP_DIR = os.path.abspath(os.path.dirname(__file__))    # this directory
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_DIR, os.pardir))

    # DATACREATOR_URI = "mongodb://%s:%s@%s" % (quote_plus('test'), quote_plus('HlCPj39gI3zu'), "123.207.7.95:31605")
    DB_URI = "mongodb://%s" % "172.20.166.50:27017"
    DB_NAME = 'city-info'
    DB_SET = {
        'city': 'city',  # 城市列表
    }
    QUERY_YEAR = '2018'
    SPIDER_URL = {
        'base': 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/%s' % QUERY_YEAR     # 爬取基本路径
    }
