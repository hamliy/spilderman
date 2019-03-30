# encoding: utf-8
"""
@author: han.li
@file  : db_operation.py
@time  : 3/19/19 7:15 PM
@dec   : 
"""
from pymongo import MongoClient

from city_info.config import Config
from datetime import datetime


def get_set(dbset):
    """
    获取表
    :param dbset:
    :return:
    """
    md = MongoClient(Config.DB_URI)
    db = md[Config.DB_NAME]
    return db[Config.DB_SET[dbset]]


SET_CITY = get_set('city')


def update_info(info):
    """
    更新城市信息
    :param info:
    :return:
    """
    # dbset = get_set('city')
    now = datetime.utcnow()
    info['queryYear'] = Config.QUERY_YEAR
    info['createTime'] = now
    info['modifiedTime'] = now
    SET_CITY.insert_one(info)


def get_by_type(type):
    """
    根据类型获取数据信息
    :param type:
    :return:
    """
    results = []
    query_info = SET_CITY.find({'type': type , 'queryYear': Config.QUERY_YEAR})
    for q in query_info:
        results.append(q)
    return results


def is_had_city(info):
    data = SET_CITY.find_one({
                            'code': info['code'],
                            'queryYear': Config.QUERY_YEAR
                              })
    if data:
        return True
    else:
        return False


def get_not_update_county():
    """
    获取更新失败的城市
    :return:
    """
    cs = SET_CITY.find({'type':'county'})
    all_c = []
    for c in cs:
        bs = SET_CITY.find_one({'connection.county': c['name']})
        if not bs:
            all_c.append(c)
    return all_c
