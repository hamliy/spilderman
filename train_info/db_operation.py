#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2019 lihan
# @Time    : 2019/1/13 上午10:02
# @Author  : lihan
# @File    : db_operation.py
# @Dec     : 数据库操作

from pymongo import MongoClient
from datetime import datetime
from itest.tools.train_info.util import getMonthFirstDayAndLastDay
from bson import ObjectId

MONGO_URL = 'mongodb://172.20.166.50:27017/'
DB_NAME = 'train'


def get_set(dbset):
    """
    获取表
    :param dbset:
    :return:
    """
    md = MongoClient(MONGO_URL)
    db = md[DB_NAME]
    return db[dbset]

def get_to_end_train(stations):
    train_ticket_list = []
    num = len(stations)
    for i in range(num-1):
        for j in range(i+1, num):
            train_ticket_list.append({
                'startStation': stations[i],
                'endStation': stations[j]
            })

    return train_ticket_list


def is_had_ticket(ticket):

    dbset = get_set('train_ticket')
    print(ticket)
    data = dbset.find_one({'trainNumber': ticket['trainNumber'],
                              'startStation': ticket['startStation'],
                              'endStation': ticket['endStation'],
                              'seatType': ticket['seatType'],
                              'seatPrice': ticket['seatPrice']
                              })
    if data:
        return True
    else:
        return False

def update_train_ticket(ticket):
    dbset = get_set('train_ticket')
    dbset.insert_one(ticket)


def save_proxy(proxy):
    dbset = get_set('proxy')
    dbset.insert_one(proxy)


def is_had_proxy(proxy):
    dbset = get_set('proxy')
    data = dbset.find_one({
                            'url': proxy['url']
                              })
    if data:
        return True
    else:
        return False


def get_proxys_url(query={}):
    proxys = []
    dbset = get_set('proxy')
    all_proxy = dbset.find(query)
    for proxy in all_proxy:
        proxys.append(proxy['url'])
    return proxys


def get_normal_proxys_url():
    return get_proxys_url({'status': 'normal'})


def update_proxy_status(url, status):
    dbset = get_set('proxy')
    dbset.update_one({'url': url}, {'$set': {'status': status}})


def clear_proxy():
    dbset = get_set('proxy')
    dbset.remove()

def clear_abnormal_proxy():
    dbset = get_set('proxy')
    dbset.delete_many({'status':'abnormal'})

def get_trains():
    """
    获取当前月的车次
    :param year:
    :param month:
    :return:
    """
    set = get_set('train_list')
    year = datetime.utcnow().year  # 当前月份
    month = datetime.utcnow().month  # 当前月份
    fist_day, last_day = getMonthFirstDayAndLastDay(year, month)
    all = set.find({'createTime':{"$gte": fist_day, "$lt": last_day }})
    trains = []
    for t in all:
        trains.append(t)
    return trains

def get_current_train_ticket_list(date=datetime.utcnow()):
    """
    获取当前月的所有车站信息
    :param date:
    :return:
    """
    year = date.year  # 当前月份
    month = date.month  # 当前月份
    dbset = get_set('train_station_list')
    fist_day, last_day = getMonthFirstDayAndLastDay(year, month)

    all_train_stations = dbset.find({'createTime': {"$gte": fist_day, "$lt": last_day}, 'errorCode': {"$nin":[303]}})
    all_train_ticket_list = []
    for train in all_train_stations:
        stations = train['stations']
        train_ticket = get_to_end_train(stations)
        train['trainTicket'] = train_ticket
        all_train_ticket_list.append(train)
    return all_train_ticket_list

def get_station_code_order_by_name():
    """
    根据获取车站名及对应编号
    :param station_name:
    :return:
    """
    set = get_set('train_station')
    year = datetime.utcnow().year  # 当前月份
    month = datetime.utcnow().month  # 当前月份
    fist_day, last_day = getMonthFirstDayAndLastDay(year, month)
    all_station = set.find({'createTime':{ "$gte" : fist_day, "$lt" : last_day }})
    result = {}
    for station in all_station:
        result[station['stationName']] = station['stationCode']
    return result


def update_train_info(data):
    """
    更新车次库
    :return:
    """
    set = get_set('train_info')
    set.insert_one(data)


def update_train_seat(train):
    """
   更新车次座位
   :return:
   """
    dbset = get_set('train_station_list')
    dbset.update_one({'_id': ObjectId(train['_id'])}, {
        '$set': {
            'errorCode':train['errorCode'],
            'error': train['error'],
            'seatType': train['seatType']
        }
    })

def update_train_ticket(ticket):
    """
   更新车票信息
   :return:
   """
    dbset = get_set('train_ticket')
    dbset.update_one({'trainNumber': ticket['trainNumber'], 'startStation': ticket['startStation'],
                      'endStation': ticket['endStation']}, {
        '$set': {
            'errorCode':ticket['errorCode'],
            'error': ticket['error'],
            'seatType': ticket['seatType']
        }
    })

def get_loaded_train_ticket():
    dbset = get_set('train_ticket')
    all = dbset.find({})
    trains = []
    for one in all:
        if one['trainNumber'] not in trains:
            trains.append(one['trainNumber'])
    return trains

def remove_ticket(id):

    dbset = get_set('train_ticket')
    dbset.remove({'_id': ObjectId(id)})

def get_fail_train_ticket():
    fail_tickets = []
    dbset = get_set('train_ticket')
    all = dbset.find({'errorCode':{ "$in" : [1003, 1002]}})
    for one in all:
        fail_tickets.append(one)
    return fail_tickets

if __name__ == '__main__':
    # dbset = get_set('train_station_list')
    # a = dbset.find_one({'_id': ObjectId('5c3ff0945f627d45f946a09d')})
    # print(a)
    remove_ticket('5c3d3c3f5f627d86b426c3fc')
