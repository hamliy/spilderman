#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2019 lihan
# @Time    : 2019/1/13 下午2:40
# @Author  : lihan
# @File    : util.py
# @Dec     : 
import calendar
import threading, queue
from datetime import datetime
from time import ctime

def getMonthFirstDayAndLastDay(year=None, month=None):
    """
    :param year: 年份，默认是本年，可传int或str类型
    :param month: 月份，默认是本月，可传int或str类型
    :return: firstDay: 当月的第一天，datetime.date类型
              lastDay: 当月的最后一天，datetime.date类型
    """
    if year:
        year = int(year)
    else:
        year = datetime.today().year

    if month:
        month = int(month)
    else:
        month = datetime.today().month

    # 获取当月第一天的星期和当月的总天数
    firstDayWeekDay, monthRange = calendar.monthrange(year, month)

    # 获取当月的第一天
    firstDay = datetime(year=year, month=month, day=1)
    lastDay = datetime(year=year, month=month, day=monthRange)

    return firstDay, lastDay

class TestThreadByQ(threading.Thread):
    """
        run thread by queues, ended by set exitFlag
    """
    def __init__(self, func, queues, queue_lock, total, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.queues = queues
        self.exit_flag = 0
        self.result = []
        self.queueLock = queue_lock
        self.total = total

    def run(self):
        print('开始执行', self.name, ' 在：', ctime())
        self.process_data()
        print(self.name, '结束于：', ctime())

    def get_result(self):
        return self.result

    def process_data(self):
        while not self.exit_flag:
            self.queueLock.acquire()
            if not self.queues.empty():
                data = self.queues.get()
                count = self.queues.qsize()
                print('%s: 开始处理任务 (%s/%s)'%(self.name, count, self.total))
                self.queueLock.release()
                resp = self.func(data)
                self.result.append({
                    'response': resp,
                    'params': data
                })
                print('%s: 完成处理任务 (%s/%s)' % (self.name, count, self.total))
            else:
                self.queueLock.release()

    def set_exit_flag(self, flag):
        self.exit_flag = flag


def run_by_thread(source_datas, run_func, threadNum, ret = False, fun2=None):
    total = len(source_datas)
    queue_lock = threading.Lock()
    queues = queue.Queue(total)
    threads = []
    results = []
    # 启动线程
    for i in range(threadNum):
        name = '线程%s' % i
        tq = TestThreadByQ(run_func, queues, queue_lock, total, name=name)
        tq.start()
        threads.append(tq)
    # 填充队列
    print('queue init')
    queue_lock.acquire()
    for train in source_datas:
        queues.put(train)
    queue_lock.release()
    print('queue end')
    # 等待队列清空
    while not queues.empty():
        if fun2 and queues.qsize()%100 == 0:
            fun2()
        pass

    print('通知线程退出')
    # 通知线程退出
    for t in threads:
        t.set_exit_flag(1)

    print('等待线程完成')
    # 等待线程完成
    for t in threads:
        t.join()
        if ret:
            if results == []:
                results = t.get_result()
            else:
                results.extend(t.get_result())
    print('线程完成')
    if ret:
        return results
