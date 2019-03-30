# encoding: utf-8
"""
@author: han.li
@file  : util.py
@time  : 3/19/19 7:32 PM
@dec   : 通用方法
"""

from city_info.config import Config
import os, requests
import threading, queue
from time import ctime
from requests.exceptions import ReadTimeout
from urllib3.exceptions import ReadTimeoutError


def splice_url(herf):
    """
    组合跳转链接
    :param herf:
    :return:
    """
    return os.path.join(Config.SPIDER_URL['base'], herf)


def request_get(url):
    try:
        resp = requests.get(url=url, timeout=5)
        # 默认编码
        coding = 'utf-8'
        if resp.encoding == 'ISO-8859-1':
            # 'ISO-8859-1'对应Latin1 编码
            coding = 'latin1'
        try:
            change_text = resp.text.encode(coding).decode("gbk")
        except UnicodeDecodeError:
            print(resp.text.encode(coding))
            change_text = resp.text
    except ReadTimeout:
        return request_get(url)
    except ReadTimeoutError:
        return request_get(url)
    return change_text


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




