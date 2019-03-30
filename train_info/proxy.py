# encoding: utf-8
"""
@author: han.li
@file  : proxy.py
@time  : 1/22/19 2:15 PM
@dec   : 获取ip代理
"""

import requests, threading, queue
import lxml.html
from itest.tools.train_info.db_operation import save_proxy, get_proxys_url, update_proxy_status, clear_abnormal_proxy
from itest.tools.train_info.util import TestThreadByQ
import time
etree = lxml.html.etree


def update_proxy_ips_89():
    headers = {}
        # 'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36'}
    url = 'http://www.89ip.cn/tqdl.html?num=600'
    s = requests.get(url, headers=headers)
    if s.status_code == 200:
        html = etree.HTML(s.text)
        ips = html.xpath("//div[@class='layui-col-md8']/div/div/text()")
        # ips = strs.spilt(',')
        # print(ips)
        del ips[0]
        del ips[-1]
        # clear_proxy()
        clear_abnormal_proxy()
        for ip in ips:
            save_proxy({
                'url':ip,
                'status': 'uncheck'
            })
    else:
        print('get url %s failed' % url)


def check_proxy_url_status(proxy_url):
        # url = 'http://www.baidu.com'
        # url = 'https://kyfw.12306.cn/otn/leftTicket/queryZ'
        url = 'http://m.114piaowu.com'
        try:
            # s = requests.get(url, proxies={p['connectType'].lower(): p_url})
            s = requests.get(url, proxies={'http': proxy_url,
                                           'https': proxy_url}, timeout=5)
            if s.status_code == 200 and '114票务网' in s.text:
                update_proxy_status(proxy_url, 'normal')
                print(proxy_url)
            else:
                print(proxy_url, s.status_code)
                update_proxy_status(proxy_url, 'abnormal')
        except Exception as e:
            print(proxy_url, e)
            update_proxy_status(proxy_url, 'abnormal')


def check_by_thread(source_datas, run_func, threadNum, ret = False):
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
        pass

    # 通知线程退出
    for t in threads:
        t.set_exit_flag(1)

    # 等待线程完成
    for t in threads:
        t.join()
        if ret:
            if results == []:
                results = t.get_result()
            else:
                results.extend(t.get_result())
    if ret:
        return results


def init_proxy():
    update_proxy_ips_89()
    proxies = get_proxys_url()
    check_by_thread(proxies, check_proxy_url_status, 10)

def recheck_proxy():
    proxies = get_proxys_url()
    check_by_thread(proxies, check_proxy_url_status, 10)

def timer(n):
    '''''
    每n秒执行一次
    '''
    while True:
        print(time.strftime('%Y-%m-%d %X',time.localtime()))
        init_proxy()  # 此处为要执行的任务
        time.sleep(n)

if __name__ == '__main__':
    timer(3600)
