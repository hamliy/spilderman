# encoding: utf-8
"""
@author: han.li
@file  : get_from_114.py
@time  : 1/21/19 4:55 PM
@dec   : 从114网站获取价格数据
"""

from itest.tools.train_info.db_operation import get_current_train_ticket_list, is_had_ticket,\
    update_train_ticket, get_normal_proxys_url, update_proxy_status, get_loaded_train_ticket,\
    get_fail_train_ticket, get_trains, remove_ticket
from itest.tools.train_info.util import run_by_thread
from itest.tools.train_info.proxy import recheck_proxy
import requests, random
import lxml.html

etree = lxml.html.etree


def check_proxy_url_status(proxy):
    print('reget proxy url')
    url = 'http://m.114piaowu.com/huochepiao/xiamenbei-shanghaihongqiao_G1658.html'
    try:
        # s = requests.get(url, proxies={p['connectType'].lower(): p_url})
        s = requests.get(url, proxies={'http': proxy,
                                       'https': proxy}, timeout=5)
        if s.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        return False

def reset_proxys():
    global PROXYS
    PROXYS = get_normal_proxys_url()

def get_proxy_url(proxys):
    for proxy in proxys:
        if check_proxy_url_status(proxy):
            return proxy
    print('not can')
    return random.choice(proxys)

def request_always(url, params):

    # proxy_url = get_proxy_url(PROXYS)
    proxy_url = random.choice(PROXYS)
    try:
        resp = requests.get(url=url, params=params, proxies={'http': proxy_url,
                                                             'https': proxy_url}, timeout=5)
    except Exception as e:
        # reset_proxys(proxy_url)
        resp = request_always(url, params)
    return resp

def get_ticket(ticket):
    train_no = ticket['trainNumber']
    start = ticket['startStation']
    end = ticket['endStation']
    date = ticket['queryDate']
    url = 'http://m.114piaowu.com/huochepiao/xiamenbei-shanghaihongqiao_G1658.html'
    params = {
        'trainNumber': train_no,
        'startStation': start,
        'endStation': end,
        'goDate': date,
        'seatType': 'WZ'
    }
    resp = request_always(url, params)
    html = etree.HTML(resp.text)
    tickets= []
    if html is not None:
        failed_info = html.xpath('/html/body/article')
        if len(failed_info) > 0:
            print('get failed')
            ticket = {
                'trainNumber': train_no,
                'startStation': start,
                'endStation': end,
                'queryDate': date,
                'seatType': '',
                'seatPrice': '',
                'errorCode': 1001,
                'error': 'query failed'
            }
            tickets.append(ticket)
        else:
            seat_list_div = html.xpath("//div[@id='seatList_id']")
            if len(seat_list_div) == 0:
                ticket = {
                    'trainNumber': train_no,
                    'startStation': start,
                    'endStation': end,
                    'queryDate': date,
                    'seatType': '',
                    'seatPrice': '',
                    'errorCode': 1002,
                    'error': 'seat_list_div not found'
                }
                tickets.append(ticket)
            else:
                seat_list = seat_list_div[0].xpath('./div/div[1]')
                for seat in seat_list:
                    seat_type = seat.xpath("./span[@class='seat-type']/text()")
                    seat_price = seat.xpath("./span/span[@class='price']/text()")
                    ticket = {
                        'trainNumber': train_no,
                        'startStation': start,
                        'endStation': end,
                        'queryDate': date,
                        'seatType': seat_type[0],
                        'seatPrice': seat_price[0].replace('元', ''),
                        'errorCode': 0,
                        'error': ''
                    }
                    tickets.append(ticket)
    else:
        print('get resp failed')
        ticket = {
            'trainNumber': train_no,
            'startStation': start,
            'endStation': end,
            'queryDate': date,
            'seatType': '',
            'seatPrice': '',
            'errorCode': 1003,
            'error': 'resp.txet is none'
        }
        tickets.append(ticket)
    return tickets


def get_tickets(train):
    tickets = []
    count = 1
    for train_ticket in train['trainTicket']:
        t = dict()
        t['trainNumber'] = train['trainNumber']
        t['startStation'] = train_ticket['startStation']
        t['endStation'] = train_ticket['endStation']
        t['queryDate'] = train['trainDate']
        print('get train %s, total %s/%s' % (train['trainNumber'], count, len(train['trainTicket'])))
        ts = get_ticket(t)
        tickets.extend(ts)
        count +=1
    # print(tickets)
    print('get ok %s' % train['trainNumber'])
    return tickets


PROXYS = get_normal_proxys_url()

def get_update_ticket(train):
    tickets = get_tickets(train)
    for t in tickets:
        if not is_had_ticket(t):
            print(t)
            update_train_ticket(t)

def update_train_price():
    all_trains = get_current_train_ticket_list()
    loaded_train_numbers = get_loaded_train_ticket()
    list = []
    for train in all_trains:
        if train['trainNumber'] in loaded_train_numbers:
            list.append(train)
    for i in list:
        all_trains.remove(i)
    run_by_thread(all_trains, get_update_ticket, 10, fun2=reset_proxys)


def get_update_fail_ticket(ticket_info):
    tickets = get_ticket(ticket_info)
    remove_ticket(ticket_info['_id'])
    for ticket in tickets:
        if ticket['errorCode'] not in [1002, 1003]:
            # print(ticket_info['_id'])
            print(ticket)
            update_train_ticket(ticket)
def get_train_date(all_trains, trainNo):
    for train in all_trains:
        if trainNo == train['trainNumber']:
            return train['trainDate']
def update_failed_train_price():
    fail_tickets = get_fail_train_ticket()
    tickets = []
    print(len(fail_tickets))
    all_trains = get_trains()
    print(len(all_trains))
    for ticket in fail_tickets:
        date = get_train_date(all_trains, ticket['trainNumber'])
        if date:
            ticket['queryDate'] = date
            tickets.append(ticket)
        else:
            print('%s not find date' % ticket['trainNumber'])
    print(len(tickets))
    run_by_thread(tickets, get_update_fail_ticket, 10, fun2=reset_proxys)

if __name__ == '__main__':
    # update_train_price()
    update_failed_train_price()