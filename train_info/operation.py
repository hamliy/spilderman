# encoding: utf-8
"""
@author: han.li
@file  : operation.py
@time  : 1/11/19 9:34 AM
@dec   : 火车票获取方法类
"""
import lxml.html
import execjs
import requests, re, json, os
import datetime
import threading
import queue, random
from itest.tools.train_info.db_operation import get_normal_proxys_url,\
    get_set, get_station_code_order_by_name,update_train_seat
from itest.tools.train_info.util import getMonthFirstDayAndLastDay, TestThreadByQ, run_by_thread

etree = lxml.html.etree


def get_train_number():
    """
    从 jt2345网站获取所有火车车次信息
    :return:
    """
    train_numbers = []
    url = "http://www.jt2345.com/huoche/checi"
    resp = requests.get(url)
    html = etree.HTML(resp.text)
    # result = etree.tostring(html)
    # print(result.decode('utf8'))
    html_data = html.xpath('/html/body/center/table/tr/td/a')
    for i in html_data:
        train_numbers.append(i.text)
    return train_numbers


def save_train_station_from_12306(path):
    """
    1. 拿到全国所有车站的电码信息编号,并保存为js
    数据格式:@bjb|北京北|VAP|beijingbei|bjb|0
    可拿到的重要信息有:
        车站名缩写:bjb
        车站名称:北京北
        车站电码编号:VAP
        车站数字编号:0

    :param: path 保存文件路径
    :return:
    """
    url = 'https://kyfw.12306.cn/otn/resources/js/framework/station_name.js'
    resp = requests.get(url, stream=True)
    with open(path, 'wb') as code:
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                code.write(chunk)


def save_train_list_from_12306(path):
    """
    从12306获取所有车次信息（按月更新车次数据），并保存为js
    1. 通过12306月排班表拿到所有的车次信息(未来50天的班次信息)
    :param: month 解析月份 2019-2
    :param: dir_path 保存文件路径
    :return:
    """
    url = 'https://kyfw.12306.cn/otn/resources/js/query/train_list.js'
    # url = 'https://kyfw.12306.cn/otn/resources/js/query/train_list.js?scriptVersion=1.0'
    resp = requests.get(url, stream=True)
    print(resp.text)
    with open(path, 'wb') as code:
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                code.write(chunk)


def get_train_station_list_from_12306(train):
    """
    调用12306 拿到指定车次7天内的所有站点信息
    请求Param:
        train_no: 车次编号
        from_station_telecode: 起始站点的电码编号
        to_station_telecode: 目的站点的电码编号
        depart_date: 查询日期
    返回数据包括:
        station_name: 站名
        arrive_time: 到站时间
        start_time: 出站时间
        stopover_time: 停留时间
        station_no: 车站在该线路的编号
    :return:
    """
    url = 'https://kyfw.12306.cn/otn/czxx/queryByTrainNo'

    params = {
        'train_no': train['trainNo'],
        'from_station_telecode': train['fromCode'],
        'to_station_telecode': train['toCode'],
        'depart_date': train['trainDate']
    }
    try:
        resp = requests.get(url, params=params)
    except ConnectionError:
        resp = {'text': 'TimeoutError'}
    return resp.text


def request_train_station_by_thread(all_train_list, threadNum):
    total = len(all_train_list)
    queue_lock = threading.Lock()
    queues = queue.Queue(total)
    threads = []
    results = []
    # 启动线程
    for i in range(threadNum):
        name = '线程%s' % i
        tq = TestThreadByQ(get_train_station_list_from_12306, queues, queue_lock, total, name=name)
        tq.start()
        threads.append(tq)
    # 填充队列
    print('queue init')
    station_code_obj = get_station_code_order_by_name()
    queue_lock.acquire()
    for train in all_train_list:
        train['fromCode'] = station_code_obj[train['startStation']]
        train['toCode'] = station_code_obj[train['endStation']]
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
        if results == []:
            results = t.get_result()
        else:
            results.extend(t.get_result())
        print(len(results))
    return results


def update_seat_type(train):
    """
    更新车次座位信息
    :return:
    """
    print('get seat %s' % train['trainNo'])
    for i in range(3):
        train_with_seat = update_seat_type_by_query_z(train)
        if train_with_seat['errorCode'] == 0:
            break

    print('update date %s' % train['trainNo'])
    update_train_seat(train_with_seat)


def update_seat_type_by_query_z(train):
    """
    从查询站到站的车次查询 座位号
    :return:
    """
    url = 'https://kyfw.12306.cn/otn/leftTicket/queryZ'
    params = {
        'leftTicketDTO.train_date': train['trainDate'],
        'leftTicketDTO.from_station': train['startCode'],
        'leftTicketDTO.to_station': train['endCode'],
        'purpose_codes': 'ADULT'
    }
    proxies = get_normal_proxys_url()
    proxy_url = random.choice(proxies)
    try:
        resp = requests.get(url, params=params, proxies={'http': proxy_url,
                                                 'https': proxy_url}, timeout=5)
        if '<html' in resp.text:
            print('seat type load failed')
            train['errorCode'] = 404
            train['error'] = 'seat type load failed'
            train['seatType'] = ''
        else:
            result = json.loads(resp.text)
            if result['status']:
                infos = json.loads(resp.text)['data']['result']
                # info = "RxKOJhkdq3meu5VL5x816HPcQl2wprfZS3XkUJeEQ5II3vL1QYAiFI5EJ8IqJj6pKe6a%2Ff%2FtHp1H%0Ar2ViMmXdi1OWU6%2BHnyi0F8xkvCm2Kf24%2FN1J%2FcXFWsMMHBWA6PFINzlV0b%2BZVU8oGUZbm1VrU%2B7y%0AnQMNUPdBS6GuQdthYbWRCnDxyVY%2BXgYtkkDwbn0C3xVEoWoCGe2ku%2BmQyz%2BPOqeJfBy%2FxsDHIWJD%0AADmREzfzyiYWvilvHruCIFADIyNpTvmro%2BVZ%2FUe70j2mIKWIeUNfGKg%2FI66BLP%2BMkU8J5uvSgIye%0AIqBMcA%3D%3D|预订|330000K59810|K599|BTC|GZQ|BXP|GZQ|05:14|11:06|29:52|Y|31RMgZmiY%2FUh40pakE45kIplgFGat2H4doHPTv3VgBF7wVbYVRd8OvcOnw4%3D|20190122|3|C1|11|34|0|0||||无|||有||无|有|||||10401030|1413|1|0|null"
                if len(infos) == 0:
                    train['errorCode'] = 403
                    train['error'] = 'train list not found'
                    train['seatType'] = ''
                for info in infos:
                    seat_type = info.split('|')[-4]
                    train_no = info.split('|')[2]
                    # train_number = info.split('|')[3]
                    if train_no == train['trainNo']:
                        train['seatType'] = seat_type
                        break
                if 'seatType' not in train:
                    train['errorCode'] = 404
                    train['error'] = 'train_no not found'
                    train['seatType'] = ''
            else:
                train['errorCode'] = 401
                train['seatType'] = ''
                train['error'] = 'seat_type query error'

    except Exception as e:
        train['errorCode'] = 402
        train['seatType'] = ''
        train['error'] = 'seat_type query error: %s' % e
    return train


def request_train_seat_type_by_thread(all_train_list, threadNum):
    total = len(all_train_list)
    queue_lock = threading.Lock()
    queues = queue.Queue(total)
    threads = []
    results = []
    # 启动线程
    for i in range(threadNum):
        name = '线程%s' % i
        tq = TestThreadByQ(update_seat_type_by_query_z, queues, queue_lock, total, name=name)
        tq.start()
        threads.append(tq)
    # 填充队列
    print('queue init')
    queue_lock.acquire()
    for train in all_train_list:
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
        if results == []:
            results = t.get_result()
        else:
            results.extend(t.get_result())
        print(len(results))
    return results


def get_all_train_station_list_from_12306(year, month):
    """
    获取所有车次的站点信息(查询7天内)
    根据对应的年月的车次信息查询并并更新数据
    :param year:
    :param month:
    :return:
    """
    set = get_set('train_list')
    fist_day, last_day = getMonthFirstDayAndLastDay(year, month)
    all_train = set.find({'createTime':{ "$gte" : fist_day, "$lt" : last_day }})
    all_train_list = []
    all_train_station_list = []
    for train in all_train:
        # C3开头的火车 主要是金山卫到上海南的城际火车，12306 找不到该车次及车站，先过滤掉
        # 香港 九龙 12306查找不到 ，先过滤
        # 沙岭 客运属于乘降所，不网售
        # 梅江 找不到
        black_list = ['九龙', '沙岭', '梅江', '元龙', '马海', '唐山南']
        if not train['trainNumber'].startswith('C3') and train['endStation'] not in black_list \
                and train['startStation'] not in black_list:
            all_train_list.append(train)
        else:
            all_train_station_list.append({
                'header': train['header'],
                'trainNumber': train['trainNumber'],
                'trainNo': train['trainNo'],
                'trainDate': train['trainDate'],
                'startStation': train['startStation'],
                'endStation': train['endStation'],
                'startCode': '',
                'endCode': '',
                'stations': [],
                'errorCode': 303,
                'error': 'Station code not found'
            })
    print(len(all_train_list),all_train_list[0])
    results = request_train_station_by_thread(all_train_list, 10)
    for result in results:
        info = result['response']
        train = result['params']
        if info == 'TimeoutError':
            print('get error for %s' % train)
            all_train_station_list.append({
                'header': train['header'],
                'trainNumber': train['trainNumber'],
                'trainNo': train['trainNo'],
                'trainDate': train['trainDate'],
                'startStation': train['startStation'],
                'endStation': train['endStation'],
                'startCode': train['fromCode'],
                'endCode': train['toCode'],
                'stations': [],
                'errorCode': 304,
                'error': 'Load station list failed'
            })
        elif '<html' in info:
            print('get error for %s, response %s' % (train, info))
            all_train_station_list.append({
                'header': train['header'],
                'trainNumber': train['trainNumber'],
                'trainNo': train['trainNo'],
                'trainDate': train['trainDate'],
                'startStation': train['startStation'],
                'endStation': train['endStation'],
                'startCode': train['fromCode'],
                'endCode': train['toCode'],
                'stations': [],
                'errorCode': 301,
                'error': 'TimeoutError'
            })
        else:
            print(info)
            station_list = json.loads(info)['data']['data']
            if len(station_list) == 0:
                print('get error for %s' % train)
                all_train_station_list.append({
                    'header': train['header'],
                    'trainNumber': train['trainNumber'],
                    'trainNo': train['trainNo'],
                    'trainDate': train['trainDate'],
                    'startStation': train['startStation'],
                    'endStation': train['endStation'],
                    'startCode': train['fromCode'],
                    'endCode': train['toCode'],
                    'stations': [],
                    'errorCode': 302,
                    'error': 'Not found'
                })
            else:
                stations = []
                for station in station_list:
                    stations.append({
                        'stationName': station['station_name'],
                        'arriveTime': station['arrive_time'],
                        'startTime': station['start_time'],
                        'stopoverTime': station['stopover_time'],
                        'stationNo': station['station_no']
                    })
                all_train_station_list.append({
                    'header': train['header'],
                    'trainNumber': train['trainNumber'],
                    'trainNo': train['trainNo'],
                    'trainDate': train['trainDate'],
                    'startStation': train['startStation'],
                    'endStation': train['endStation'],
                    'startCode': train['fromCode'],
                    'endCode': train['toCode'],
                    'stations': stations,
                    'errorCode': 0,
                    'error': ''
                })
    return all_train_station_list


def update_all_train_seat_type():
    """
    获取所有车次的座席信息信息(查询7天内)
    根据对应的年月的车次信息查询并并更新数据
    :param year:
    :param month:
    :return:
    """
    year = datetime.datetime.utcnow().year  # 当前月份
    month = datetime.datetime.utcnow().month  # 当前月份
    dbset = get_set('train_station_list')
    fist_day, last_day = getMonthFirstDayAndLastDay(year, month)
    train_0 = dbset.find_one({'createTime':{ "$gte" : fist_day, "$lt" : last_day}, 'errorCode':0})
    # 如果这个月已经有数据了 且存在座位字段 不更新
    if train_0 and 'seatType' in train_0:
        print('All seat_type ready update this month %s ' % datetime.datetime.utcnow().strftime('%Y-%m'))
    else:
        all_train_stations = dbset.find({'createTime': {"$gte": fist_day, "$lt": last_day}, 'errorCode': 0})
        all_train_list = []
        for train in all_train_stations[0:10]:
            all_train_list.append(train)
        run_by_thread(all_train_list, update_seat_type, 5 , True)
        # results = request_train_seat_type_by_thread(all_train_list, 5)
        # for result in results:
        #     c = {'createTime': {"$gte": fist_day, "$lt": last_day},
        #          'trainNo': result['response']['trainNo'],
        #          'trainNumber': result['response']['trainNumber']}
        #     t = dbset.find_one(c)
        #     t['seatType'] = result['response']['seatType']
        #     t['errorCode'] = result['response']['errorCode']
        #     t['error'] = result['response']['error']
        #     print('update train_no %s, seat_type %s' % (result['response']['trainNo'], result['response']['seatType']))
        #     dbset.update(c, t)



def analyze_train_number_js(js_path):
    """
    解析月排班数据，且数据去重
    :param js_path:
    :return:
    """
    context = execjs.compile(open(js_path).read())
    train_data = context.eval('train_list')
    train_all_data = {}
    for date, datas in train_data.items():
        print('anaylze %s'% date)
        for header, list in datas.items():
            if header not in train_all_data:
                train_all_data[header] = []
            for info in list:
                isHad = False
                for item in train_all_data[header]:
                    if info['station_train_code'] == item['station_train_code'] and info['train_no']==item['train_no']:
                        isHad = True
                        break
                if not isHad:
                    train_all_data[header].append({
                        'station_train_code': info['station_train_code'],
                        'train_no': info['train_no'],
                        'train_date': date
                    })
    return train_all_data


def analyze_train_station_js(js_path):
    """
    解析车站数据，且数据去重
    数据格式 var station_names = '@bjb|北京北|VAP|beijingbei|bjb|0@...'
    提取的重要数据有:
        车站名缩写:bjb
        车站名称:北京北
        车站电码编号:VAP
        车站数字编号:0
    :param js_path:
    :return:
    """
    context = execjs.compile(open(js_path).read())
    station_data = context.eval('station_names')
    all_train_station = []
    # 按@ 分隔，移除第一位 空 其他为 bjb|北京北|VAP|beijingbei|bjb|0
    station_list = station_data.split('@')
    station_list.pop(0)
    for station_str in station_list:
        # 解析bjb|北京北|VAP|beijingbei|bjb|0 转换为 bjb 北京北 VAP beijingbei bjb 0
        str_list = station_str.split('|')
        sample_name = str_list[0]
        station_name = str_list[1]
        station_code = str_list[2]
        station_pinyin = str_list[3]
        station_no = str_list[5]
        all_train_station.append({
            'sampleName': sample_name,
            'stationName': station_name,
            'stationCode': station_code,
            'stationPinyin': station_pinyin,
            'stationNo': station_no,
        })
    return all_train_station


def update_train_number_data():
    """
    更新车次数据(每月更新一次)
    :return:
    """
    js_dir = os.path.join(os.getcwd(), 'train_list')
    if not os.path.exists(js_dir):
        os.mkdir(js_dir)
    month = datetime.datetime.utcnow().strftime('%Y-%m')  # 当前月份
    js_path = os.path.join(js_dir, 'train_list_%s.js' % month)  # 保存地址
    # 如果已存在不再下载
    if os.path.exists(js_path):
        print('All ready update this month %s ' % month)
    else:
        # 获取并保存月车次数据
        save_train_list_from_12306(js_path)
        print('begin analyze data')
        # 解析车次数据
        train_data = analyze_train_number_js(js_path)
        # 保存车次数据
        all = []
        time = datetime.datetime.utcnow()
        for header, list in train_data.items():
            print(header, len(list), list[0])
            for info in list:
                # 解析D1(北京-沈阳南) 转换为 D1 北京 沈阳南
                s = re.search('(.*)\((.*)-(.*)\)', info['station_train_code'])
                train_info = {
                    'header': header,
                    'stationTrainCode': info['station_train_code'],
                    'trainNumber': s[1],
                    'startStation': s[2],
                    'endStation': s[3],
                    'trainNo': info['train_no'],
                    'trainDate': info['train_date'],
                    'createTime': time,
                    'updateTime': time
                }
                all.append(train_info)
        print(len(all), all[0])
        print('start update db')
        set = get_set('train_list')
        set.insert_many(all)


def update_train_station_data():
    """
    更新车站数据(每月更新一次)
    :return:
    """
    js_dir = os.path.join(os.getcwd(), 'train_station')
    if not os.path.exists(js_dir):
        os.mkdir(js_dir)
    month = datetime.datetime.utcnow().strftime('%Y-%m')  # 当前月份
    js_path = os.path.join(js_dir, 'train_station_%s.js' % month)  # 保存地址
    # 如果已存在不再下载
    if os.path.exists(js_path):
        print('All ready update this month %s ' % month)
    else:
        # 获取并保存月车站数据
        save_train_station_from_12306(js_path)
        print('begin analyze station data')
        # 解析车次获取车站信息
        all_train_station = analyze_train_station_js(js_path)
        time = datetime.datetime.utcnow()
        all = []
        for train_station in all_train_station:
            train_station['createTime'] = time
            train_station['updateTime'] = time
            all.append(train_station)
        print(len(all), all[0])
        print('start update db for train_station')
        set = get_set('train_station')
        set.insert_many(all)


def update_train_station_list_data():
    """
    更新车次站点数据(每月更新一次)
    :return:
    """
    year = datetime.datetime.utcnow().year  # 当前月份
    month = datetime.datetime.utcnow().month  # 当前月份
    set = get_set('train_station_list')
    fist_day, last_day = getMonthFirstDayAndLastDay(year, month)
    all_train_stations = set.find({'createTime':{ "$gte" : fist_day, "$lt" : last_day }})
    # 如果这个月已经有数据了 不更新
    if all_train_stations.count() < 0:
        print('All ready update this month %s ' % datetime.datetime.utcnow().strftime('%Y-%m'))
    else:
        all_train_station_list = get_all_train_station_list_from_12306(year, month)
        time = datetime.datetime.utcnow()
        all = []
        for train_station in all_train_station_list:
            train_station['createTime'] = time
            train_station['updateTime'] = time
            all.append(train_station)
        print(len(all), all[0])
        print('start update db for train_station_list')
        set.insert_many(all)

def update_train_price_data():
    """
    更新火车票价格信息
    :return:
    """

#
# def get_train_station_by_number(train_number):
#     url = "http://www.jt2345.com/huoche/checi/%s.htm" % train_number
#     resp = requests.get(url)
#     # 默认编码
#     coding = 'utf-8'
#     if resp.encoding == 'ISO-8859-1':
#         # 'ISO-8859-1'对应Latin1 编码
#         coding = 'latin1'
#     try:
#         change_text = resp.text.encode(coding).decode("gbk")
#     except UnicodeDecodeError:
#         print(resp.text.encode(coding))
#         change_text = resp.text
#     html = etree.HTML(change_text)
#     # print(change_text)
#     # print(etree.tostring(html, encoding='gb2312'))
#     base_info_table = html.xpath('/html/body/center/table')[0]
#     station_info_table = html.xpath('/html/body/center/table')[1]
#     base_info_tr = base_info_table.xpath('./tr')
#     station_info_tr = station_info_table.xpath('./tr')
#     base_info = {'车次': train_number}
#     station_info = []
#     data = {
#         'checi': '',
#         'start':'',
#         'start_time':'',
#         'end': '',
#         'end_time': '',
#         'price': '',
#         'cost_time': '',
#         'zuoxi':{'type':'price', '':''}
#     }
#     for i in base_info_tr:
#         data = i.xpath('./td/text()')
#         if len(data) == 2:
#             base_info[data[0].encode(coding).decode('gbk')] = data[1].encode(coding).decode('gbk')
#     # for i in station_info_tr[0]:
#     #     print(i.text)
#     for i in station_info_tr:
#         if len(i.xpath('./td/a/text()')) > 0:
#             station_name = i.xpath('./td/a/text()')[0]
#
#             print(station_name)
#             station_number = i.xpath('./td[1]/text()')[0].encode(coding).decode('gbk')
#             station_info.append({
#                 'station_number': station_number,
#                 'station_name': station_name.encode(coding).decode('gbk')
#             })
#     print(base_info,station_info)
#     return base_info, station_info
#
#
# def init_tain_info():
#     """
#     初始化所有车次+车站信息
#     :return:
#     """
#     all_train_numbers = get_train_number()
#     print(len(all_train_numbers), all_train_numbers[0])
#     all_count = len(all_train_numbers)
#     count = 1
#     for train_number in all_train_numbers[261:]:
#         print('init: %s（%s/%s）' %(train_number, count, all_count))
#         base_info, station_info = get_train_station_by_number(train_number)
#         # update_train_info({
#         #     'baseInfo': base_info,
#         #     'trainNumber': train_number,
#         #     'stationInfo': station_info,
#         #     'createTime': datetime.utcnow(),
#         #     'desc': 'init'
#         # })
#         count +=1
#
# def replasceCharEntity(htmlstr):
#     CHAR_ENTITYES= {
#         'nbsp': ' ', '160': ' ',
#         'lt': '<', '60': '<',
#         'gt': '>', '62': '>',
#         'amp': '&', '38': '&',
#         'quot': '"', '34': '"'
#     }
#     re_charEntity = re.compile(r'&#?(?P<name>\w+);')
#     sz = re_charEntity.search(htmlstr)
#     while sz:
#         key = sz.group('name') # 去除&#：
#         try:
#             htmlstr = re_charEntity.sub(CHAR_ENTITYES[key], htmlstr, 1)
#             sz = re_charEntity.search(htmlstr)
#         except KeyError:
#             # 以空串代替
#             htmlstr = re_charEntity.sub('', htmlstr)
#             sz = re_charEntity.search(htmlstr)
#     return htmlstr


if __name__ == '__main__':
    # init_train_number_data()
    # update_train_number_data()
    # update_train_station_data()
    # update_train_station_list_data()
    # data = "RxKOJhkdq3meu5VL5x816HPcQl2wprfZS3XkUJeEQ5II3vL1QYAiFI5EJ8IqJj6pKe6a%2Ff%2FtHp1H%0Ar2ViMmXdi1OWU6%2BHnyi0F8xkvCm2Kf24%2FN1J%2FcXFWsMMHBWA6PFINzlV0b%2BZVU8oGUZbm1VrU%2B7y%0AnQMNUPdBS6GuQdthYbWRCnDxyVY%2BXgYtkkDwbn0C3xVEoWoCGe2ku%2BmQyz%2BPOqeJfBy%2FxsDHIWJD%0AADmREzfzyiYWvilvHruCIFADIyNpTvmro%2BVZ%2FUe70j2mIKWIeUNfGKg%2FI66BLP%2BMkU8J5uvSgIye%0AIqBMcA%3D%3D|预订|330000K59810|K599|BTC|GZQ|BXP|GZQ|05:14|11:06|29:52|Y|31RMgZmiY%2FUh40pakE45kIplgFGat2H4doHPTv3VgBF7wVbYVRd8OvcOnw4%3D|20190122|3|C1|11|34|0|0||||无|||有||无|有|||||10401030|1413|1|0|null"
    # info = data.split('|')
    # print(info[-4])
    # print(info[2])
    # update_all_train_seat_type()
    update_train_number_data()