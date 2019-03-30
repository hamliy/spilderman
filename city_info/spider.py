# encoding: utf-8
"""
@author: han.li
@file  : spider.py
@time  : 3/19/19 7:15 PM
@dec   : 
"""

import lxml.html
from city_info.util import splice_url, request_get, run_by_thread
from city_info.db_operation import update_info, is_had_city, get_by_type, get_not_update_county
import os

ETREE = lxml.html.etree


def explain_index(text):
    """
    解析首页获取省份信息
    :param text:
    :return:
    """
    html = ETREE.HTML(text)
    province_line = html.xpath("//tr[@class='provincetr']")
    province_info = []
    for line in province_line:
        p_td = line.xpath('./td')
        for p in p_td:
            province = p.xpath('./a/text()')[0]
            province_url = p.xpath('./a/@href')[0]
            province_info.append({
                'name': province,
                'url': province_url,
                'code': province_url.split('.')[0],
                'type': 'province',
                'connection': {
                    'province': '',
                    'city': '',
                    'county': '',
                    'town': ''
                }
            })
    return province_info


def explain_city(text, province):
    """
    解析省份获取城市信息
    :param text:
    :return:
    """
    html = ETREE.HTML(text)
    lines = html.xpath("//tr[@class='citytr']")
    city_info = []
    for line in lines:
        code = line.xpath('./td/a/text()')[0]
        href = line.xpath('./td/a/@href')[0]
        city = line.xpath('./td/a/text()')[1]
        city_info.append({
            'name': city,
            'url': href,
            'code': code,
            'type': 'city',
            'connection': {
                'province': province,
                'city': '',
                'county': '',
                'town': ''
            }
        })
    return city_info


def explain_county(text, city):
    """
    解析城市获取城镇信息
    :param text:
    :return:
    """
    html = ETREE.HTML(text)
    lines = html.xpath("//tr[@class='countytr']")
    county_info = []
    city_href = city['url'].split('/')[0]
    for line in lines:
        if len(line.xpath('./td/a/text()')) == 0:
            code = line.xpath('./td/text()')[0]
            county = line.xpath('./td/text()')[1]
            county_info.append({
                'name': county,
                'url': '',
                'code': code,
                'type': 'county',
                'connection': {
                    'province': city['connection']['province'],
                    'city': city['name'],
                    'county': '',
                    'town': ''
                }
            })
        else:
            code = line.xpath('./td/a/text()')[0]
            href = line.xpath('./td/a/@href')[0]
            county = line.xpath('./td/a/text()')[1]
            county_info.append({
                'name': county,
                'url': os.path.join(city_href, href),
                'code': code,
                'type': 'county',
                'connection': {
                    'province': city['connection']['province'],
                    'city': city['name'],
                    'county': '',
                    'town': ''
                }
            })
    return county_info


def explain_town(text, county):
    """
    解析城镇获取村子信息
    :param text:
    :return:
    """
    html = ETREE.HTML(text)
    lines = html.xpath("//tr[@class='towntr']")
    town_info = []
    county_href = os.path.join(county['url'].split('/')[0],county['url'].split('/')[1])
    for line in lines:
        code = line.xpath('./td/a/text()')[0]
        href = line.xpath('./td/a/@href')[0]
        town = line.xpath('./td/a/text()')[1]
        town_info.append({
            'name': town,
            'url': os.path.join(county_href, href),
            'code': code,
            'type': 'town',
            'connection': {
                'province': county['connection']['province'],
                'city': county['connection']['city'],
                'county': county['name'],
                'town': ''
            }
        })
    return town_info


def explain_village(text, town):
    """
    解析城镇获取村子信息
    :param text:
    :return:
    """
    html = ETREE.HTML(text)
    lines = html.xpath("//tr[@class='villagetr']")
    village_info = []
    for line in lines:
        code = line.xpath('./td/text()')[0]
        href = line.xpath('./td/text()')[1]
        village = line.xpath('./td/text()')[2]
        village_info.append({
            'name': village,
            'url': href,
            'code': code,
            'type': 'village',
            'connection': {
                'province': town['connection']['province'],
                'city': town['connection']['city'],
                'county': town['connection']['county'],
                'town': town['name'],
            }
        })
    return village_info


def update_provice():
    """
    更新省份信息
    :return:
    """
    url = splice_url('index.html')
    provinces = explain_index(request_get(url))
    for p in provinces:
        if not is_had_city(p):
            update_info(p)


def update_city(province):
    """
    更新省份信息
    :return:
    """
    url = splice_url(province['url'])
    citys = explain_city(request_get(url), province['name'])

    for c in citys:
        if not is_had_city(c):
            update_info(c)
            print(c)

    print('更新省份：%s - 城市数：%s' % (province['name'], len(citys)))


def update_citys():
    provinces = get_by_type('province')
    for province in provinces:
        print('开始查询省份：%s' % province['name'])
        update_city(province)


def update_county(city):
    """
    更新省份信息
    :return:
    """
    url = splice_url(city['url'])
    countys = explain_county(request_get(url), city)

    for c in countys:
        if not is_had_city(c):
            update_info(c)
        else:
            print('error%s' % c)
    print('更新城市：%s - 数量：%s' % (city['name'], len(countys)))


def update_countys():
    citys = get_by_type('city')
    for city in citys:
        print('开始查询城市：%s' % city['name'])
        update_county(city)



def update_town(county):
    """
    更新村子信息
    :return:
    """
    if county['url'] == '':
        return
    url = splice_url(county['url'])
    towns = explain_town(request_get(url), county)

    for t in towns:
        if not is_had_city(t):
            update_info(t)
        else:
            print('error%s' % t)
    print('更新县区：%s-%s-%s- 数量：%s' % ( county['connection']['province'],
                                     county['connection']['city'], county['name'], len(towns)))


def update_towns():
    countys = get_by_type('county')
    # count = 1
    # total = len(countys)
    # for county in countys:
    #     print('%s/%s 开始查询县区：%s-%s-%s' % (count, total,county['connection']['province'],
    #                                      county['connection']['city'], county['name']))
    #     update_town(county)
    #     count +=1

    run_by_thread(countys, update_town, 10)


def update_village(town):
    """
    更新村子信息
    :return:
    """
    url = splice_url(town['url'])
    villages = explain_village(request_get(url), town)

    for v in villages:
        if not is_had_city(v):
            update_info(v)
        else:
            print('error%s' % v)
    print('更新县区：%s - 数量：%s' % (town['name'], len(villages)))


def update_villages():
    towns = get_by_type('town')
    # for town in towns[10:11]:
    #     print('开始查询县区：%s' % town['name'])
    #     update_village(town)
    run_by_thread(towns[24400:], update_village, 10)

def update_failed_towns():
    fail_countys = get_not_update_county()
    count = 1
    total = len(fail_countys)
    for county in fail_countys:
        print('%s/%s 开始查询县区：%s-%s-%s' % (count, total,county['connection']['province'],
                                         county['connection']['city'], county['name']))
        update_town(county)
        count +=1

def run_spider():
    # update_provice()
    # update_citys()
    # update_countys()
    # update_towns()
    # update_failed_towns()
    update_villages()

if __name__ == '__main__':
    run_spider()
    # print(get_all_province())
    # url = splice_url('32/06/320671.html')
    # print(request_get(url))