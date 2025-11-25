# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 17:35:23 2025

@author: 123shu
"""

import os
import re
import requests
import datetime
from pymongo import MongoClient

# 请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Mobile Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-encoding": "gzip, deflate, br",
}

# MongoDB 连接
client = MongoClient('mongodb://localhost:27017/')
db = client['weibo']
collection = db['weibo_data']

def trans_time(v_str):
    """转换GMT时间为标准格式"""
    GMT_FORMAT = '%a %b %d %H:%M:%S +0800 %Y'
    try:
        timeArray = datetime.datetime.strptime(v_str, GMT_FORMAT)
        ret_time = timeArray.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        ret_time = v_str  # 如果解析失败，返回原始字符串
    return ret_time

def get_containerid(v_loc):
    """
    获取地点对应的containerid
    :param v_loc: 地点
    :return: containerid
    """
    url = 'https://m.weibo.cn/api/container/getIndex'
    params = {
        "containerid": f"100103type=92&q={v_loc}&t=",
        "page_type": "searchall",
    }
    try:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        cards = r.json().get("data", {}).get("cards", [])
        if not cards:
            raise ValueError("No cards found in response.")
        scheme = cards[0].get('card_group', [{}])[0].get('scheme', '')
        containerid = re.findall(r'containerid=(.*?)(?:&|$)', scheme)[0]
    except (requests.RequestException, IndexError, KeyError, ValueError) as e:
        print(f"Error fetching containerid for {v_loc}: {e}")
        containerid = None
    return containerid

def getLongText(v_id):
    """爬取长微博全文"""
    url = f'https://m.weibo.cn/statuses/extend?id={v_id}'
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        json_data = r.json()
        long_text = json_data['data']['longTextContent']
        dr = re.compile(r'<[^>]+>', re.S)
        long_text2 = dr.sub('', long_text)
    except (requests.RequestException, KeyError) as e:
        print(f"Error fetching long text for ID {v_id}: {e}")
        long_text2 = ''
    return long_text2

def get_location(v_text):
    """
    从博文中提取签到地点
    :param v_text: 博文
    :return: 地点
    """
    loc = ''
    if v_text:
        try:
            loc = re.findall(r'<span class=\"surl-text\">(.*?)</span>', v_text)[-1]
        except IndexError:
            pass
    return loc

def get_weibo_data(card):
    """
    从微博卡片中提取数据
    :param card: 微博卡片
    :return: 提取的数据字典
    """
    mblog = card['mblog']
    create_time = trans_time(mblog['created_at']) if 'created_at' in mblog else ''
    author = mblog['user']['screen_name'] if 'user' in mblog and 'screen_name' in mblog['user'] else ''
    id = mblog.get('id', '')
    bid = mblog.get('bid', '')
    text = mblog.get('text', '')
    dr = re.compile(r'<[^>]+>', re.S)
    text2 = dr.sub('', text)
    if mblog.get('isLongText'):
        text2 = getLongText(id)
    loc = get_location(v_text=text)
    reposts_count = mblog.get('reposts_count', '')
    comments_count = mblog.get('comments_count', '')
    attitudes_count = mblog.get('attitudes_count', '')

    return {
        '微博id': id,
        '微博bid': bid,
        '微博作者': author,
        '发布时间': create_time,
        '微博内容': text2,
        '签到地点': loc,
        '转发数': reposts_count,
        '评论数': comments_count,
        '点赞数': attitudes_count,
    }

def get_weibo_list(v_keyword, v_max_page):
    """
    爬取微博内容列表
    :param v_keyword: 搜索关键字
    :param v_max_page: 爬取前几页
    :return: None
    """
    containerid = get_containerid(v_loc=v_keyword)
    if not containerid:
        print(f"Could not retrieve containerid for keyword: {v_keyword}")
        return

    for page in range(2, v_max_page + 1):
        print(f'===开始爬取第{page}页微博===')
        url = 'https://m.weibo.cn/api/container/getIndex'
        params = {
            "containerid": containerid,
            "luicode": "10000011",
            "lcardid": "frompoi",
            "extparam": "frompoi",
            "lfid": f"100103type=92&q={v_keyword}",
            "since_id": page,
        }

        try:
            r = requests.get(url, headers=headers, params=params)
            r.raise_for_status()
            card_group = r.json().get("data", {}).get("cards", [-1])[-1].get('card_group', [])
        except (requests.RequestException, KeyError) as e:
            print(f"Error fetching page {page}: {e}")
            card_group = []

        for card in card_group:
            try:
                weibo_data = get_weibo_data(card)
                weibo_data['页码'] = page
                collection.insert_one(weibo_data)
                print(f"插入成功: {weibo_data}")
            except Exception as e:
                print(f"处理失败: {e}")

if __name__ == '__main__':
    max_search_page = 10  # 爬前n页
    search_keywords = ['花溪区','云岩区','南明区']
    for search_keyword in search_keywords:
        get_weibo_list(v_keyword=search_keyword, v_max_page=max_search_page)

    print('数据爬取和插入完成')
