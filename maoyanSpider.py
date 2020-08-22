# -*- coding: UTF-8 -*-
# !/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2019-10-23 09:09:23
# Project: maoyanSpider

from pyspider.libs.base_handler import *
import json
from pyspider.result import ResultWorker
import pymysql
import re
from io import BytesIO
from fontTools.ttLib import TTFont
import base64
from scrapy import Selector
import requests
import datetime
import time


class Handler(BaseHandler):
    crawl_config = {
    }

    # 连接数据库
    def __init__(self):
        self.db = pymysql.connect('localhost', 'root', 'hujing', 'resultdb', charset='utf8')

    def add_Mysql(self, sql):
        try:
            cursor = self.db.cursor()
            print(sql)
            cursor.execute(sql)
            print(cursor.lastrowid)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

    def dateRange(self, start, end, step=1, format="%Y%m%d"):
        strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
        days = (strptime(end, format) - strptime(start, format)).days
        return [strftime(strptime(start, format) + datetime.timedelta(i), format) for i in range(0, days, step)]

    @every(seconds=2 * 60)
    def on_start(self):
        self.crawl('http://pf.maoyan.com/dashboard', callback=self.index_page)

    @config(age=60)
    def index_page(self, response):
        day_list = self.dateRange("20191001", time.strftime("%Y%m%d"))
        for day in day_list:
            self.crawl('http://pf.maoyan.com/second-box?beginDate={}'.format(day), callback=self.detail_page)

    @config(age=60)
    def detail_page(self, response):
        createtime = response.json['data']['queryDate']  # 票房日期
        for each in response.json['data']['list']:
            movieId = each['movieId']  # 电影id
            movieName = each['movieName']  # 电影名称
            releaseInfo = each['releaseInfo']  # 上映天数
            sumBoxInfo_str = each['sumBoxInfo']  # 总票房
            boxInfo = each['boxInfo']  # 综合票房
            boxRate = each['boxRate']  # 综合票房占比
            avgSeatView = each['avgSeatView']  # 上座率
            avgShowView = each['avgShowView']  # 场均人次
            showInfo = each['showInfo']  # 排片场次
            showRate = each['showRate']  # 排片占比

            # 正则表达式提取数字和中文字符
            p1 = re.compile(r'[\u4E00-\u9FA5]')
            p2 = re.compile(r'\d+(\.\d+)?')
            ch1 = p1.search(sumBoxInfo_str).group
            sumBoxInfo = float(p2.search(sumBoxInfo_str).group())
            if ch1 == "亿":
                sumBoxInfo = sumBoxInfo * 10000

            sql = 'insert into maoyanday(movieId, createtime, movieName, releaseInfo, boxInfo,sumBoxInfo, boxRate, showInfo, showRate, avgShowView, avgSeatView) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' % (
            movieId, createtime, movieName, releaseInfo, boxInfo, sumBoxInfo, boxRate, showInfo, showRate, avgShowView,
            avgSeatView);
            self.add_Mysql(sql)

            # 电影详细信息url
            movie_url = "http://pf.maoyan.com/movie/{}?_v_=yes".format(movieId)
            self.crawl(movie_url, callback=self.movie_page, fetch_type='js', save={'movieId': movieId})

            # 导演、演员url
            celebrity_url = "http://pf.maoyan.com/movie/{}/celebritylist".format(movieId)
            self.crawl(celebrity_url, callback=self.celebrity_page, save={'movieId': movieId})

    @config(age=60)
    def movie_page(self, response):

        # 获取评分
        score = response.doc('span[class="rating-num"]').eq(0).text()
        if not score:
            score = 0

        # 评分人数
        score_num_str = response.doc('p[class="detail-score-count"]').text()
        p1 = re.compile(r'[\u4E00-\u9FA5]')
        p2 = re.compile(r'\d+(\.\d+)?')
        if score_num_str:
            ch1 = p1.search(score_num_str).group()
            num1 = float(p2.search(score_num_str).group())
            if ch1 != "万":
                score_num = num1 / 10000
            else:
                score_num = num1
        else:
            score_num = 0

            # 想看人数
        wish_num_str1 = response.doc('p[class="detail-wish-count"]').text()
        wish_num_str2 = response.doc('div[class="block-wish-item left"]').text()
        if wish_num_str1:
            ch2 = p1.search(wish_num_str1).group()
            num2 = float(p2.search(wish_num_str1).group())
            if ch2 != "万":
                wish_num = num2 / 10000
            else:
                wish_num = num2
        elif wish_num_str2:
            ch2 = p1.search(wish_num_str2).group()
            num2 = float(p2.search(wish_num_str2).group())
            if ch2 != "万":
                wish_num = num2 / 10000
            else:
                wish_num = num2
        else:
            wish_num = 0

        # 获取地区
        region_1 = response.doc('div[class="info-source-duration"]')
        region_2 = region_1('div > p').text()  # 中国大陆 / 155分钟
        region = region_2.split(" ")[0]

        # 获取介绍
        description = response.doc('div[class="detail-block-content"]').text()

        sql = 'update maoyanday set score="%s",score_num="%s", wish_num="%s", region="%s", description="%s" where movieId = "%s";' % (
        score, score_num, wish_num, region, description, response.save['movieId'])
        self.add_Mysql(sql)

    @config(age=60)
    def celebrity_page(self, response):
        # 获取导演
        director_list = []
        director_all = response.doc('div[class="panel-c"]').eq(0)
        for each in director_all.items('div[class=p-item]'):
            director = each('a > div.p-desc > p.p-item-name.ellipsis-1').text()
            director_list.append(director)
        directors = ','.join(director_list)

        # 获取演员
        actor_list = []
        actor_all = response.doc('div[class="panel-c"]').eq(1)
        for each in actor_all.items('div[class=p-item]'):
            actor = each('a > div.p-desc > p.p-item-name.ellipsis-1').text()
            actor_list.append(actor)
        actors = ','.join(actor_list)

        sql = 'update maoyanday set directors="%s", actors="%s" where movieId = "%s";' % (
        directors, actors, response.save['movieId'])
        self.add_Mysql(sql)



