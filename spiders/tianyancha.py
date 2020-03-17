#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import scrapy
import random
import datetime
import time
from datadig.items import Job
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived, RequestGenerationFailed


# 定义天眼查招聘的前缀、后缀字符串
prefix = 'https://job.tianyancha.com/search/'
suffix = '/p'
# 定义查询的关键字
keyword = []
keyword.extend(list('abcdefghijklmnopqrstuvwxyz0123456789'))
keyword.extend(['c', 'c++', 'java', 'python', 'php', 'js', u'数据库', u'数据', u'软件', u'开发', u'测试', u'框架', u'设计', u'云计算', u'计算机', u'网络'])
random.shuffle(keyword)


class TianyanchaSpider(scrapy.Spider):
    name = 'tianyancha'
    start_urls = ['https://job.tianyancha.com/']

    def parse(self, response):
        for kw in keyword:
            for page in range(1, 501):
                link = prefix + kw + suffix + str(page)
                if page == 1:
                    refererlink = link
                else:
                    refererlink = prefix + kw + suffix + str(page - 1)
                yield scrapy.Request(url=link, headers={'Referer': refererlink}, callback=self.parse_page, errback=self.parse_errback, dont_filter=True)


    # 解析搜索页面，并获取具体页面的地址或直接拿到item对应的字段
    def parse_page(self, response):
        links = response.xpath('//div[@class="filter_risk"]')

        for link in links:
            # 获取招聘状态，处于停招状态的数据则过滤
            recruit_status = link.xpath('div[@class="risk-title"]/span[1]/span/text()').extract()[0]

            if recruit_status == u'（在招）':
                item = Job()

                # title
                try:
                    item['title'] = link.xpath('div[@class="risk-title"]/span[1]/a/text()|div[@class="risk-title"]/span[1]/text()').extract()[0]
                except:
                    item['title'] = ''

                # salary_min,salary_max
                try:
                    salary = link.xpath(
                        'div[@class="risk-title"]/span[2]/text()').extract()[0].replace(u'元', '')
                    item['salary_min'] = salary.split('-')[0]
                    if u'K' in item['salary_min']:
                        item['salary_min'] = int(item['salary_min'].replace(u'K', '')) * 1000

                    item['salary_max'] = salary.split('-')[1]
                    if u'K' in item['salary_max']:
                        item['salary_max'] = int(item['salary_max'].replace(u'K', '')) * 1000
                except:
                    item['salary_min'] = 0
                    item['salary_max'] = 0

                # job_company
                try:
                    item['job_company'] = link.xpath('div[2]/div[2]/span/text()').extract()[0]
                except:
                    item['job_company'] = ''

                # city
                try:
                    item['city'] = link.xpath('div[3]/div[1]/span/text()').extract()[0]
                except:
                    item['city'] = ''

                # experience
                try:
                    item['experience'] = link.xpath('div[3]/div[2]/span/text()').extract()[0]
                    if u'不限' == item['experience']:
                        item['experience'] = u'无要求'
                except:
                    item['experience'] = ''

                # catalog
                item['catalog'] = u'全职'

                # category
                item['category'] = u'其他行业'

                # date
                item['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

                # from_site
                item['from_site'] = u'job.tianyancha.com-天眼查招聘'

                # 获取职位详情页面URL地址
                try:
                    detailurl = link.xpath('a/@href').extract()[0]
                    yield scrapy.Request(url=detailurl, meta={'item': item}, callback=self.parse_detail, errback=self.parse_errback, dont_filter=True)
                except:
                    # 异常情况即职位信息只能预览的情况，即职位信息是弹出框的展现方式，取得具体内容并yield
                    detail_str = link.xpath('script/text()').extract()[0].replace('null', 'None').replace('true', 'True').replace('false', 'False')
                    detail_dict = eval(detail_str)

                    # area
                    try:
                        item['area'] = detail_dict['district']
                        if u'' == item['area'] or u'不限' in item['area']:
                            item['area'] = u'全区域'
                    except:
                        item['area'] = ''

                    # education
                    try:
                        item['education'] = detail_dict['education']
                        if u'不限' == item['education']:
                            item['education'] = u'无要求'
                    except:
                        item['education'] = ''

                    # job_desc
                    try:
                        item['job_desc'] = detail_dict['description']
                    except:
                        item['job_desc'] = ''

                    # job_url
                    # 该职位信息没有具体的详情页面，因此使用引用页作为职位信息页
                    item['job_url'] = response.url

                    time.sleep(random.uniform(1, 5))

                    yield item
            else:
                # 处于停招状态的职位信息
                pass


    def parse_detail(self, response):
        item = response.meta['item']

        # area
        try:
            item['area'] = response.xpath('//span[@class="area"]/text()').extract()[0].split('-')[1]
            if u'' == item['area'] or u'不限' in item['area']:
                item['area'] = u'全区域'
        except:
            item['area'] = u'全区域'

        # education
        try:
            item['education'] = response.xpath('//span[@class="education"]/text()').extract()[0]
            if u'不限' == item['education']:
                item['education'] = u'无要求'
        except:
            item['education'] = ''

        # job_desc
        try:
            item['job_desc'] = response.xpath('//div[@class="job"]/div[@class="content"]/text()').extract()[0]
        except:
            item['job_desc'] = ''

        # job_url
        item['job_url'] = response.url

        time.sleep(random.uniform(1, 5))

        yield item


    # 处理异常的回调函数
    def parse_errback(self, failure):
        request = failure.request

        if failure.check(HttpError):
            try:
                response = failure.value.response
                self.logger.error('errback <%s> HttpError %s , response status:%s' % (request.url, failure.value, response_status_message(response.status)))
            except:
                print('httperror')
        elif failure.check(TCPTimedOutError, TimeoutError):
            self.logger.error('errback <%s> TimeoutError' % request.url)
        elif failure.check(ConnectError):
            self.logger.error('errback <%s> ConnectError' % request.url)
        elif failure.check(ConnectionRefusedError):
            self.logger.error('errback <%s> ConnectionRefusedError' % request.url)
        elif failure.check(DNSLookupError):
            self.logger.error('errback <%s> DNSLookupError' % request.url)
        elif failure.check(ResponseFailed):
            self.logger.error('errback <%s> ResponseFailed' % request.url)
        elif failure.check(ResponseNeverReceived):
            self.logger.error('errback <%s> ResponseNeverReceived' % request.url)
        elif failure.check(RequestGenerationFailed):
            # 如果请求方法为get，则request.body返回NoneType；请求方法为post则request.body会返回post的参数
            # self.logger.error('errback <%s> <post data: %s> RequestGenerationFailed' % (request.url, failure.request.body))
            self.logger.error('errback <%s> RequestGenerationFailed' % (request.url))
        else:
            self.logger.error('errback <%s> OtherError' % request.url)
