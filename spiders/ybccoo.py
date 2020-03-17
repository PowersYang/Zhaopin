#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived, RequestGenerationFailed

from datadig.items import Job

"""
宜宾信息港招聘
"""


class YbccooSpider(scrapy.Spider):
    name = "ybccoo"
    start_urls = ['http://www.yibin.ccoo.cn/post/zhaopin/']

    def parse(self, response):
        links = response.xpath('//a[@class="title"]/@href').extract()
        links = ['http://www.yibin.ccoo.cn' + link for link in links]

        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_data, errback=self.parse_errback, dont_filter=True)

        # 在第一页开始分页
        current_page = response.xpath('//*[@id="page_x"]/span/text()').extract()[0]
        if int(current_page) == 1:
            last_page = response.xpath('//*[@id="page_x"]/a[10]/@href').extract()[0]
            last_page = last_page.replace('/post/zhaopin/pn', '').replace('/', '')
            last_page = int(last_page)

            for i in range(2, last_page + 1):
                link = 'http://www.yibin.ccoo.cn/post/zhaopin/pn' + str(i) + '/'
                yield scrapy.Request(url=link, callback=self.parse, errback=self.parse_errback, dont_filter=True)

    def parse_data(self, response):
        item = Job()
        item['province'] = '四川'
        item['city'] = '宜宾'
        item['area'] = ''
        item['job_url'] = response.url
        item['from_site'] = 'www.yibin.ccoo.cn-宜宾信息港'
        item['phone_num'] = ''
        item['catalog'] = ''

        try:
            item['title'] = response.xpath('//span[@class="tit tab fl"]/text()').extract()[0]
        except:
            item['title'] = ''

        try:
            salary = response.xpath('//p[@class="price"]/text()').extract()[0]
            salary = salary.strip()
            if salary == '面议':
                item['salary_min'] = '面议'
                item['salary_max'] = '面议'
            elif '-' in salary:
                # 3000-5000
                salary_arr = salary.split('-')
                item['salary_min'] = int(salary_arr[0])
                item['salary_max'] = int(salary_arr[1])
            else:
                item['salary_min'] = ''
                item['salary_max'] = ''
        except:
            item['salary_min'] = ''
            item['salary_max'] = ''

        try:
            category = response.xpath('//ul[@class="items"]/li[1]/span/text()').extract()[0]
            item['category'] = category.replace('公司行业：', '')
        except:
            item['category'] = ''

        try:
            item['education'] = response.xpath('//*[@id="baseInfo"]/ul/li[2]/span[2]/text()').extract()[0]
        except:
            item['education'] = ''

        try:
            job_desc = response.xpath('//div[@class="fc-editBox"]/text()').extract()
            item['job_desc'] = '，'.join(job_desc)
        except:
            item['job_desc'] = ''

        try:
            item['job_company'] = response.xpath('//div[@class="conBox zComBox"]/p/text()').extract()[0]
        except:
            item['job_company'] = ''

        try:
            item['date'] = response.xpath('//div[@class="tabs1 fl"]/span/text()').extract()[0]
            item['date'] = item['date'][5:]
        except:
            item['date'] = ''

        yield item

    # 处理异常的回调函数
    def parse_errback(self, failure):
        request = failure.request

        if failure.check(HttpError):
            try:
                response = failure.value.response
                self.logger.error('errback <%s> HttpError %s , response status:%s' % (
                    request.url, failure.value, response_status_message(response.status)))
            except:
                print('httperror')
        elif failure.check(TCPTimedOutError, TimeoutError):
            self.logger.error('errback <%s> TimeoutError' % request.url)
        elif failure.check(ConnectError):
            self.logger.error('errback <%s> ConnectError' % request.url)
        elif failure.check(ConnectionRefusedError):
            self.logger.error(
                'errback <%s> ConnectionRefusedError' % request.url)
        elif failure.check(DNSLookupError):
            self.logger.error(
                'errback <%s> DNSLookupError' % request.url)
        elif failure.check(ResponseFailed):
            self.logger.error('errback <%s> ResponseFailed' % request.url)
        elif failure.check(ResponseNeverReceived):
            self.logger.error(
                'errback <%s> ResponseNeverReceived' % request.url)
        elif failure.check(RequestGenerationFailed):
            # 如果请求方法为get，则request.body返回NoneType；请求方法为post则request.body会返回post的参数
            # self.logger.error('errback <%s> <post data: %s> RequestGenerationFailed' % (request.url, failure.request.body))
            self.logger.error(
                'errback <%s> RequestGenerationFailed' % (request.url))
        else:
            self.logger.error('errback <%s> OtherError' % request.url)
