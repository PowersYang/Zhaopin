#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived, RequestGenerationFailed

from datadig.items import Job


class ScrcSpider(scrapy.Spider):
    name = "scrc"
    start_urls = ['https://www.scrc168.com/Jobs/jobs_list/citycategory/sichuan.htm']

    # 取出所有城市
    def parse(self, response):
        links = response.xpath('/html/body/div[4]/div[2]/div[@class="li "]/@onclick').extract()
        links = [link.replace('javascript:location.href=\'', 'https://www.scrc168.com/')[:-1] for link in links]

        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_area, errback=self.parse_errback, dont_filter=True)

    # 解析每个区县
    def parse_area(self, response):
        links = response.xpath('/html/body/div[4]/div[2]/div[@class="li "]/@onclick').extract()
        links = [link.replace('javascript:location.href=\'', 'https://www.scrc168.com/')[:-1] for link in links]

        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_joburls, errback=self.parse_errback, dont_filter=True)

    def parse_joburls(self, response):
        # 首先获取当前页面的职位列表
        job_urls = response.xpath('//div[@class="td-j-name"]/a/@href').extract()
        job_urls = ['https://www.scrc168.com/' + url for url in job_urls]
        for url in job_urls:
            yield scrapy.Request(url=url, callback=self.parse_data, errback=self.parse_errback, dont_filter=True)

        # 按照分页提取，只在第一页使用分页
        try:
            current_page = response.xpath('/html/body/div[5]/div[1]/div[2]/div[22]/span/text()').extract()[0]
            if current_page == '1':
                last_page_url = response.xpath('/html/body/div[5]/div[1]/div[2]/div[22]/a[7]/@href').extract()[0]
            last_page = int(last_page_url.split('&')[-1].replace('page=', ''))

            for i in range(2, last_page + 1):
                next_page = last_page_url[:last_page_url.rfind('=')] + '=' + str(i)
                next_page = response.url + next_page
                yield scrapy.Request(url=next_page, callback=self.parse_joburls, errback=self.parse_errback,
                                     dont_filter=True)
        except IndexError:
            pass

    def parse_data(self, response):
        item = Job()
        item['province'] = '四川'
        item['area'] = ''
        item['job_url'] = response.url
        item['from_site'] = 'www.scrc168.com-四川人才网'
        item['phone_num'] = ''

        try:
            item['title'] = response.xpath('/html/body/div[3]/div/div/div[2]/div[1]/text()').extract()[0]
        except:
            item['title'] = ''

        try:
            salary = response.xpath('/html/body/div[3]/div/div/div[3]/text()').extract()[0]
            if salary == '面议':
                item['salary_min'] = '面议'
                item['salary_max'] = '面议'
            elif '/月' in salary:
                # 10K-15K/月
                salary_arr = salary.replace('K', '').replace('/月', '').split('-')
                item['salary_min'] = int(salary_arr[0]) * 1000
                item['salary_max'] = int(salary_arr[1]) * 1000
            else:
                item['salary_min'] = ''
                item['salary_max'] = ''
        except:
            item['salary_min'] = ''
            item['salary_max'] = ''

        try:
            item['city'] = response.xpath('/html/body/div[4]/div[2]/div[1]/div[6]/text()').extract()[0].split('/')[-1]
        except:
            item['city'] = ''

        try:
            item['catalog'] = response.xpath('/html/body/div[4]/div[1]/div[1]/div[1]/div[2]/text()').extract()[0]
        except:
            item['catalog'] = ''

        try:
            item['category'] = response.xpath('/html/body/div[4]/div[2]/div[1]/div[4]/text()').extract()[0]
        except:
            item['category'] = ''

        try:
            item['education'] = response.xpath('/html/body/div[4]/div[1]/div[1]/div[1]/div[6]/text()').extract()[0]
        except:
            item['education'] = ''

        try:
            job_desc = response.xpath('/html/body/div[4]/div[1]/div[1]/div[3]/div[2]/text()').extract()
            item['job_desc'] = '，'.join(job_desc)
        except:
            item['job_desc'] = ''

        try:
            item['job_company'] = response.xpath('/html/body/div[4]/div[2]/div[1]/div[2]/a/text()').extract()[0]
        except:
            item['job_company'] = ''

        try:
            item['date'] = response.xpath('/html/body/div[3]/div/div/div[1]/div[1]/text()').extract()[0]
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
