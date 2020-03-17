#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import scrapy
import datetime
import random
from w3lib.html import remove_tags
from datadig.items import Job
from datadig.utils import getCity
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived, RequestGenerationFailed


prefix = 'https://search.51job.com/list/'
middle = '0000,000000,0000,00,9,99,%2B,2,'
suffix = '.html?lang=c&stype=&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&providesalary=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare='


class Job51Spider(scrapy.Spider):
    name = "job51"
    start_urls = []

    # 代表省份对应的id，python的列表元素数据类型可以不一致，选择条件时注意核对
    provinceids = list(range(10, 33))
    for digit in range(10):
        provinceids.append('0' + str(digit))

    for province in provinceids:
        for pageno in range(1, 2001):
            start_urls.append(prefix + str(province) + middle + str(pageno) + suffix)

    random.shuffle(start_urls)

    def parse(self, response):
        # 取出所有链接地址，找不到时为空列表，不进入下面的循环
        links = response.xpath(
            '//div[@class="el"]/p[starts-with(@class, "t1 ")]/span/a')

        for link in links:
            link = link.xpath('@href').extract()[0]
            yield scrapy.Request(url=link, callback=self.parse_page, errback=self.parse_errback, dont_filter=True)

    # 爬取单个链接对应的页面内容

    def parse_page(self, response):
        item = Job()

        # 得到title、city、area、salary_min、salary_max、job_company、category等信息的基准xpath
        jobHeader = response.xpath(
            '//div[@class="tHeader tHjob"]/div[@class="in"]')

        # 标题title
        try:
            item['title'] = jobHeader.xpath('div[@class="cn"]/h1/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').strip()
        except Exception as e:
            item['title'] = ''

        # 城市，区域city、area
        try:
            location = jobHeader.xpath('div[@class="cn"]/p[2]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')
            # 正常情况下，为下面的情况
            if u'-' in location:
                item['city'] = location.split('-')[0]
                item['area'] = location.split('-')[1]
            else:
                # 只包含城市名或区域名时，下面是从location中提取城市名，如果没有，则将location赋给area
                item['city'] = getCity(location)
                item['area'] = u'全区域'
        except Exception as e:
            item['city'] = ''
            item['area'] = u'全区域'

        # 薪水salary_min、salary_max
        try:
            salary = jobHeader.xpath('div[@class="cn"]/strong/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')
            if u'千/月' in salary:
                salary = salary.replace(u'千/月', '')
                item['salary_min'] = round(float(salary.split('-')[0])*1000)
                item['salary_max'] = round(float(salary.split('-')[1])*1000)
            elif u'万/月' in salary:
                salary = salary.replace(u'万/月', '')
                item['salary_min'] = round(float(salary.split('-')[0])*10000)
                item['salary_max'] = round(float(salary.split('-')[1])*10000)
            elif u'千/年' in salary:
                salary = salary.replace(u'千/年', '')
                item['salary_min'] = round(float(salary.split('-')[0])*1000/12)
                item['salary_max'] = round(float(salary.split('-')[1])*1000/12)
            elif u'万/年' in salary:
                salary = salary.replace(u'万/年', '')
                item['salary_min'] = round(
                    float(salary.split('-')[0])*10000/12)
                item['salary_max'] = round(
                    float(salary.split('-')[1])*10000/12)
            elif u'元/天' in salary:
                salary = salary.replace(u'元/天', '')
                item['salary_min'] = round(float(salary)*23)
                item['salary_max'] = round(float(salary)*23)
            elif u'万以下/年' in salary:
                salary = salary.replace(u'万以下/年', '')
                item['salary_min'] = round(float(salary)*10000/12)
                item['salary_max'] = round(float(salary)*10000/12)
            elif u'千以下/月' in salary:
                salary = salary.replace(u'千以下/月', '')
                item['salary_min'] = round(float(salary)*1000)
                item['salary_max'] = round(float(salary)*1000)
            else:
                item['salary_min'] = 0
                item['salary_max'] = 0
        except Exception as e:
            item['salary_min'] = 0
            item['salary_max'] = 0

        # 公司名称job_company
        try:
            item['job_company'] = jobHeader.xpath('div[@class="cn"]/p[@class="cname"]/a/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')
        except Exception as e:
            item['job_company'] = ''

        # 行业类别category
        try:
            item['category'] = response.xpath('//div[@class="com_tag"]/p[3]/@title').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', ' ').replace('\u3000', ' ').replace('/', ',')\
                .replace('(', ',').replace(')', '').replace('、', ',').replace('，', ',').replace(' ', '')
        except Exception as e:
            item['category'] = ''


        try:
            for i in jobHeader.xpath('div[@class="cn"]/p[2]/text()').extract():
                if '经验' in i:
                    # 工作经验experience
                    item['experience'] = i.replace(u'无工作经验', u'无要求').replace('\r', '').replace('\n', '')\
                        .replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')
                elif '本科' in i or '博士' in i or '硕士' in i or '大专' in i or '中专' in i or '初中' in i or '高中' in i:
                    # 学历要求education
                    item['education'] = i.replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '')\
                        .replace('\xa0', '').replace('\u3000', '')

            if not item['experience']:
                item['experience'] = u'无要求'

            if not item['education']:
                item['education'] = u'无要求'

        except Exception as e:
            item['experience'] = u'无要求'
            item['education'] = u'无要求'

        # 发布日期date
        item['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

        # 工作性质catalog
        item['catalog'] = u'全职'

        # 对应网站的链接job_url
        item['job_url'] = response.url

        # 来源网站from_site
        item['from_site'] = u'www.51job.com-前程无忧'

        # 工作描述job_desc
        try:
            jobDescription = response.xpath(
                '//div[@class="tCompany_main"]/div[@class="tBorderTop_box"][1]').extract()[0].replace('<br>', '\n')
            item['job_desc'] = remove_tags(jobDescription).replace('\r', '').replace('\t', '').replace('\xa0', ' ')\
                .replace('\u3000', ' ').replace('\n', '<br>').replace(u'<br><br><br><br>微信分享<br><br><br><br>', '').replace('<br><br>', '')
        except Exception as e:
            item['job_desc'] = ''

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
