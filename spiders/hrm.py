#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import scrapy
import datetime
import random
from w3lib.html import remove_tags
from datadig.items import Job
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived, RequestGenerationFailed

prefix = 'http://www.hrm.cn/jobs?keyType=0&keyWord=&jobTypeId=&jobType=&industry=&industryname=&workId=&workPlace=&salary=&entType=&experience=&education=&entSize=&benefits=&reftime=&workTypeId=&sortField=&pageNo='


class HrmSpider(scrapy.Spider):
    name = 'hrm'
    start_urls = []

    for page in range(1, 301):
        start_urls.append(prefix + str(page))

    random.shuffle(start_urls)

    def parse(self, response):
        # 得到初始展示页面的基准xpath(某一页)，一页上有20条记录
        links = response.xpath('//li[@class="list_jobs_name mouseListen"]/a')

        for link in links:
            # 取出所有二级页面链接地址
            link = 'http://www.hrm.cn' + link.xpath('@href').extract()[0]
            yield scrapy.Request(url=link, callback=self.parse_page, errback=self.parse_errback, dont_filter=True)


    # 爬取单个链接对应的页面内容
    def parse_page(self, response):
        item = Job()

        # 招聘标题、工作性质、最低薪水、最高薪水、工作经验、学历要求、工作城市、工作区域、发布日期
        # 上述页面内容均处于下面的基准xpath内，均为后代节点
        jobDetail = response.xpath('//div[@class="detail_jobName clearfix"]')

        # 招聘标题
        try:
            item['title'] = jobDetail.xpath('h1/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').strip()
        except Exception as e:
            item['title'] = ''

        # 工作性质
        try:
            item['catalog'] = jobDetail.xpath('span[starts-with(@class,"job_type")]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')
            if item['catalog'] == u'全':
                item['catalog'] = u'全职'
            elif item['catalog'] == u'兼':
                item['catalog'] = u'兼职'
            elif item['catalog'] == u'实':
                item['catalog'] = u'实习'
        except Exception as e:
            item['catalog'] = ''

        # 最低薪水、最高薪水，并去掉附带的\r\n\t
        try:
            salary = jobDetail.xpath('span[@class="name_Salary"]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace(u'(可面议)', '').replace('\xa0', '').replace('\u3000', '')

            # 目前薪资分为3种情况：(1)、1-2K(2)、(面议)(3)(面议)1-2K
            if u'面议' in salary:
                if 'K' in salary:
                    # 第(3)种情况
                    salary = salary.replace(u'(面议)', '').replace(
                        'K', '').replace(' ', '')
                    item['salary_min'] = int(
                        float(salary.split('-')[0]) * 1000)
                    item['salary_max'] = int(
                        float(salary.split('-')[1]) * 1000)
                else:
                    # 第(2)种情况
                    item['salary_min'] = 0
                    item['salary_max'] = 0
            elif u'K' in salary:
                # 第(1)种情况
                salary = salary.replace('K', '').replace(' ', '')
                item['salary_min'] = int(float(salary.split('-')[0]) * 1000)
                item['salary_max'] = int(float(salary.split('-')[1]) * 1000)
            else:
                # 异常情况
                item['salary_min'] = 0
                item['salary_max'] = 0
        except Exception as e:
            item['salary_min'] = 0
            item['salary_max'] = 0

        # 工作经验
        try:
            item['experience'] = response.xpath('//div[@class="jobsInfo clearfix"]/text()').extract()[1]\
                .replace(u'经验', '').replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')

            if item['experience'] in ('', u'不限'):
                item['experience'] = u'无要求'
        except Exception as e:
            item['experience'] = u'无要求'

        # 学历要求
        try:
            item['education'] = response.xpath('//div[@class="jobsInfo clearfix"]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')

            if item['education'] == '':
                item['education'] = u'无要求'
        except Exception as e:
            item['education'] = u'无要求'

        # 工作城市、工作区域
        try:
            location = response.xpath('//div[@class="jobsInfo clearfix"]/text()').extract()[2] \
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace(' ', '').replace(',', '').replace('/', '')
            if location.count(u'市') != 0:
                item['city'] = location.split(u'市')[0]
                if (location.split(u'市')[1] == ''):
                    item['area'] = u'全区域'
                else:
                    item['area'] = location.split(u'市')[1]
            else:
                item['city'] = u'重庆'
                item['area'] = location
        except Exception as e:
            item['city'] = ''
            item['area'] = u'全区域'

        # 不包括县，因为县不用单独处理
        cqAreas = [u"万州", u"涪陵", u"渝中", u"大渡口", u"江北", u"沙坪坝", u"九龙坡",
                   u"南岸", u"北碚", u"綦江", u"大足", u"渝北", u"巴南", u"黔江", u"长寿", u"江津",
                   u"合川", u"永川", u"南川"]

        # 处理item['area']如江北区观音桥这种情况
        for ele in cqAreas:
            if ele in item['area']:
                item['area'] = ele + u'区'
                break

        # 发布时间
        item['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

        # 职位来源
        item['from_site'] = u'www.hrm.cn-联英人才网'

        # 职位源网址
        item['job_url'] = response.url

        # 职位描述，职位描述分为5个部分：职位信息、职位动态、公司福利（可选项，可能没有此项）、岗位职责、任职要求
        # 职位描述左边部分，包含上述所有内容及其他部分内容，以此作为基准xpath
        try:
            job_responsibility = response.xpath(
                '//div[@class="jobs_content"]').xpath('string(.)').extract()[0]
        except:
            job_responsibility = ''

        try:
            job_condition = response.xpath(
                '//div[@class="jobs_content"]').xpath('string(.)').extract()[1]
        except:
            job_condition = ''

        job_desc = job_responsibility + job_condition

        # 取子串
        job_desc = remove_tags(job_desc)
        item['job_desc'] = job_desc.replace('\r', '').replace('\t', '').replace('\xa0', '').replace(
            '\u3000', '').replace('\n', '<br>').replace('<br><br>', '').replace('  ', '')

        # 公司名称
        try:
            item['job_company'] = response.xpath('//div[@class="comapny_name clearfix"]/a/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '')
        except Exception as e:
            item['job_company'] = ''

        # 行业类别
        try:
            item['category'] = response.xpath(
                '//li[@class="indstName"]/text()').extract()[0]
            item['category'] = item['category'].replace(u'，', ',').replace(u'。', ',').replace(u'/模', '').replace('/', ',')\
                .replace(u'、', ',').replace(u'（', ',').replace('(', ',').replace(u'）', '').replace(')', '').replace(u'请选择,', u'其他行业')\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace(' ', '').replace('\xa0', '').replace('\u3000', '').strip(',')

            # 有逗号 ',' 才考虑去重的问题
            if ',' in item['category']:
                # 定义空列表
                elements = []
                for e in item['category'].split(','):
                    # 利用,分隔的元素值添加到列表elements中
                    elements.append(e)

                # 去除重复元素后，转换为列表
                elements = list(set(elements))

                item['category'] = ','.join(elements)

        except Exception as e:
            item['category'] = ''

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
