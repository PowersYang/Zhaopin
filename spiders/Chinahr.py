#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import scrapy
import datetime
import re
import requests
import math
import time
import random
from w3lib.html import remove_tags
from datadig.items import Job
from datadig.utils import getCity
from lxml import etree
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived, RequestGenerationFailed


# 拼接url的前缀、后缀
prefix = 'http://www.chinahr.com/sou/?orderField=relate&city='
suffix = '&page='


# 该函数获取某城市对应下的职位数量，并用math.ceil(num/20)求得页码数量
def getpagenum(url):
    # 获得相应对象
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3'})
    # 转换结构，可使用xpath语法了
    selector = etree.HTML(response.text)

    # 获取到职位数量
    try:
        jobnum = selector.xpath(
            '//div[@class="totalResult cutWord"]/span/text()')[0]
    except:
        # 存在部分页面没有展示职位数量的情况，经验证这些页面至多4页
        return 4

    return math.ceil(int(jobnum) / 20)


class ChinahrSpider(scrapy.Spider):
    name = "chinahr"
    start_urls = []

    # 下面为各省份对应的city值，经过比较发现省份下面的城市所包含的工作都在对应省份下的工作范围中，因此只取省份即可，避免大量重复
    # 其范围就是11-45，共35个省市自治区、直辖市
    for cityid in list(range(11, 46)):
        # 求得对应city职位数量的页码数pageno
        pagenum = getpagenum(prefix + str(cityid) + suffix + '1')

        for pageid in range(1, pagenum + 1):
            start_urls.append(prefix + str(cityid) + suffix + str(pageid))

    # 请求页面乱序功能
    random.shuffle(start_urls)

    def parse(self, response):
        # 获得一个页面上的所有二级页面链接
        links = response.xpath('//div[@class="jobList"]/ul/li[1]/span[1]/a')

        # 循环取出每一页上的每一个链接url地址，并调用parse_page函数解析每一个url上的页面内容
        for link in links:
            # 获取链接URL（页面上所有的链接，每个链接单独处理）
            link = link.xpath('@href').extract()[0]
            # 内部调用parse_page函数
            yield scrapy.Request(url=link, callback=self.parse_page, errback=self.parse_errback, dont_filter=True)


    # 爬取单个链接对应的页面内容
    def parse_page(self, response):
        item = Job()

        # 得到title、salary_min、salary_max、city、area、catalog、education、experience等信息的基准xpath
        contentList = response.xpath('//div[@class="base_info"]')

        # 标题title
        try:
            item['title'] = contentList.xpath('div/h1/span[@class="job_name"]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').strip()
        except Exception as e:
            item['title'] = ''

        # 薪水，异常情况均置为0
        try:
            salary = contentList.xpath('div[@class="job_require"]/span[@class="job_price"]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace(' ', '')
            item['salary_min'] = salary.split('-')[0]
            item['salary_max'] = salary.split('-')[1]
        except Exception as e:
            item['salary_min'] = 0
            item['salary_max'] = 0

        # 城市，区域
        try:
            location = contentList.xpath('div[@class="job_require"]/span[@class="job_loc"]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '')
            # 正常情况下，为下面的情况
            if ' ' in location:
                item['city'] = location.split(' ')[0]
                item['area'] = location.split(' ')[1]
            else:
                # 只包含城市名或区域名时，下面是从location中提取城市名，如果没有，则将location赋给area
                item['city'] = getCity(location)
                item['area'] = u'全区域'
        except Exception as e:
            item['city'] = ''
            item['area'] = u'全区域'

        # 职位类别
        try:
            item['catalog'] = contentList.xpath('div[@class="job_require"]/span[3]/text()').extract()[0]\
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace(' ', '')

            if item['catalog'] == '':
                item['catalog'] = u'全职'
        except Exception as e:
            item['catalog'] = u'全职'

        # 教育经历
        try:
            item['education'] = contentList.xpath('div[@class="job_require"]/span[4]/text()').extract()[0] \
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace(' ', '')

            if item['education'] == '':
                item['education'] = u'无要求'
        except Exception as e:
            item['education'] = u'无要求'

        # 工作经验
        try:
            item['experience'] = contentList.xpath('div[@class="job_require"]/span[@class="job_exp"]/text()').extract()[0] \
                .replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace(' ', '')

            if item['experience'] == '':
                item['experience'] = u'无要求'
        except Exception as e:
            item['experience'] = u'无要求'

        # 工作描述
        try:
            jobDescription = response.xpath(
                '//div[@class="job_intro_info"]').extract()[0].replace('<br>', '\n')
            item['job_desc'] = remove_tags(jobDescription)
            item['job_desc'] = item['job_desc'].replace('\r', '').replace('\t', '').replace('\xa0', ' ').replace('\u3000', ' ').replace('\n', '<br>') \
                .replace(u'驾照：不要求', '').replace(u'查看更多', '').replace(u'职位介绍', '').replace(u'性别：不限', '').replace('<br><br>', '').replace('  ', '')
        except Exception as e:
            item['job_desc'] = ''

        # 公司名称
        try:
            item['job_company'] = response.xpath('//div[@class="job-company jrpadding"]/h4/a/text()').extract()[0]\
                .replace(' ', '').replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '')
        except Exception as e:
            item['job_company'] = ''

        # 职位网址
        item['from_site'] = u'www.chinahr.com-中华英才网'

        # 职位源网址
        item['job_url'] = response.url

        # 由于职位行业类别有可能顺序不一致，所以根据td[1]的关键字来做判断
        # 见http://www.chinahr.com/job/5805305393678594.html?searchplace=22
        # 见http://www.chinahr.com/job/7642430403546115.html?searchplace=22
        # 见http://www.chinahr.com/job/7699801923259911.html?searchplace=11
        # 职位行业
        try:
            keys = response.xpath(
                '//div[@class="job-company jrpadding"]/table/tbody/tr/td[1]/text()').extract()
            # 求出行业所在tr中的索引值，并且需要索引值+1，因为列表的索引值与xpath语法的索引不一致
            for k in range(len(keys)):
                if u'行业' in keys[k]:
                    index = k + 1
                    break

            # 取出td标签，然后取出下面的文本内容。有可能直接位于td下，也有部分位于td/a/下
            category = response.xpath(
                '//div[@class="job-company jrpadding"]/table/tbody/tr[position()=' + str(index) + ']/td[2]')
            item['category'] = category.xpath('string(.)').extract()[0].replace('/', ',').replace('\r', '').replace(
                '\n', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').strip()

        except Exception as e:
            item['category'] = ''

        # 发布日期
        item['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

        # 由于phone_num字段未使用，故注释该部分代码
        """
        mobilePhone = r'(1[38]\d{9})|(14[5-8]\d{8})|(15[0-35-9]\d{8})|(16[56]\d{8})|(17[0-8]\d{8})|(19[89]\d{8})'
        fixedPhone = r'''((010|02[0-57-9])-\d{8})|((03(1[2-90]|7[2-608]|5\d|35|9[1-68]))-\d{7})|((04([15][2-90]|7\d|2[79]|8[23]|3[3-9]|40))-\d{7})
        |((05(3[3-90]|5[2-90]|7[028]|6[1-6]|80|9[2-46-9]))-\d{7})|((06(6[0-3]|9[12]))-\d{7})|((07([17]\d|3[2-90]|5[0-3689]|01|2[248]|4[3-6]|6[2368]|9[2-90]))-\d{7})
        |((08([13]\d|2[5-7]|40|5[4-92]|8[13678]|9[0-39]|7[2-90]))-\d{7})|((09(0[1-3689]|1[0-79]|3[0-8]|4[13]|5[1-5]|7[1-7]|9\d))-\d{7})
        |((03(11|7[179]))-\d{8})|((04([15]1|3[12]))-\d{8})|((05(1\d|2[37]|3[12]|51|7[3-719]|9[15]))-\d{8})|((07([39]1|5[457]|6[09]))-\d{8})|((08(5[13]|98|71))-\d{8})'''
        reg = mobilePhone + r'|' + fixedPhone

        # 定义空列表
        phone_num = []

        # findall方法返回的是一个list，并且其中的元素又是tuple，因此再取tuple中的元素值，然后做长度判断
        result = re.findall(reg, item['job_desc'])
        for tup in result:
            for ele in tup:
                if len(ele) in (11, 12, 13):
                    phone_num.append(ele)

        # 去除重复元素
        phone_num = list(set(phone_num))
        item['phone_num'] = ','.join(phone_num)
        """

        time.sleep(random.uniform(0.3, 0.5))

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
