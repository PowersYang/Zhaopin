# -*- coding: utf-8 -*-

import scrapy
import datetime
import time
import re
from w3lib.html import remove_tags
from datadig.items import Company
from datadig.utils import getCity
from datadig.utils import strip_dashes

# 定义全局，防止parse方法重复循环，parse中pageIndex用global声明
pageIndex = 2

class  Job51CompanySpider(scrapy.Spider):    
    name = "job51Company"
    start_urls = ['https://search.51job.com/jobsearch/search_result.php?fromJs=1&keywordtype=2&lang=c&stype=2&postchannel=0000&fromType=1&confirmdate=9']
        
    def parse(self, response):
        global pageIndex

        item = Company()
        # 得到初始页面的基准xpath(某一页)
        pages = response.xpath('//div[@class="el"]/span[@class="t2"]/a')
        
        # 循环取出每一页上的每一个链接url地址，并调用parse_page函数解析每一个url上的页面内容
        for eachPage in pages:          
            # 获取链接URL（页面上所有的链接，每个链接单独处理）
            singleUrl = eachPage.xpath('@href').extract()[0]
            # 内部调用parse_page函数
            yield scrapy.Request(url = singleUrl,meta={'item':item},callback=self.parse_page)
        
        # 拼凑页面，实现翻页
        prefix = 'https://search.51job.com/list/000000,000000,0000,00,9,99,%2B,2,'
        suffix = '.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=1&dibiaoid=0&address=&line=&specialarea=00&from=&welfare='
        
        # 循环执行，注意pageIndex需为global，否则循环会重复，注意逻辑的理解
        while pageIndex < 2001:
            nextPage = prefix + str(pageIndex) + suffix
            pageIndex += 1
            yield scrapy.Request(url=nextPage, callback=self.parse)     

    # 爬取单个链接对应的页面内容
    def parse_page(self, response):
        # 通过meta得到item
        item = response.meta['item']

        # 得到name、company_type、company_size、compnay_category等信息的基准xpath
        companyHeader = response.xpath('//div[@class="tHeader tHCop"]')

        # 公司名称name
        try:
            item['name'] = companyHeader.xpath('div/h1/text()').extract()[0]\
            .replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
        except Exception as e:
            item['name'] = ''
        
        # 公司类型、公司人数、公司行业
        try:
            company_info = companyHeader.xpath('div/p[@class="ltype"]/@title').extract()[0]\
            .replace('/',',').replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
        except Exception as e:
            company_info = ''
        
        # 公司类型company_type
        try:
            item['company_type'] = company_info.split('|')[0].replace('(',',').replace(')','').replace(u'（',',').replace(u'）','').replace(u'、',',')
        except Exception as e:
            item['company_type'] = ''
        
        # 公司人数company_size
        try:
            item['company_size'] = company_info.split('|')[1]
        except Exception as e:
            item['company_size'] = ''

        # 公司行业compnay_category，出现异常情况，(人数、行业或类型、行业或一个字段)注意下面的异常代码
        try:
            item['compnay_category'] = company_info.split('|')[2].replace('(',',').replace(')','').replace(u'（',',').replace(u'）','').replace(u'、',',')
        except Exception as e:
            item['compnay_category'] = ''            
        
        # 公司描述company_desc
        try:
            company_desc = response.xpath('//div[@class="tmsg inbox"]').extract()[0].replace('<br>','\n')
            item['company_desc'] = remove_tags(company_desc)
            item['company_desc'] = item['company_desc'].replace('\r','').replace('\t','').replace('\xa0',' ').replace('\u3000',' ') \
            .replace('\n','<br>').replace(u'展开全部<br><br><br>屏蔽该公司','').replace('<br><br><br><br>','<br>')          
        except Exception as e:
            item['company_desc'] = ''

        # 联系方式从company_desc中提取即可phone_num
        mobilePhone = r'(1[38]\d{9})|(14[5-8]\d{8})|(15[0-35-9]\d{8})|(16[56]\d{8})|(17[0-8]\d{8})|(19[89]\d{8})'
        fixedPhone = r'''((010|02[0-57-9])-\d{8})|((03(1[2-90]|7[2-608]|5\d|35|9[1-68]))-\d{7})|((04([15][2-90]|7\d|2[79]|8[23]|3[3-9]|40))-\d{7})
        |((05(3[3-90]|5[2-90]|7[028]|6[1-6]|80|9[2-46-9]))-\d{7})|((06(6[0-3]|9[12]))-\d{7})|((07([17]\d|3[2-90]|5[0-3689]|01|2[248]|4[3-6]|6[2368]|9[2-90]))-\d{7})
        |((08([13]\d|2[5-7]|40|5[4-92]|8[13678]|9[0-39]|7[2-90]))-\d{7})|((09(0[1-3689]|1[0-79]|3[0-8]|4[13]|5[1-5]|7[1-7]|9\d))-\d{7})
        |((03(11|7[179]))-\d{8})|((04([15]1|3[12]))-\d{8})|((05(1\d|2[37]|3[12]|51|7[3-719]|9[15]))-\d{8})|((07([39]1|5[457]|6[09]))-\d{8})|((08(5[13]|98|71))-\d{8})'''
        reg = mobilePhone + r'|' + fixedPhone
        # 初始为空
        item['phone_num'] = ''

        # findall方法返回的是一个list，并且其中的元素又是tuple，因此再取tuple中的元素值，然后做长度判断
        # 极少情况下出现异常(当公司页面不是chinahr制定的公司主页模板时)
        try:
            result = re.findall(reg,item['company_desc'])
        except Exception as e:
            pass
        
        # 从company_desc中提取联系方式
        try:
            for tuple in result:
                for ele in tuple:
                    if len(ele) in (11,12,13):
                        item['phone_num'] = item['phone_num'] + ele + ','
        except Exception as e:
            pass
        
        # 拿到字符串形式的数据如：021-11111111，,12345678910，去除重复元素
        if ',' in item['phone_num']:
            # 去除最后一个,
            item['phone_num'] = item['phone_num'][:-1]
            # 定义空列表
            elements = []
            for e in item['phone_num'].split(','):
                elements.append(e)

            # 去除重复元素
            elements = list(set(elements))
            item['phone_num'] = str(elements).replace("'","").replace('[','').replace(']','').replace(' ','')
        
        # 联系地址address
        try:
            address = response.xpath('//div[@class="tBorderTop_box bmsg"]/div/p')
            address = address.xpath('string(.)').extract()[0]
            item['address'] = address.replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')[5:]
        except Exception as e:
            item['address'] = ''
        
        # 城市名称，直接从公司名称、联系地址中提取，由于在refer中有些网页有错误，致使城市名称对不上号
        try:
            item['company_city'] = getCity(item['name'])
            if item['company_city'] == '':
                item['company_city'] = getCity(item['address'])
        except Exception as e:
            item['company_city'] = ''

        # 公司所属省份company_province从company_city中获得
        if item['company_city'] != '':
            item['company_province'] = strip_dashes(item['company_city'])
        else:
            item['company_province'] = ''
        
        # 对应招聘网站上的公司主页company_url
        item['company_url'] = response.url

        # 发布日期date
        today = datetime.datetime.now()
        item['date'] = today.strftime('%Y-%m-%d')       

        yield item
    