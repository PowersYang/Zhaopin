# -*- coding: utf-8 -*-

import scrapy
import datetime
import time
import re
from w3lib.html import remove_tags
from datadig.items import Company
from datadig.utils import getCity
from datadig.utils import strip_dashes

class  ChinahrCompanySpider(scrapy.Spider):    
    name = "chinahrCompany"
    # 由于city决定了工作的地址，所以将其扩散至全国，下面通过省份对应city值进行构造完整的start_urls列表
    prefix = 'http://www.chinahr.com/sou/?orderField=relate&city='
    suffix = '&page=1'
    start_urls = []

    # 下面为各省份对应的city值，经过比较发现省份下面的城市所包含的工作都在对应省份下的工作范围中，因此只取省份即可，避免大量重复
    # 其范围就是11-45，共35个省市自治区、直辖市
    cityId = ['18','44','34','37','19','31','25','38','28','26','45','11','22','15','23','24','14'
    ,'16','20','13','39','41','32','21','30','12','36','27','33','35','40','43','42','29','17']

    for city in cityId:
        start_urls.append(prefix + str(city) + suffix)
    
    def parse(self, response):
        item = Company()

        # 得到初始页面的基准xpath(某一页)，只到ul即可， 因为还有一个字段需要以其为基准
        pages = response.xpath('//div[@class="resultList"]/div[@class="jobList"]/ul')
        
        # 循环取出每一页上的每一个链接url地址，并调用parse_page函数解析每一个url上的页面内容
        for eachPage in pages:
            # 由于公司规模信息无法从公司页面上爬取，所以直接从refer页上提取
            # 下面提取company_size字段
            # 下面获得上述字段的基准xpath
            company_base = eachPage.xpath('li[@class="l2"]/span[@class="e3"]')
            # 下面获得字段的全部文本内容
            company_base = company_base.xpath('string(.)').extract()[0].replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
            
            # 公司人数company_size
            try:
                item['company_size'] = company_base.split('|')[2]
            except Exception as e:
                item['company_size'] = ''            

            # 获取链接URL（页面上所有的链接，每个链接单独处理）
            singleUrl = eachPage.xpath('li[@class="l1"]/span[@class="e3 cutWord"]/a/@href').extract()[0]
            # 内部调用parse_page函数
            yield scrapy.Request(url = singleUrl,meta={'item':item},callback=self.parse_page)
              
        # 除最后一页之外，div[@class=""pageList]中的a标签包含   下一页
        try:
            tag = response.xpath('//div[@class="pageList"]/a')
            if u'下一页' in tag.xpath('string(.)').extract():
                rindex = response.url.rfind('=')
                nextPage = response.url[:rindex] + '=' + str(int(response.url[rindex + 1:]) + 1)
                yield scrapy.Request(url=nextPage, callback=self.parse)
            else:
                pass
        except Exception as e:
            pass

    # 爬取单个链接对应的页面内容
    def parse_page(self, response):
        # 通过meta得到item
        item = response.meta['item']

        # 公司行业compnay_category
        try:
            item['compnay_category'] = response.xpath('//div[@class="wrap-mc"]/em[2]/text()').extract()[0]\
            .replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('/',',').replace('\xa0','').replace('\u3000','')
        except Exception as e:
            item['compnay_category'] = ''

        # 公司类型company_type
        try:
            item['company_type'] = response.xpath('//div[@class="wrap-mc"]/em[3]/text()').extract()[0]\
            .replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('/',',').replace('\xa0','').replace('\u3000','')
        except Exception as e:
            item['company_type'] = ''

        # 公司名称name
        try:
            item['name'] = response.xpath('//div[@class="mc-company"]/div[@class="wrap-til"]/h1/text()').extract()[0]\
            .replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('/',',').replace('\xa0','').replace('\u3000','')
        except Exception as e:
            item['name'] = ''

        # 公司地址address，由于之前提取公司地址不够详细，所以换了方式
        try:
            address = response.xpath('//div[@class="address-company clear"]/div[@class="address"]')
            address = address.xpath('string(.)').extract()[0]
            # 标准的为公司地址：   为了防止：  中英文符号混淆，因此截取字符串时加上  公司地址：的长度
            # 下面获取phone_num时可以复用address
            index = address.rfind(u'公司地址')
            item['address'] = address[index + 5:]
            item['address'] = item['address'].replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
        except Exception as e:
            item['address'] = ''

        # 公司所属城市company_city从address中提取
        if item['address'] != '':
            item['company_city'] = getCity(item['address'])
            # 由于address中较大比例1/8未提供城市名，所以继续提取
            if item['company_city'] == '':
                try:
                    location = response.xpath('//div[@class="wrap-mc"]/em[1]/text()').extract()[0]
                    item['company_city'] = getCity(location)
                except Exception as e:
                    pass
        else:
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

        # 公司介绍company_desc
        try:
            companyDescription = response.xpath('//div[@class="art-company"]/div[@class="article"]').extract()[0].replace('<br>','\n')
            item['company_desc'] = remove_tags(companyDescription)
            item['company_desc'] = item['company_desc'].replace('\r','').replace('\t','').replace('\xa0',' ').replace('\u3000',' ').replace('\n','<br>')
            item['company_desc'] = '<br>' + item['company_desc'] + '<br>'
        except Exception as e:
            item['company_desc'] = ''
        
        # 联系方式从address、company_desc中提取即可phone_num
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
            result = re.findall(reg,address)
            desc_result = re.findall(reg,item['company_desc'])
        except Exception as e:
            pass
        
        # 分别从address和company_desc中提取联系方式
        try:
            for tuple in result:
                for ele in tuple:
                    if len(ele) in (11,12,13):
                        item['phone_num'] = item['phone_num'] + ele + ','

            for desc_tuple in desc_result:
                for desc_ele in desc_tuple:
                    if len(desc_ele) in (11,12,13):
                        item['phone_num'] = item['phone_num'] + desc_ele + ','
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

        time.sleep(0.3)

        yield item
    