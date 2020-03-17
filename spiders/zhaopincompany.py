#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import scrapy,re,json,datetime
from w3lib.html import remove_tags
from datadig.items import Company
from datadig.utils import strip_dashes
# 定义全局的请求页码，范围从1~90页
pageIndex = 1

'''
代码的实现原理是：
1、直接循环请求不同的页码，为防止反爬虫加入了referer，对应parse函数
2、每一页获取要请求的json数据页面URL地址，对应parse_getJsonPage函数
3、根据返回的json数据，解析出实际要获取数据的URL地址等大部分数据，对应parse_json函数
4、最后获取实际页面内容，少部分数据由该步骤进行获取，对应parse_detail函数
'''

class ZhaopinCompanySpider(scrapy.Spider):
    name = 'zhaopincompany'
    
    # 初始页面
    start_urls = ['https://sou.zhaopin.com/?p=1&pageSize=60&jl=489&kt=3']
    
    # 第一层请求的页面
    def parse(self,response):
        global pageIndex

        # 构造下一页的URL地址，相当于加了一层referer，为了防止根据referer而设计的反爬虫
        while pageIndex < 91:
            nextUrl = 'https://sou.zhaopin.com/?p=' + str(pageIndex) + '&pageSize=60&jl=489&kt=3' 
            referUrl = 'https://sou.zhaopin.com/?p=' + str(pageIndex - 1) + '&pageSize=60&jl=489&kt=3' 

            # 请求json页面地址
            yield scrapy.Request(url=nextUrl,headers={'Referer':referUrl},callback=self.parse_getJsonPage)
            pageIndex += 1
        
    # 获得要请求的json页面地址
    def parse_getJsonPage(self,response):
        # 通过页面左右进行定位
        pageLeftIndex = response.url.find('p=')
        pageRightIndex = response.url.find('&pageSize')
        # 定位具体页码，并转换为数字类型
        pageNum = int(response.url[pageLeftIndex + 2:pageRightIndex])

        nextPage = 'https://fe-api.zhaopin.com/c/i/sou?start=' + str(pageNum * 60) + '&pageSize=60&cityId=489&workExperience=-1&education=-1&companyType=-1&employmentType=-1&jobWelfareTag=-1&kt=3&lastUrlQuery=%7B%22p%22:' + str(pageNum) + ',%22pageSize%22:%2260%22,%22jl%22:%22489%22,%22kt%22:%223%22%7D'
        # 请求json数据页面后，调用解析json数据的函数
        yield scrapy.Request(url=nextPage,meta={'firstLevelUrl':response.url},callback=self.parse_json)

    # 获取json数据，从中提取到链接地址等内容
    def parse_json(self,response):
        # 通过meta得到firstLevelUrl,用于传递给具体页面时作为Referer使用
        firstLevelUrl = response.meta['firstLevelUrl']

        # 获得json数据，数据的格式为60条数据中每一条数据为一个dict，然后组成一个list
        datas = json.loads(response.text)['data']['results']

        # 遍历60条数据
        for singleData in datas:
            # 构造Company对象
            item = Company()

            # 公司名称
            try:
                item['name'] = singleData['company']['name']
            except:
                item['name'] = ''
            
            # 公司人数
            try:
                item['company_size'] = singleData['company']['size']['name']
            except:
                item['company_size'] = ''
            
            # 公司类型
            try:
                item['company_type'] = singleData['company']['type']['name']
            except:
                item['company_type'] = ''
            
            # 所属城市
            try:
                item['company_city'] = singleData['city']['items'][0]['name']
            except:
                item['company_city'] = ''
            
            # 所属省份
            try:
                if item['company_city'] == '':
                    item['company_province'] = ''
                else:
                    item['company_province'] = strip_dashes(item['company_city'])
            except:
                item['company_province'] = ''
            
            # 公司主页
            try:
                item['company_url'] = singleData['company']['url']
            except:
                item['company_url'] = ''
            
            # 发布日期
            item['date'] = datetime.datetime.now().strftime('%Y-%m-%d')
   
            # 请求具体的公司页面，并传递item
            try:
                yield scrapy.Request(url=item['company_url'],meta={'item':item},headers={'Referer':firstLevelUrl},callback=self.parse_detail)
            except:
                print(response.url,'\n',item['company_url'],'\t has error!')
    
    # 解析具体的公司页面
    def parse_detail(self,response):
        # 通过meta得到item
        item = response.meta['item']

        # 公司行业
        try:
            compnay_category = re.findall('industries(.+?)industryIds',response.text)[0]
            item['compnay_category'] = compnay_category.replace('[','').replace(']','').replace('"','').replace(':','').replace(u'（',',') \
            .replace('(',',').replace(u'）','').replace(')','').replace(u'其他',u'其他行业').replace('\r','').replace('\n','').replace('\t','') \
            .replace(' ','').replace('\xa0','').replace('\u3000','').replace('/',',').replace(u'、',',').replace(u'，',',').strip(',')
        except:
            item['compnay_category'] = ''

        # 联系地址
        try:
            address = re.findall('"company":{"address":"(.+?)","city":',response.text)[0]
            item['address'] = address.replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
        except:
            item['address'] = ''
        
        # 公司描述
        try:
            company_desc = re.findall('"description":"(.+?)","industries":',response.text)[0].replace('\xa0','').replace('\u3000','') \
            .replace('\r\n','').replace(' ','').replace('\ufeff','').replace('&nbsp;','')
            item['company_desc'] = remove_tags(company_desc)
            
            # 排除如special.zhaopin.com开头的页面,此类页面可能在取div下标14时就出现异常，被下方except的异常代码所处理
            if 'company.zhaopin.com' not in response.url:
                item['company_desc'] = ''
        except:
            item['company_desc'] = ''

        # 公司联系电话
        mobilePhone = r'(1[38]\d{9})|(14[5-8]\d{8})|(15[0-35-9]\d{8})|(16[56]\d{8})|(17[0-8]\d{8})|(19[89]\d{8})'
        fixedPhone = r'''((010|02[0-57-9])-\d{8})|((03(1[2-90]|7[2-608]|5\d|35|9[1-68]))-\d{7})|((04([15][2-90]|7\d|2[79]|8[23]|3[3-9]|40))-\d{7})
        |((05(3[3-90]|5[2-90]|7[028]|6[1-6]|80|9[2-46-9]))-\d{7})|((06(6[0-3]|9[12]))-\d{7})|((07([17]\d|3[2-90]|5[0-3689]|01|2[248]|4[3-6]|6[2368]|9[2-90]))-\d{7})
        |((08([13]\d|2[5-7]|40|5[4-92]|8[13678]|9[0-39]|7[2-90]))-\d{7})|((09(0[1-3689]|1[0-79]|3[0-8]|4[13]|5[1-5]|7[1-7]|9\d))-\d{7})
        |((03(11|7[179]))-\d{8})|((04([15]1|3[12]))-\d{8})|((05(1\d|2[37]|3[12]|51|7[3-719]|9[15]))-\d{8})|((07([39]1|5[457]|6[09]))-\d{8})|((08(5[13]|98|71))-\d{8})'''
        reg = mobilePhone + r'|' + fixedPhone
        # 初始为空
        item['phone_num'] = ''

        # findall方法返回的是一个list，并且其中的元素又是tuple，因此再取tuple中的元素值，然后做长度判断
        result = re.findall(reg,item['company_desc'])
        for tuple in result:
            for ele in tuple:
                if len(ele) in (11,12,13):
                    item['phone_num'] = item['phone_num'] + ele + ','

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

        yield item
