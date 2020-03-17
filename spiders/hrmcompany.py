# -*- coding: utf-8 -*-

import scrapy
import re
import datetime
from w3lib.html import remove_tags
from datadig.items import Company
from datadig.utils import strip_dashes
from datadig.utils import getCityName

class HrmCompanySpider(scrapy.Spider):
    name = 'hrmcompany'
    start_urls = ['http://www.hrm.cn/jobs?jobTypeId=&industry=&workId=&keyType=0&keyWord=&experienceMin=&experienceMax=&educationMin=&educationMax=&lang=&reftime=&minSalary=&maxSalary=&workTypeId=&entType=&entSize=&isDistrict=0&sortField=&listType=&pageNo=1']

    def parse(self,response):
        item = Company()
        #得到初始展示页面的基准xpath(某一页)，一页上有20条记录
        pages = response.xpath('//li[@class="list_com_name"]/a/@href')

        #循环取出每一页上的每一个链接url地址，并调用parse_page函数解析每一个url上的页面内容
        for eachPage in pages:
            # 获取链接URL（页面上所有的链接，每个链接单独处理），此处的singleUrl前的http://是必不可少的，要指明http协议
            singleUrl = 'http://www.hrm.cn' + eachPage.extract()
            #内部调用parse_page函数
            yield scrapy.Request(url = singleUrl,meta={'item':item},callback=self.parse_page)

        #直接循环剩余的299页
        try:
            #最后一页的a标签中是没有onclick属性值的，其余页码皆有，只改变原始链接的页码值
            if 'searchList' in response.xpath('//*[@id="pagediv"]/a[13]/@onclick').extract()[0]:
                nextPage = response.url.split('pageNo=')[0] + 'pageNo=' + str(int(response.url.split('pageNo=')[1]) + 1)
                yield scrapy.Request(url=nextPage, callback=self.parse)
        except IndexError as ie:
            # 因最后一页的a标签中是没有onclick属性值的，所以不满足条件，即可退出递归
            try:
                exit()
            except SystemExit as se:
                pass

    #爬取单个链接对应的页面内容
    def parse_page(self, response):
          # 通过meta得到item
          item = response.meta['item']

          #公司名称
          try:
              item['name'] = response.xpath('//div[@class="comName"]/div/text()').extract()[0]\
              .replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
          except IndexError as ie:
              item['name'] = ''

          #公司介绍
          try:
              company_desc = response.xpath('//div[@class="com_txt"]').extract()[0]
              #将原有<br>转换为\n
              company_desc = company_desc.replace('<br>','\n')
              # 取子串，将<>所有标签去除，使用非贪婪模式
              company_desc = remove_tags(company_desc)
              item['company_desc'] = company_desc.replace('\r', '').replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace('\n','<br>').replace(u'<br>点击收起<br><br><br><br>','').replace(u'点击收起<br>                        <br>                        ','')

              #因为有重复内容，找到"点击展开<br>                        <br>                        "的索引值
              if u'点击展开<br>                        <br>                        ' in item['company_desc']:
                  #获得重复的索引值
                  index = item['company_desc'].find(u'点击展开<br>                        <br>                        ')
                  item['company_desc'] = item['company_desc'][index:]
                  item['company_desc'] = item['company_desc'].replace(u'点击展开<br>                        <br>                        ','')

              #因为有重复，以 "<br>                        <br>                            "为索引值做判断，注意是rfind()方法
              if '<br>                        <br>                            ' in item['company_desc']:
                  rindex = item['company_desc'].rfind('<br>                        <br>                            ')
                  item['company_desc'] = item['company_desc'][rindex:]
                  item['company_desc'] = item['company_desc'].replace('<br>                        <br>                            ','')

              item['company_desc'] = item['company_desc'].replace('<br><br>                            ','<br>').replace('<br><br>                    ','<br>')\
                  .replace('<br>                            ','<br>')
          except IndexError as ie:
              item['company_desc'] = ''

          #联系地址
          try:
              item['address'] = response.xpath('//div[@class="com_Addr"]/text()').extract()[0]\
              .replace(u'公司地址：','').replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
          except IndexError as ie:
              item['address'] = ''

          #所述城市
          if item['address'] == '':
              item['company_city'] = ''
          else:
              item['company_city'] = getCityName(item['address'])

          #所述省份
          if item['company_city'] == '':
              item['company_province'] = ''
          else:
              item['company_province'] = strip_dashes(item['company_city'])

          #公司类型、公司人数，所在的ul基准xpath
          company_info = response.xpath('//div[@class="company_infoBox"]')
          #公司类型
          try:
              item['company_type'] = company_info.xpath('ul/li[1]/text()').extract()[0]\
              .replace('/',',').replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
          except IndexError as ie:
              item['company_type'] = ''

          #公司行业
          try:
              item['compnay_category'] = company_info.xpath('ul/li[2][@class="indstName"]/text()').extract()[0]
              item['compnay_category'] = item['compnay_category'].replace('，', ',').replace('。',',').replace(u'/模', '').replace('/',',').replace('、', ',').replace('（', ',').replace('(', ',').replace('）', '').replace(')', '').replace(u'请选择,', u'其他行业')\
              .replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')

              # 有逗号 ',' 才考虑去重的问题
              if ',' in item['compnay_category']:
                  # 定义空列表
                  elements = []
                  for e in item['compnay_category'].split(','):
                      #利用,分隔的元素值添加到列表elements中
                      elements.append(e)

                  # 去除重复元素后，转换为列表
                  elements = list(set(elements))

                  item['compnay_category'] = str(elements).replace("'","").replace('[','').replace(']','').replace(' ','')

          except IndexError as ie:
              item['compnay_category'] = ''

          #公司人数
          try:
              item['company_size'] = company_info.xpath('ul/li[3][@class="sizename"]/text()').extract()[0]\
              .replace('\r','').replace('\n','').replace('\t','').replace(' ','').replace('\xa0','').replace('\u3000','')
          except IndexError as ie:
              item['company_size'] = 0

          # 联系电话
          # 正则表达式，前部分括号代表3位-8位   4位-7位或8位   手机号正则
          mobilePhone = r'(1[38]\d{9})|(14[5-8]\d{8})|(15[0-35-9]\d{8})|(16[56]\d{8})|(17[0-8]\d{8})|(19[89]\d{8})'
          fixedPhone = r'''((010|02[0-57-9])-\d{8})|((03(1[2-90]|7[2-608]|5\d|35|9[1-68]))-\d{7})|((04([15][2-90]|7\d|2[79]|8[23]|3[3-9]|40))-\d{7})
          |((05(3[3-90]|5[2-90]|7[028]|6[1-6]|80|9[2-46-9]))-\d{7})|((06(6[0-3]|9[12]))-\d{7})|((07([17]\d|3[2-90]|5[0-3689]|01|2[248]|4[3-6]|6[2368]|9[2-90]))-\d{7})
          |((08([13]\d|2[5-7]|40|5[4-92]|8[13678]|9[0-39]|7[2-90]))-\d{7})|((09(0[1-3689]|1[0-79]|3[0-8]|4[13]|5[1-5]|7[1-7]|9\d))-\d{7})
          |((03(11|7[179]))-\d{8})|((04([15]1|3[12]))-\d{8})|((05(1\d|2[37]|3[12]|51|7[3-719]|9[15]))-\d{8})|((07([39]1|5[457]|6[09]))-\d{8})|((08(5[13]|98|71))-\d{8})'''
          reg = mobilePhone + r'|' + fixedPhone
          # 初始为空
          item['phone_num'] = ''

          # findall方法返回的是一个list，并且其中的元素又是tuple，因此再取tuple中的元素值，然后做长度判断
          result = re.findall(reg, item['company_desc'])
          for tuple in result:
              for ele in tuple:
                  if len(ele) in (11, 12, 13):
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
              item['phone_num'] = str(elements).replace("'", "").replace('[', '').replace(']', '').replace(' ', '')

          #发布日期
          today = datetime.datetime.now()
          item['date'] = today.strftime('%Y-%m-%d')

          #对应招聘网站上的公司主页
          item['company_url'] = response.url

          yield item