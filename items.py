#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
公司和职位集合项定义
'''

import scrapy

class Company(scrapy.Item):
    className = 'company'
    name = scrapy.Field() # 公司名称
    company_size = scrapy.Field() # 公司人数
    company_type = scrapy.Field() # 公司类型
    compnay_category = scrapy.Field() # 公司行业
    company_province = scrapy.Field() # 所属省份
    company_city = scrapy.Field() # 所属城市
    phone_num = scrapy.Field() # 联系电话
    address = scrapy.Field() # 联系地址
    company_desc = scrapy.Field() # 公司描述
    company_url = scrapy.Field() # 公司主页
    date = scrapy.Field() # 发布日期

class Job(scrapy.Item):
    className = 'job'
    title = scrapy.Field() # 职位名称
    salary_min = scrapy.Field() # 最低薪资
    salary_max = scrapy.Field() # 最高薪资
    province = scrapy.Field() # 工作省份
    city = scrapy.Field() # 工作城市
    area = scrapy.Field() # 工作区域
    catalog = scrapy.Field() # 工作性质
    category = scrapy.Field() # 行业类别
    experience = scrapy.Field() # 工作经验
    education = scrapy.Field() # 学历要求
    job_desc = scrapy.Field() # 职位描述
    job_url = scrapy.Field() # 职位源网址
    job_company = scrapy.Field() # 公司名称
    from_site = scrapy.Field() # 职位来源网站，如：www.cqhc.org-合川人才网-合川区
    date = scrapy.Field() # 发布日期
    phone_num = scrapy.Field() # 联系电话

class Author(scrapy.Item):
    className = 'author'
    name = scrapy.Field()
    birthdate = scrapy.Field()
    bio = scrapy.Field()
