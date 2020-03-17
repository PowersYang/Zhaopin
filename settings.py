#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
爬虫全局定义
'''


TIMEZONE = 'Asia/Chengdu'

FEED_EXPORT_ENCODING = 'utf-8'
# FEED_EXPORT_ENCODING = 'gb2312'

MONGO_URI = '192.168.2.61'
MONGO_DATABASE = 'cqbigdata'

MYSQL_URI = 'localhost'
MYSQL_DATABASE = 'ybbigdata'
MYSQL_USER = 'root'
MYSQL_PWD = 'root'

BOT_NAME = 'datadig'

SPIDER_MODULES = [
    'datadig.spiders',
]
NEWSPIDER_MODULE = 'datadig.spiders'

ITEM_PIPELINES = {
    # 'datadig.pipelines.JsonPipeline':100, # 若windows测试，请注释本行。或者注释JsonPipeline中的8行
    # 'datadig.pipelines.MongoPipeline':200,
    # 'datadig.pipelines.MysqlPipeline':300,
}

DOWNLOAD_DELAY = 0.3
DOWNLOADER_MIDDLEWARES = {
    # 'datadig.middlewares.DatadigHttpProxyMiddleware': 1000,
    'datadig.middlewares.DatadigUserAgentMiddleware': 1100,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
}

COOKIES_ENABLED = True
ROBOTSTXT_OBEY = False

# LOG_LEVEL = 'ERROR'
# LOG_FILE = 'datadig.log'


