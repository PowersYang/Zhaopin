#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
mongodb API
'''

import datetime
import json
import logging
import pymongo

from datadig.settings import MONGO_URI
from datadig.settings import MONGO_DATABASE

logging.basicConfig(filename='digging.log')
logger = logging.getLogger(__name__)


class JsonPipeline(object):

    def open_spider(self, spider):
        # 若windows测试，请注释以下4行
        today = datetime.datetime.now().strftime('%Y%m%d')
        prePath = '/home/bigdata/jsonfiles/'
        self.jobsFile = open(prePath + 'jobs/' + today + '.jl', 'a')
        self.companiesFile = open(prePath + 'companies/' + today + '.jl', 'a')

    def process_item(self, item, spider):
        if 'job' == item.className:
            # 若windows测试，请注释以下2行
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.jobsFile.write(line)

        elif 'company' == item.className:
            # 若windows测试，请注释以下2行
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.companiesFile.write(line)

        elif 'author' == item.className:
            pass

        return item

    def close_spider(self, spider):
        self.jobsFile.close()
        self.companiesFile.close()


class MongoPipeline(object):
    collection_companies = 'companies'
    collection_jobs = 'jobs'

    def __init__(self):
        self.mongo_uri = MONGO_URI
        self.mongo_db = MONGO_DATABASE

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        date = item.get('date', '').strip()
        if date == '':
            date = datetime.datetime.now().strftime('%Y-%m-%d')

        if 'job' == item.className:
            title = item.get('title', '').strip()
            job_company = item.get('job_company', '').strip()
            if title != '' and job_company != '':
                collection = self.db[self.collection_jobs]

                jobQuery = {
                    "title": title,
                    "job_company": job_company,
                    "city": item.get('city', '').strip(),
                    # "area": item.get('area', '').strip(),
                    "date": date
                }
                jobIsExist = collection.find_one(jobQuery)

                if jobIsExist is None:
                    jobInfo = {
                        "title": title,
                        "salary_min": item.get('salary_min', 0),
                        "salary_max": item.get('salary_max', 0),
                        "province": item.get('province', '').strip(),
                        "city": item.get('city', '').strip(),
                        "area": item.get('area', '').strip(),
                        "catalog": item.get('catalog', '').strip(),
                        "category": item.get('category', '').strip(),
                        "experience": item.get('experience', '').strip(),
                        "education": item.get('education', '').strip(),
                        "job_desc": item.get('job_desc', '').strip(),
                        "job_url": item.get('job_url', '').strip(),
                        "job_company": job_company,
                        "from_site": item.get('from_site', '').strip(),
                        "date": date
                    }

                    collection.insert_one(jobInfo)

            else:
                pass

        elif 'company' == item.className:
            name = item.get('name', '').strip()
            company_city = item.get('company_city', '').strip()
            if name != '' and company_city != '':
                collection = self.db[self.collection_companies]

                companyQuery = {
                    "name": name,
                    "company_city": company_city
                }
                companyIsExist = collection.find_one(companyQuery)

                if companyIsExist is None:
                    companyInfo = {
                        "name": name,
                        "company_size": item.get('company_size', ''),
                        "company_type": item.get('company_type', '').strip(),
                        "compnay_category": item.get('compnay_category', '').strip(),
                        "company_province": item.get('company_province', '').strip(),
                        "company_city": company_city,
                        "phone_num": item.get('phone_num', '').strip(),
                        "address": item.get('address', '').strip(),
                        "company_desc": item.get('company_desc', '').strip(),
                        "company_url": item.get('company_url', '').strip(),
                        "date": date
                    }

                    collection.insert_one(companyInfo)

            else:
                pass

        elif 'author' == item.className:
            pass

        return item

    def close_spider(self, spider):
        self.client.close()


'''
class MysqlPipeline(object):

    def __init__(self):
        self.mysql_uri = settings.get('MYSQL_URI')
        self.mysql_db = settings.get('MYSQL_DATABASE')

    def open_spider(self, spider):
        self.mysql_conn = pymysql.connect(
            host = self.mysql_uri,
            user = 'root',
            password = 'root',
            db = self.mysql_db,
            charset = 'utf8mb4',
            cursorclass = pymysql.cursors.DictCursor)

    def process_item(self, item, spider):
        if 'job' == item.className:
            try:
                sql_search = "SELECT * FROM `cqbigdata_job` WHERE `title`=%s AND `city`=%s AND `job_company`=%s AND `date_`=%s"
                with self.mysql_conn.cursor() as cursor:
                    cursor.execute(sql_search, (item.get('title', ''), item.get('city', ''), item.get('job_company', ''), item.get('date', '')))
                    jobIsExist = cursor.fetchone()
                    
                    if jobIsExist is None:
                        sql_write = "INSERT INTO `cqbigdata_job` (`title`, `salary_min`, `salary_max`, `city`, `area`, `catalog`, `category`, `experience`, `education`, `job_desc`, `job_company`, `from_site`, `date_`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql_write, (item.get('title', ''), item.get('salary_min', ''), item.get('salary_max', ''), item.get('city', ''), item.get('area', ''), item.get('catalog', ''), item.get('category', ''), item.get('experience', ''), item.get('education', ''), item.get('job_desc', ''), item.get('job_company', ''), item.get('from_site', ''), item.get('date', '')))

                self.mysql_conn.commit()
                
            except Exception as e:
                logger.error(e)
                pass
            
        elif 'company' == item.className:
            try:
                sql_search = "SELECT * FROM `cqbigdata_company` WHERE `name`=%s AND `company_city`=%s"
                with self.mysql_conn.cursor() as cursor:
                    cursor.execute(sql_search, (item.get('name', ''), item.get('company_city', '')))
                    companyIsExist = cursor.fetchone()
                    
                    if companyIsExist is None:
                        sql_write = "INSERT INTO `cqbigdata_company` (`name`, `company_size`, `company_type`, `compnay_category`, `company_province`, `company_city`, `phone_num`, `address`, `company_desc`, `date_`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql_write, (item.get('name', ''), item.get('company_size', ''), item.get('company_type', ''), item.get('compnay_category', ''), item.get('company_province', ''), item.get('company_city', ''), item.get('phone_num', ''), item.get('address', ''), item.get('company_desc', ''), item.get('date', '')))

                self.mysql_conn.commit()

            except Exception as e:
                logger.error(e)
                pass

        elif 'author' == item.className:
            pass
        
        return item

    def close_spider(self, spider):
        self.mysql_conn.close()
'''
