#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import scrapy
import json
import datetime
import random
from w3lib.html import remove_tags
from datadig.items import Job
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived, RequestGenerationFailed

# 智联允许获取的最大页码值
maxpageno = 12
# 城市与区域的对应关系
city_area = {
    '530': ['530', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012',
            '2013', '2014', '2015', '2016', '2017', '2018'],
    '531': ['531', '2165', '2166', '2167', '2168', '2169', '2170', '2171', '2172', '2173', '2174', '2175', '2176',
            '2177', '2178', '2179', '2180'],
    '532': ['10143', '565', '566', '567', '568', '569', '570', '571', '572', '573', '574', '575'],
    '533': ['576', '577', '578', '579', '580', '581', '582', '583', '584', '585', '586', '910'],
    '534': ['587', '588', '589', '590', '591', '592', '593', '594', '595', '596', '597', '598', '10031', '10157'],
    '535': ['10023', '10144', '599', '10070', '10080', '600', '601', '602', '603', '604', '605', '606', '607', '608',
            '609', '610', '611', '612', '931'],
    '536': ['10122', '613', '614', '615', '616', '617', '618', '619', '620', '621', '10198'],
    '537': ['10081', '622', '623', '624', '625', '626', '627', '628', '629', '630', '631', '632', '633', '634', '10159',
            '10161', '10160', '10510'],
    '538': ['538', '2019', '2021', '2022', '2023', '2024', '2025', '2026', '2027', '2028', '2029', '2030', '2031',
            '2032', '2033', '2034', '2035', '2036'],
    '539': ['635', '636', '637', '638', '639', '640', '641', '642', '643', '644', '645', '646', '647', '648', '650',
            '652', '911'],
    '540': ['653', '654', '655', '656', '657', '658', '659', '660', '661', '662', '663', '10158'],
    '541': ['10069', '664', '665', '666', '667', '668', '669', '670', '671', '672', '673', '674', '675', '677', '678',
            '679', '680', '10181', '10182'], '542': ['681', '682', '683', '684', '685', '687', '688', '689', '690'],
    '543': ['691', '692', '693', '694', '695', '696', '697', '698', '699', '700', '701'],
    '544': ['702', '703', '704', '705', '706', '707', '708', '709', '710', '711', '712', '713', '714', '715', '716',
            '717', '718'],
    '545': ['10059', '10044', '719', '720', '721', '722', '723', '724', '725', '726', '727', '728', '729', '730', '731',
            '732', '733', '734', '735'],
    '546': ['10139', '10140', '10057', '10171', '10179', '10169', '10168', '736', '737', '738', '739', '740', '741',
            '742', '743', '744', '745', '746', '747', '748'],
    '547': ['749', '750', '751', '752', '753', '754', '755', '756', '757', '758', '759', '760', '761', '762'],
    '548': ['763', '764', '765', '766', '767', '768', '769', '770', '771', '772', '773', '774', '775', '776', '777',
            '778', '779', '780', '781', '782', '783'],
    '549': ['785', '786', '787', '788', '789', '790', '791', '792', '793', '794', '795', '796', '904', '905'],
    '550': ['10190', '10192', '10191', '10194', '10193', '10196', '10195', '10197', '10183', '10185', '10184', '10187',
            '10186', '10189', '10188', '10153', '10303', '799', '800', '907'],
    '551': ['551', '2312', '2313', '2314', '2315', '2316', '2317', '2318', '2319', '2320', '2321', '2322', '2323',
            '2324', '2325', '2326', '2327', '2328', '2329', '2330', '2331', '2332', '2333', '2334', '2335', '2336',
            '2337', '2338', '2339', '2340', '2341', '2342', '2343', '2344', '2345', '2346', '2347', '2348', '2349',
            '2350', '2351', '2360', '2433', '2434', '2435', '2436'],
    '552': ['10104', '10065', '10201', '801', '802', '803', '804', '805', '806', '807', '808', '809', '810', '811',
            '812', '813', '814', '815', '816', '817', '818', '819', '820', '821'],
    '553': ['822', '823', '824', '825', '826', '827', '828', '829', '830'],
    '554': ['10163', '831', '832', '833', '834', '835', '836', '837', '838', '840', '841', '842', '843', '844', '845',
            '846'], '555': ['847', '848', '849', '850', '851', '852', '853'],
    '556': ['10058', '854', '855', '856', '857', '858', '859', '860', '861', '862', '863', '10470', '933'],
    '557': ['864', '865', '866', '867', '868', '869', '870', '871', '872', '873', '874', '875', '876', '877'],
    '558': ['878', '879', '880', '881', '882', '883', '884', '885'], '559': ['886', '887', '888', '889', '906'],
    '560': ['10061', '10176', '10178', '10177', '10164', '10166', '10301', '10302', '890', '891', '892', '893', '894',
            '895', '896', '897', '898', '899', '900', '901', '902', '903', '932'], '561': [], '562': [],
    '563': ['10304'], '489': [],
    '480': ['918', '481', '502', '493', '920', '526', '946', '913', '482', '483', '100933', '484', '513', '947', '934',
            '487', '486', '485', '939', '499', '935', '514', '492', '496', '948', '516', '495', '921', '494', '922',
            '945', '949', '480', '506', '923', '509', '914', '488', '930', '491', '936', '924', '951', '507', '952',
            '925', '926', '937', '508', '528', '938', '953', '927', '519', '915', '511', '515', '512', '505', '521',
            '522', '940', '919', '490', '517', '941', '954', '928', '523', '916', '524', '942', '955', '917', '525',
            '929', '520', '497', '518', '510', '498', '504', '943', '100930', '500', '501', '527', '503', '944', '529',
            '100931'], '765': ['765', '2037', '2038', '2039', '2040', '2041', '2042', '2043', '2044', '2361', '2362'],
    '763': ['763', '2045', '2046', '2047', '2048', '2049', '2050', '2052', '2051', '2053', '2054', '2475', '2474'],
    '801': ['801', '2107', '2108', '2109', '2110', '2111', '2112', '2113', '2114', '2115', '2116', '2117', '2118',
            '2119', '2120', '2121', '2377', '2378', '2379', '2380', '2381'],
    '653': ['653', '2238', '2242', '2478', '2236', '2409', '2235', '2479', '2233', '2241', '2234', '2239', '2457',
            '2237', '2240'],
    '736': ['736', '2064', '2059', '2366', '2065', '2066', '2060', '2063', '2068', '2057', '2058', '2067', '2365',
            '2062', '2061', '2367', '2069'],
    '600': ['600', '2397', '2398', '2184', '2185', '2188', '2186', '2187', '2394', '2183', '2395', '2181', '2182',
            '2396'],
    '613': ['613', '2142', '2389', '2143', '2145', '2146', '2388', '2141', '2144', '2140', '2390', '2147', '2148',
            '2387'],
    '635': ['635', '2084', '2086', '2087', '2088', '2090', '2091', '2092', '2093', '2094', '2095', '2096'],
    '702': ['702', '2102', '2376', '2100', '2104', '2101', '2098', '2103', '2105', '2097', '2099', '2471'],
    '703': ['703', '2391', '2393', '2159', '2157', '2160', '2161', '2392', '2164', '2158', '2156', '2162', '2154',
            '2153'], '639': ['639', '2404', '2218', '2511', '2215', '2561', '2216', '2217'],
    '599': ['599', '2126', '2127', '2128', '2129', '2130', '2132', '2133', '2134', '2135', '2382', '2383', '2384',
            '2385', '2386'],
    '854': ['854', '2070', '2071', '2072', '2073', '2074', '2075', '2076', '2077', '2078', '2079', '2080', '2081',
            '2082', '2083', '2368', '2369', '2370', '2371', '2372', '2373', '2374'],
    '719': ['719', '2194', '2195', '2196', '2197', '2198', '2199', '2203', '2204', '2205', '2399', '2400', '2401',
            '2402', '2403', '2444', '2445'],
    '749': ['749', '2406', '2224', '2227', '2408', '2407', '2225', '2405', '2226', '2228'],
    '622': ['622', '2277', '2429', '2428', '2271', '2272', '2426', '2276', '2430', '2270', '2275', '2274', '2431',
            '2424', '2273', '2432', '2427'],
    '636': ['2516', '2517', '2514', '2519', '2512', '2515', '2518', '2520', '2513'],
    '654': ['3006', '3370', '3001', '3008', '3003', '3005', '3004', '3372', '3373', '3002', '3371', '3007'],
    '681': ['681', '2253', '2472', '2473', '2251', '2255', '2258', '2257', '2254', '2256', '2260', '2261', '2252',
            '2259'], '682': ['682', '2267', '2265', '2266', '2264', '2268', '2269'],
    '565': ['565', '2288', '2293', '2296', '2418', '2420', '2294', '2297', '2414', '2412', '2299', '2301', '2289',
            '2290', '2415', '2416', '2413', '2291', '2295', '2298', '2302', '2292', '2419', '2417', '2300'],
    '664': ['664', '2355', '2438', '2357', '2359', '2356', '2352', '2354', '2358', '2353', '2437'],
    '773': ['3255', '3254', '2246', '3256', '2247', '3257', '3253']}
# 构造初始请求需要的4部分字符串
prefix = 'https://sou.zhaopin.com/?p='
locationargs = '&jl='
areaargs = '&re='
suffix = '&sf=0&st=0'
# 构造ajax请求的固定字符串
json_first = 'https://fe-api.zhaopin.com/c/i/sou?start='
json_second = '&pageSize=90&cityId='
json_third = '&salary=0,0&workExperience=-1&education=-1&companyType=-1&employmentType=-1&jobWelfareTag=-1&kt=3&=0&_v='
json_fourth = '&x-zp-page-request-id='
json_fifth = '&x-zp-client-id='

'''
由于智联允许获取的页码受限制，因此需要找到获取所有数据的方式。本代码通过循环请求所有地域的方式来获取所有数据(地域有限，关键字无限)
1、根据地域参数请求不同的URL，get请求中的URL部分参数作为ajax请求的参数来源，且此URL作为referer，防止ban。对应parse函数
2、从初始的URL中提取部分参数用于构造ajax请求的URL地址，该ajax请求获取到的数据为json格式。对应get_json函数
3、解析json格式的数据内容，并在json中提取实际job页面的URL地址，并请求该URL地址，此函数获取大部分item的字段。对应parse_json函数
4、请求实际的job页面，并获取剩余部分item对应的字段内容，最后yield。对应parse_detail函数
'''


class ZhaopinSpider(scrapy.Spider):
    name = 'zhaopin'
    start_urls = ['https://sou.zhaopin.com/?jl=489&sf=0&st=0']
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1
    }

    def parse(self, response):
        # 通过地域参数来确定URL，目前已经找到城市与区域的对应关系，可直接构造原始请求的URL
        # k为locationargs参数，v为areaargs参数构造的列表
        for k, v in self.random_dict(city_area).items():
            # 部分v为空列表
            if len(v) == 0:
                # 构造页码参数
                for x in range(1, maxpageno + 1):
                    url = prefix + str(x) + locationargs + k + suffix
                    refererurl = prefix + str(x - 1) + locationargs + k + suffix
                    yield scrapy.Request(url=url, headers={'Referer': refererurl}, meta={'start': x, 'cityId': k},
                                         callback=self.get_json, errback=self.parse_errback, dont_filter=True)
            else:
                # 此处外循环用页码循环，x、y为页码
                for y in range(1, maxpageno + 1):
                    for areaid in v:
                        link = prefix + str(y) + locationargs + k + areaargs + areaid + suffix
                        refererlink = prefix + str(y - 1) + locationargs + k + areaargs + areaid + suffix
                        yield scrapy.Request(url=link, headers={'Referer': refererlink},
                                             meta={'start': y, 'cityId': areaid}, callback=self.get_json,
                                             errback=self.parse_errback, dont_filter=True)

    # 将字典按照键乱序排序后返回新生成的字典
    def random_dict(self, old_dict):
        # 获取旧的字典的键值列表并乱序
        old_list = list(old_dict.keys())
        random.shuffle(old_list)

        # 定义新的字典用于存放乱序后的元素
        new_dict = {}
        for ele in old_list:
            new_dict[ele] = old_dict[ele]
        return new_dict

    # 构造ajax请求的URL生成函数
    def get_json(self, response):
        # 确定5个变化的参数
        start = str((response.meta['start'] - 1) * 90)
        cityId = response.meta['cityId']
        v = str(random.random())[:10]
        xzpri = ''
        xzci = ''

        jsonurl = json_first + start + json_second + cityId + json_third + v + json_fourth + xzpri + json_fifth + xzci
        yield scrapy.Request(url=jsonurl, meta={'firstlevelurl': response.url}, callback=self.parse_json,
                             errback=self.parse_errback, dont_filter=True)

    # 获取json数据，从中提取到链接地址等内容
    def parse_json(self, response):
        # 通过meta得到firstlevelurl,用于传递给具体页面时作为Referer使用
        firstlevelurl = response.meta['firstlevelurl']

        # 获得json数据，数据的格式为90条数据中每一条数据为一个dict，然后组成一个list
        # 请求无问题，但可能因网络等原因造成请求的数据未获取到，需要对datas做相应的异常处理
        try:
            datas = json.loads(response.text)['data']['results']
        except Exception as e:
            datas = []
            self.logger.error('response.text is <', str(response.text), '>')
            self.logger.error('url <', response.url, '> requested successful, but nothing returned!')

        # 遍历60条数据
        for singleData in datas:
            # 构造Job对象
            item = Job()

            # 职位名称
            try:
                item['title'] = singleData['jobName']
            except:
                item['title'] = ''

            # 最低薪资
            try:
                # '薪资面议'用split('-')分割，仍然有一个元素
                if len(singleData['salary'].split(u'-')) == 2:
                    salary_min = singleData['salary'].split(u'-')[0]
                    if u'K' in salary_min:
                        salary_min = int(
                            float(salary_min.replace(u'K', '')) * 1000)

                    item['salary_min'] = salary_min
                else:
                    item['salary_min'] = 0
            except:
                item['salary_min'] = 0

            # 最高薪资
            try:
                if len(singleData['salary'].split(u'-')) == 2:
                    salary_max = singleData['salary'].split(u'-')[1]
                    if u'K' in salary_max:
                        salary_max = int(
                            float(salary_max.replace(u'K', '')) * 1000)

                    item['salary_max'] = salary_max
                else:
                    item['salary_max'] = 0
            except:
                item['salary_max'] = 0

            # 工作城市
            try:
                item['city'] = singleData['city']['items'][0]['name']
            except:
                item['city'] = ''

            # 工作区域
            try:
                item['area'] = singleData['city']['items'][1]['name']
            except:
                item['area'] = u'全区域'

            # 工作性质
            try:
                item['catalog'] = singleData['emplType']
            except:
                item['catalog'] = ''

            # 工作经验
            try:
                item['experience'] = singleData['workingExp']['name']
                if u'不限' in item['experience']:
                    item['experience'] = u'无要求'

                if item['experience'] == '':
                    item['experience'] = u'无要求'
            except:
                item['experience'] = u'无要求'

            # 学历要求
            try:
                item['education'] = singleData['eduLevel']['name']
                if u'不限' in item['education']:
                    item['education'] = u'无要求'

                if item['education'] == '':
                    item['education'] = u'无要求'
            except:
                item['education'] = u'无要求'

            # 职位URL网址
            try:
                item['job_url'] = singleData['positionURL']
            except:
                item['job_url'] = ''

            # 职位公司名称
            try:
                item['job_company'] = singleData['company']['name']
            except:
                item['job_company'] = ''

            # 职位来源网址
            item['from_site'] = u'www.zhaopin.com-智联招聘网'

            # 职位发布日期
            item['date'] = datetime.datetime.now().strftime('%Y-%m-%d')

            # 请求具体的招聘页面，并传递item
            try:
                yield scrapy.Request(url=item['job_url'], meta={'item': item}, headers={'Referer': firstlevelurl},
                                     callback=self.parse_detail, errback=self.parse_errback, dont_filter=True)
            except:
                print(response.url, '\n', item['job_url'], '\t has error!')

    # 解析具体的招聘页面
    def parse_detail(self, response):
        # 通过meta得到item
        item = response.meta['item']

        # 行业类别
        try:
            category = response.xpath(
                '//button[@class="company__industry"]/text()').extract()[0]
            item['category'] = category.replace(u'，', ',').replace('/', ',').replace(u'、', ',').replace(u'（',
                                                                                                        ',').replace(
                '(', ',') \
                .replace(u'）', '').replace(')', '').replace(u'其他', u'其他行业').replace('\r', '').replace('\n', '').replace(
                '\t', '') \
                .replace(' ', '').replace('\xa0', '').replace('\u3000', '')
        except:
            item['category'] = ''

        # 职位描述
        try:
            job_desc = response.xpath(
                '//div[@class="describtion__detail-content"]').extract()[0].replace('<br>', '\n')
            job_desc = remove_tags(job_desc).replace('&nbsp;', '').replace(
                '\xa0', '').replace('\u3000', '').replace('\n', '').replace('<br><br>', '')
            item['job_desc'] = remove_tags(job_desc)
        except:
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
