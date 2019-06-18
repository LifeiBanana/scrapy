# -*- coding:utf-8 -*-
import time
import requests
from lxml import etree
import re
import json
import csv
import pandas as pd
from threading import Thread
from multiprocessing.dummy import Pool as ThreadPool
from pymongo import MongoClient


class Login(object):
    # 初始化爬虫，伪装成Chrome
    def __init__(self):
        self.headers = {
            'referer': 'https://www.themoviedb.org/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
        }
        self.login_url = 'https://www.themoviedb.org/login'
        self.post_url = 'https://www.themoviedb.org/login'
        self.session = requests.Session()

    # 登录账号密码，修改cookie
    def login(self, email, password):
        post_data = {
            'username': email,
            'password': password
        }
        self.session.get(self.login_url, headers=self.headers)
        response = self.session.post(self.post_url, data=post_data, headers=self.headers)
        print('提交账号密码', response.status_code)
        if response.status_code == 200:
            # 改造cookie
            # cookie = ""
            # login = re.findall(r"^[\S]*", response.headers['Set-Cookie'])
            # session = re.findall(r"session[\S]*", response.headers['Set-Cookie'])
            # dc = "dc_gtm_UA-2087971-10 = 1; gali=main; "
            # access_token = "access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1NTQxODcwOTMsImV4cCI6MTU1NTM5NjY5Mywic2NvcGVzIjpbIndlYl91c2VyIl0sInN1YiI6IjVjOWRkODFhMGUwYTI2MzhkZWVkMWFjYyIsImp0aSI6MTI2Mjg1MywiYXVkIjoiZTE1ZjM1Y2E1NjJiYTZiNDJhY2MxZGI1NzgxYjhhZjUiLCJyZWRpcmVjdF90byI6bnVsbCwidmVyc2lvbiI6MX0.OgFyCfScxaF4ygqNtCs7NF9d1yWTdKLD1UV8H4DsZ0M;"
            # cookie = cookie + login[0] + session[0] + dc + access_token
            # self.headers['cookie'] = cookie
            self.headers[
                'cookie'] = 'session=BAh7CEkiD3Nlc3Npb25faWQGOgZFVEkiRTU5YWQ3ZTkxNWFiYzhjYjhjZGMz%0AOTdmYzdjYzI2YWQ5MjI4MTQyMjQyMTFlM2UwOTc2NDliMjM3NDFmZjhlNjgG%0AOwBGSSIJY3NyZgY7AEZJIjF3cDR3VVpuL25aMmZ3dHpsZ0F3SUROSmdKQzBR%0AckRHYmFwOU15U3Y1eGZ3PQY7AEZJIg10cmFja2luZwY7AEZ7BkkiFEhUVFBf%0AVVNFUl9BR0VOVAY7AFRJIi05YmZhMmY1MzdmYWYyM2JkOWI1ODg4Mzk0NjBi%0ANjU3ZjYzNzhhYzkzBjsARg%3D%3D%0A--a5340ea991552420cec3e59bdb1a158b185aa0d5; _ga=GA1.2.349019275.1559787128; _gid=GA1.2.644450946.1559787128; prefs=v%3D1%26language%3Den%26locale%3Dus%26i18n_language%3Den-US%26i18n_fallback_language%3Den-US%26adult%3Dfalse%26timezone%3DAmerica%2FNew_York%26list_style%3D1; login=ID%3D5cf87775c3a3686bb320b98b%26R%3Deff99403f0802e39a6c68fb099320e35; access_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1NTk3ODczOTAsImV4cCI6MTU2MDk5Njk5MCwic2NvcGVzIjpbIndlYl91c2VyIl0sInN1YiI6IjVjOWRkODFhMGUwYTI2MzhkZWVkMWFjYyIsImp0aSI6MTM2OTM2MywiYXVkIjoiZTE1ZjM1Y2E1NjJiYTZiNDJhY2MxZGI1NzgxYjhhZjUiLCJyZWRpcmVjdF90byI6bnVsbCwidmVyc2lvbiI6MX0.1Ijk_PXZ6t9-UWVLqgQmksUOahhUD5xFCSIFJQWDmog'
        else:
            print('login_err:提交账号密码页面注册码失效，请重新载入')

    # 判断爬取的数据所存入的csv是否存在，不存在创建加上第一行
    def file_not_exit(self, csv_file):
        attribute = ['Keywords', 'Movie_id', 'backdrops_path', 'belongs_to_collection', 'budget', 'cast', 'crew',
                     'genres',
                     'homepage',
                     'imdb_id', 'original_language', 'original_title', 'overview', 'popularity', 'poster_path',
                     'production_companies', 'production_countries', 'release_date', 'revenue', 'runtime',
                     'spoken_languages', 'status', 'tagline', 'title']
        try:
            # 判断文件是否存在，不存在则新建并加上第一行属性
            open(csv_file)
            print('train文件存在')
        except:
            print('train文件不存在')
            csvfile = open(csv_file, 'w', newline='', encoding='utf-8')
            csv_write = csv.writer(csvfile, dialect='excel')
            csv_write.writerow(attribute)
            csvfile.close()

    # 获取爬取该年份的最大页码数
    def max_page(self, max_page_url, headers):
        response = self.retryGet(url=max_page_url, headers=headers, timeout=3)
        if response == 'retry_times_out':
            print('无法获取最大页数')
        # print('首页登录：', response.status_code)
        elif response.status_code == 200:
            # self.movieindex(response.text, self.movie_id)
            selector = etree.HTML(response.text)
            # 利用xpath提取地址后缀
            try:
                max_page_num = selector.xpath('//div[@class="pagination"]/a[last()-1]/text()')[0]
            except:
                print('警告警告，无法获取最大页数')
                max_page_num = 1
            return max_page_num
        else:
            print('找不到最大页码')

    # 每一个网页都有三次重新爬去的机会，若时间超时自动判为失败，重新加载
    def retryGet(self, url, headers, timeout):
        # 重试次数
        retry_num = 0
        while 1:
            try:
                if retry_num < 3:
                    response = self.session.get(url, headers=headers, timeout=timeout)
                    return response
                else:
                    # 超过三次抛出异常
                    return 'retry_times_out'
            except:
                retry_num += 1


class scrapy_url(Thread):
    def __init__(self, collection_url, movie_20, headers, ids):
        super(scrapy_url, self).__init__()
        self.collection_url = collection_url
        self.movie_20 = movie_20
        self.headers = headers
        self.ids = ids
        self.movie_someone = 'https://www.themoviedb.org/movie/'
        self.session = requests.Session()

    # 获取爬虫关键词
    def run(self):
        # 统计第几个20页
        global movie_20_count
        # 访问20部电影页面
        while self.movie_20:
            try:
                response = self.session.get(self.collection_url, headers=self.headers, timeout=3)
                self.movie_20 = False
                # 成功爬取后+1
                movie_20_count += 1
            except:
                print('20部电影页面超时，重试中...')
        print('第{}个20部电影页面{}'.format(movie_20_count, response.status_code))
        if (response.status_code == 200):
            # movies_id_list是存储所有电影的id列表
            movies_id_list = self.id_collect(response.text)
            # movie_id用于存储所有爬虫关键词
            movie_id = []
            # 遍历所有id，伪装成地址的一部分(id+电影名)
            for i in movies_id_list:
                someone_movie = self.movie_someone + i[0] + '?'
                try:
                    response = self.session.get(someone_movie, headers=self.headers, allow_redirects=False)
                    movie_id.append(response.headers['location'].split('/')[2])
                except:
                    # 个别不常规网页处理
                    try:
                        movie_id.append(someone_movie.split('/')[4][:-1])
                    except:
                        print('login_err:' + someone_movie + '页面进入失败')
            # 将电影关键词和页数存入数据库中
            for id in movie_id:
                self.ids.insert_one({"movie_id": id, "age": i})
            # return movie_id
        else:
            print('login_err:20部电影页面注册码失效，请重新载入')

    # 获取该页所有电影伪id
    def id_collect(self, html):
        movies_id_list = []
        selector = etree.HTML(html)
        # 利用xpath提取地址后缀
        movies_id = selector.xpath('//div[@class="item poster card"]//a[@class="title result"]/@href')
        # 遍历所有id，正则化所需部分，添加到列表movies_id_list中
        for i in movies_id:
            i = re.findall(r"^([\d]*)", i.split('/')[2])
            movies_id_list.append(i)
        return movies_id_list


class Scrapy_test(Thread):
    # 爬取所需信息
    def __init__(self, results, movie_id, csv_file, headers, ids):
        super(Scrapy_test, self).__init__()
        self.headers = headers
        self.results = results
        self.movie_id = movie_id
        self.csv_file = csv_file
        self.ids = ids
        self.session = requests.Session()

    def run(self):
        global success_num
        self.movie_index = 'https://www.themoviedb.org/movie/' + self.movie_id
        self.movie_edit = 'https://www.themoviedb.org/movie/' + self.movie_id + '/edit'
        self.movie_genres = 'https://www.themoviedb.org/movie/' + self.movie_id + '/remote/genres?translate=false'
        self.movie_popularity = 'https://www.themoviedb.org/movie/' + self.movie_id + '/remote/popularity?'
        self.movie_imdb_id = 'https://www.themoviedb.org/movie/' + self.movie_id + '/edit?active_nav_item=external_ids'
        self.movie_porduction_companies = 'https://www.themoviedb.org/movie/' + self.movie_id + '/remote/production_companies?translate=false'
        self.movie_producton_countries = 'https://www.themoviedb.org/movie/' + self.movie_id + '/edit?active_nav_item=production_information'
        self.movie_keyword = 'https://www.themoviedb.org/movie/' + self.movie_id + '/remote/keywords?translate=false'
        self.movie_cast = 'https://www.themoviedb.org/movie/' + self.movie_id + '/remote/cast?translate=false'
        self.movie_crew = 'https://www.themoviedb.org/movie/' + self.movie_id + '/remote/crew?translate=false'
        self.movie_backdrops = 'https://www.themoviedb.org/movie/' + self.movie_id + '/images/backdrops'

        # 首页爬取
        ###########################################################################
        response = self.retryGet(url=self.movie_index, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:该{}电影没有首页面'.format(self.movie_id))
            return
        # print('首页登录：', response.status_code)
        elif response.status_code == 200:
            self.movieindex(response.text, self.movie_id)
        else:
            print('login_err:首页登录注册码失效，请重新载入')
        ############################################################################
        # 判断该电影是否不存在英文翻译
        # 电影编辑页面爬取
        response = self.retryGet(url=self.movie_edit, headers=self.headers, timeout=7)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有电影编辑页面'.format(self.movie_id))
            # 成功执行，删除该电影关键词，有待商榷
            # self.ids.delete_one({'movie_id': self.movie_id})
            return
        # print('电影编辑页：', response.status_code)
        elif response.status_code == 200:
            self.movieedit(response.text, self.movie_id)
        else:
            print('language_err:该{}电影没有英文解析'.format(self.movie_id))
            self.ids.delete_one({'movie_id': self.movie_id})
            return
            # print('login_err:电影编辑页注册码失效，请重新载入')
        ############################################################################
        # 电影关键词爬取
        response = self.retryGet(url=self.movie_keyword, headers=self.headers, timeout=3)
        # print('电影关键词页：', response.status_code)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有关键词信息'.format(self.movie_id))
            return
        elif response.status_code == 200:
            try:
                self.results['Keywords'] = self.delete_attribute(response.json())
            except:
                pass
                # print('scrapy_err:该{}电影没有关键词信息'.format(self.movie_id))
            # print('电影关键词:', self.results)
        else:
            print('login_err:电影关键词页注册码失效，请重新载入')
        ############################################################################
        # 电影演员爬取
        response = self.retryGet(url=self.movie_cast, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有演员信息'.format(self.movie_id))
            return
        # print('电影演员页：', response.status_code)
        elif response.status_code == 200:
            try:
                # 由于csv一个单元格只能装下32768个字符，所以取前150个电影演员
                self.results['cast'] = self.delete_attribute(response.json()[:150])
            except:
                pass
                # print('scrapy_err:该{}电影没有演员信息'.format(self.movie_id))
            # print('电影演员页面:', self.results)
        else:
            print('login_err:电影演员页注册码失效，请重新载入')
        ############################################################################
        # 电影职员爬取
        response = self.retryGet(url=self.movie_crew, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有电影职员/工作人员信息'.format(self.movie_id))
            return
        # print('电影职员/工作人员页：', response.status_code)
        elif response.status_code == 200:
            try:
                # 由于csv一个单元格只能装下32768个字符，所以取前150个电影职员/工作人员
                self.results['crew'] = self.delete_attribute(response.json()[:150])
            except:
                pass
                # print('scrapy_err:该{}电影没有电影职员/工作人员信息'.format(self.movie_id))
            # print('电影职员/工作人员页面:', self.results)
        else:
            print('login_err:电影职员/工作人员页注册码失效，请重新载入')
        ############################################################################
        # 电影imdb注册码爬取
        response = self.retryGet(url=self.movie_imdb_id, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有imdb页注册码'.format(self.movie_id))
            return
        # print('imdb页：', response.status_code)
        elif response.status_code == 200:
            self.movieimdbid(response.text, self.movie_id)
        else:
            print('login_err:imdb页注册码失效，请重新载入')
        ############################################################################
        # 电影制作公司爬取
        response = self.retryGet(url=self.movie_porduction_companies, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有制作公司信息'.format(self.movie_id))
            return
        # print('制作公司页：', response.status_code)
        elif response.status_code == 200:
            try:
                self.results['production_companies'] = self.delete_attribute(response.json())
            except:
                pass
                # print('scrapy_err:该{}电影没有制作公司信息'.format(self.movie_id))
            # print('制作公司页面:', self.results)
        else:
            print('login_err:制作公司页注册码失效，请重新载入')
        ############################################################################
        # 电影类型爬取
        response = self.retryGet(url=self.movie_genres, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有电影类型信息'.format(self.movie_id))
            return
        # print('电影类型页：', response.status_code)
        elif response.status_code == 200:
            try:
                self.results['genres'] = self.delete_attribute(response.json())
            except:
                pass
                # print('scrapy_err:该{}电影没有电影类型信息'.format(self.movie_id))
                # print('电影类型页面:', self.results)
        else:
            print('login_err:电影类型页注册码失效，请重新载入')
        ############################################################################
        # 电影发行信息爬取
        response = self.retryGet(url=self.movie_producton_countries, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有发行信息'.format(self.movie_id))
            return
        # print('发行信息页：', response.status_code)
        elif response.status_code == 200:
            self.movieproduction_countries(response.text, self.movie_id)
        else:
            print('login_err:发行信息页注册码失效，请重新载入')
        ############################################################################
        # 电影人气爬取
        response = self.retryGet(url=self.movie_popularity, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:时间超时该{}电影没有人气信息'.format(self.movie_id))
            return
        # print('人气页面页：', response.status_code)
        elif response.status_code == 200:
            try:
                # sum求人气总数
                sum = 0
                # count统计一共多少次
                count = 0
                # popularty_dit是ajax返回的曲线图数据
                popularty_dit = json.loads(response.text)
                for i in popularty_dit:
                    sum += i['value']
                    count += 1
                # popularty为平均人气
                popularty = -sum / count
                self.results['popularity'] = popularty
            except:
                pass
                # print('scrapy_err:该{}电影没有人气信息'.format(self.movie_id))
        else:
            print('login_err:人气页面页注册码失效，请重新载入')
        ###########################################################################
        # 电影海报爬取
        response = self.retryGet(url=self.movie_backdrops, headers=self.headers, timeout=3)
        if response == 'retry_times_out':
            print('language_err:该{}电影没有海报'.format(self.movie_id))
            return
        elif response.status_code == 200:
            selector = etree.HTML(response.text)
            try:
                backdrops = selector.xpath('//a[@class="image"]/@href')[0]
                self.results['backdrops_path'] = backdrops
            except:
                pass
                # print('language_err:该{}电影没有海报'.format(self.movie_id))
        else:
            print('login_err:海报页面注册码失效，请重新载入')
        ############################################################################
        # 保存电影唯一标识
        self.results['Movie_id'] = self.movie_id
        # 统计成功次数
        success_num += 1
        # 将最终结果写入文件train中
        # 把结果按key升序排序
        self.results = sorted(self.results.items(), key=lambda item: item[0])
        write_row = []
        for i in self.results:
            # 取属性值
            write_row.append(i[1])
        # 写入到文件中
        csvfile = open(self.csv_file, 'a', newline='', encoding='utf-8')
        csv_write = csv.writer(csvfile, dialect='excel')
        csv_write.writerow(write_row)
        csvfile.close()
        # 成功执行，删除该电影关键词
        self.ids.delete_one({'movie_id': self.movie_id})

    # 首页爬取
    def movieindex(self, html, movie_id):
        selector = etree.HTML(html)
        # 爬取上映时间
        try:
            release_date = selector.xpath('//ul[@class="releases"]/li/text()')[1].strip()
            self.results['release_date'] = release_date
        except:
            pass
            # print('scrapy_err:该{}电影没有上映时间'.format(movie_id))
        try:
            poster_path = selector.xpath(
                '//section[@class="panel media_panel media scroller"]//div[@class="poster"]/img[@class="poster"]/@srcset')
            poster_path = re.findall(r"face([\S|\s]*)1x", poster_path[0])[0]
            self.results['poster_path'] = poster_path
        except:
            pass
            # print('scrapy_err:该{}电影没有电影海报'.format(movie_id))
        ############################################################################
        try:
            # 通过电影首页爬取belong_to_collection
            collection = selector.xpath(
                '//div[@id="media_v4" and @class="media movie_v4 header_large"]//section[@class="panel"]/script/text()')
            belongs_to_collection = {}
            # 爬取belong_to_collection的id
            collection_id = re.findall(r"/collection/([\d]*)/static", collection[0])[0]
            # 爬取belong_to_collection的poster_path
            collection_poster = selector.xpath(
                '//section[@class="panel media_panel media scroller"]//div[@class="poster"]/img[@class="poster"]/@srcset')
            collection_poster = re.findall(r"face([\S|\s]*)1x", collection_poster[0])[0]
            # 爬取belong_to_collection的backdrop_path
            collection_backdrop = selector.xpath(
                '//section[@class="panel media_panel media scroller"]//div[@class="backdrop"]//@data-src')
            collection_backdrop = re.findall(r"bestv2([\S|\s]*)", collection_backdrop[0])[0]
            # 进入另一个包含belong_to_collection的name的页面
            # 爬取belong_to_collection的name
            jump_web = re.findall(r"url:[\s]*'([\S]*)'", collection[0])[0]
            web = 'https://www.themoviedb.org'
            web += jump_web
            response = self.retryGet(url=web, headers=self.headers, timeout=3)
            if response == 'retry_times_out':
                print('language_err:时间超时该{}电影没有收藏信息'.format(self.movie_id))
                return
            selector = etree.HTML(response.text)
            collection_name = selector.xpath('//div[@class="header collection"]//h2/text()')
            collection_name = re.findall(r"the ([\S|\s]*)$", collection_name[0])[0]
            ############################################################################
            belongs_to_collection['id'] = collection_id
            belongs_to_collection['name'] = collection_name
            belongs_to_collection['poster_path'] = collection_poster
            belongs_to_collection['backdrop_path'] = collection_backdrop
            # belong_to_collection为字典，需要在外面加一个列表形如[{}]
            belong_to_collection_list = []
            belong_to_collection_list.append(belongs_to_collection)
            self.results['belongs_to_collection'] = belong_to_collection_list
        except:
            pass
            # print('scrapy_err:该{}电影未收藏'.format(movie_id))
        # print('主页', self.results)

    # 编辑页面爬取
    def movieedit(self, html, movie_id):
        selector = etree.HTML(html)
        # 爬取电影预算
        try:
            budget = selector.xpath('//input[@id="budget"]/@value')[0]
            self.results['budget'] = budget
        except:
            pass
            # print('scrapy_err:该{}电影没有电影预算'.format(movie_id))
        # 爬取电影主页
        try:
            homepage = selector.xpath('//input[@id="en_US_homepage"]/@value')[0]
            self.results['homepage'] = homepage
        except:
            pass
            # print('scrapy_err:该{}电影没有电影主页'.format(movie_id))
        # 爬取电影原始语言
        try:
            original_language = selector.xpath('//input[@id="iso_639_1"]/@value')[0]
            self.results['original_language'] = original_language
        except:
            pass
            # print('scrapy_err:该{}电影没有原始语言'.format(movie_id))
        # 爬取电影原始片名
        try:
            original_title = selector.xpath('//input[@id="title"]/@value')[0]
            self.results['original_title'] = original_title
        except:
            pass
            # print('scrapy_err:该{}电影没有原始片名'.format(movie_id))
        # 爬取电影概述
        try:
            overview = selector.xpath('//textarea[@id="en_US_overview"]/text()')[0]
            self.results['overview'] = overview
        except:
            pass
            # print('scrapy_err:该{}电影没有电影概述'.format(movie_id))
        # 爬取电影时长
        try:
            runtime = selector.xpath('//input[@id="en_US_runtime"]/@value')[0]
            self.results['runtime'] = runtime
        except:
            pass
            # print('scrapy_err:该{}电影没有电影时长'.format(movie_id))
        # 爬取电影状态(发行、谣传)
        try:
            status = selector.xpath('//option[@selected="selected"]/text()')[0]
            self.results['status'] = status
        except:
            pass
            # print('scrapy_err:该{}电影没有电影状态(发行、谣传)'.format(movie_id))
        # 爬取电影口号/宣传语
        try:
            tagline = selector.xpath('//input[@id="en_US_tagline"]/@value')[0]
            self.results['tagline'] = tagline
        except:
            pass
            # print('scrapy_err:该{}电影没有电影口号/宣传语'.format(movie_id))
        # 爬取电影英文片名
        try:
            if selector.xpath('//input[@id="en_US_translated_title"]/@value')[0]:
                title = selector.xpath('//input[@id="en_US_translated_title"]/@value')[0]
            else:
                title = original_title
            self.results['title'] = title
        except:
            pass
            # print('scrapy_err:该{}电影没有英文片名'.format(movie_id))
        # 爬取电影收入
        try:
            revenue = selector.xpath('//input[@id="revenue"]/@value')[0]
            self.results['revenue'] = revenue
        except:
            pass
            # print('scrapy_err:该{}电影没有电影收入'.format(movie_id))
        # 爬取发行语言
        try:
            # 取出发行语言所在模块
            collection = selector.xpath('//script[@type="text/javascript"]/text()')
            # 提取语言id，返回类型为字符串
            spoken_languages_str = re.findall(r"value: \[([\S|\s]*)\]", collection[0])[0].strip()
            # 通过都好分隔成列表，每一个代表一种发行语言
            each_language_list = spoken_languages_str.split(",")
            spoken_languages = []
            # 遍历每一种语言，提取语言id和名称，存入列表中
            for i in each_language_list:
                each_language_dic = {}
                iso_639_1 = i[14:16]
                name = re.findall("english_name\"[^{]*\"" + iso_639_1 + "\"", collection[0])[0].split(",")[0][15: -1]
                # 提取id
                each_language_dic['iso_639_1'] = iso_639_1
                # 提取名称
                each_language_dic['name'] = name
                spoken_languages.append(each_language_dic)
                self.results['spoken_languages'] = spoken_languages
        except:
            pass
            # print('scrapy_err:该{}电影没有发行语言'.format(movie_id))
        # print('编辑页面', self.results)

    # 删除列表jsonlist中不需要的属性
    def delete_attribute(self, jsonlist):
        del_list = []
        del_list_temp = ['url', 'logo_path', 'origin_country', 'bson_id']
        # 判断删除列表中的属性是否在jsonlist中
        for i in del_list_temp:
            if i in jsonlist[0]:
                del_list.append(i)
        for i in jsonlist:
            for j in del_list:
                del i[j]
        return jsonlist

    # 生产信息爬取
    def movieproduction_countries(self, html, movie_id):
        # 爬取发行国家
        try:
            selector = etree.HTML(html)
            # 取出发行国家所在模块
            collection = selector.xpath('//script[@type="text/javascript"]/text()')
            # 提取国家id，返回类型为字符串
            production_countries_str = re.findall(r"value: \[([\S|\s]*?)\]", collection[0])[0].strip()
            # 通过都好分隔成列表，每一个代表一个国家
            each_countries = production_countries_str.split(",")
            production_countries = []
            # 遍历每一个国家，提取国家id和名称，存入列表中
            for i in each_countries:
                each_countries_dic = {}
                # 提取id
                iso_3166_1 = i[9:11]
                # 提取国家名称
                name = re.findall("code\":\"" + iso_3166_1 + "\",\"english_name\":\"([^\"]*)", collection[0])[0]
                each_countries_dic['iso_3166_1'] = iso_3166_1
                each_countries_dic['name'] = name
                production_countries.append(each_countries_dic)
            self.results['production_countries'] = production_countries
        except:
            pass
            # print('scrapy_err:该{}电影没有发行国家'.format(movie_id))
        # print('发行信息页面', self.results)

    # imbd_id爬取
    def movieimdbid(self, html, movie_id):
        selector = etree.HTML(html)
        # 爬取电影imdb的id
        try:
            imdb_id = selector.xpath('//input[@id="imdb_id"]/@value')[0]
            self.results['imdb_id'] = imdb_id
        except:
            pass
            # print('scrapy_err:该{}电影没有电影imdb的id'.format(movie_id))
        # print('imdb页面', self.results)

    def retryGet(self, url, headers, timeout):
        # 重试次数
        retry_num = 0
        while 1:
            try:
                if retry_num < 3:
                    response = self.session.get(url, headers=headers, timeout=timeout)
                    return response
                else:
                    # 超过三次抛出异常
                    return 'retry_times_out'
            except:
                retry_num += 1


if __name__ == '__main__':
    start_all = time.time()
    # 条件选择
    option = ['popularity.desc', 'popularity.asc', 'vote_average.desc', 'vote_average.asc',
              'primary_release_date.desc',
              'primary_release_date.asc', 'title.asc', 'title.desc']
    # 爬虫初始化
    login = Login()
    # 爬虫登录和修改cookie
    login.login(email='3236207497', password='*z*622328**')
    for pagenum in range(2020, 2019, -1):
        csv_file = 'train' + str(pagenum) + '.csv'
        login.file_not_exit(csv_file=csv_file)
        # 获取headers
        headers = login.headers
        # 连接数据库
        conn = MongoClient('localhost', 27017)
        db = conn.tmdb  # 连接tmdb数据库，没有则自动创建
        ids = db.movie_id  # 使用表movie_id，没有则自动创建
        # 获取最大页数
        # max_page_url = 'https://www.themoviedb.org/discover/movie?language=en&list_style=1&media_type=movie&vote_count.gte=0'
        # primary_release_year = '&primary_release_year=' + str(pagenum)
        # sort_by = '&sort_by=' + option[0]
        # # max_page_url为最大页码获取的首页
        # max_page_url = max_page_url + sort_by + primary_release_year
        # # max_page_num存放最大页码
        # max_page_num = int(login.max_page(max_page_url=max_page_url, headers=headers))

        #################################################################
        # upcoming电影
        # 获取最大页数
        max_page_url = 'https://www.themoviedb.org/movie/upcoming'
        # max_page_url为最大页码获取的首页
        max_page_url = max_page_url
        # max_page_num存放最大页码
        max_page_num = int(login.max_page(max_page_url=max_page_url, headers=headers))
        ###################################################################

        # 判断数据库不为空则写入电影id，不然等待全部爬取完再写入，作用：避免爬虫中途死了，接下来的没运行
        # sep为一次爬取多少页
        sep = 10
        # 电影关键词列表
        Thread_list_q = []
        # 从第*页到第*页爬取
        for testnum in range(1, max_page_num, sep):
            # 数据库为空则存入id信息，不为空则直接执行搜索
            # if not ids.find_one():
            if 1:
                # 统计爬到第几个20部电影页面
                movie_20_count = 0
                # 获取第*到第*页的爬虫关键词
                # for i in range(testnum, testnum + sep):
                #     collection_url = 'https://www.themoviedb.org/discover/movie?language=en&list_style=1&media_type=movie&vote_count.gte=0'
                #     primary_release_year = '&primary_release_year=' + str(pagenum)
                #     page = '&page=' + str(i)
                #     sort_by = '&sort_by=' + option[0]
                #     collection_url = collection_url + page + sort_by + primary_release_year
                #     q = scrapy_url(collection_url=collection_url, movie_20=True, headers=headers, ids=ids)
                #     q.start()
                #     Thread_list_q.append(q)
                # upcoming电影
                for i in range(testnum, testnum + sep):
                    collection_url = 'https://www.themoviedb.org/movie/upcoming?'
                    page = '&page=' + str(i)
                    collection_url = collection_url + page
                    q = scrapy_url(collection_url=collection_url, movie_20=True, headers=headers, ids=ids)
                    q.start()
                    Thread_list_q.append(q)
            for j in Thread_list_q:
                j.join()
            ############################################################################
            num = 1
            success_num = 0
            # 遍历所有关键词，爬取关键词对应的信息
            start = time.time()
            Thread_list_p = []
            # 输出数据库数量
            print('一共要爬取', ids.count_documents({}), '个页面')
            idcursor = ids.find()
            for id in idcursor:
                id = id['movie_id']
                # print('进行到第{}个'.format(num))
                results = results = {'Keywords': '', 'Movie_id': '', 'backdrops_path': '', 'belongs_to_collection': '',
                                     'budget': 0,
                                     'cast': '', 'crew': '', 'genres': '', 'homepage': '', 'imdb_id': '',
                                     'original_language': '',
                                     'original_title': '', 'overview': '', 'popularity': '', 'poster_path': '',
                                     'production_companies': '', 'production_countries': '', 'release_date': '',
                                     'revenue': 10,
                                     'runtime': '', 'spoken_languages': '', 'status': '', 'tagline': '', 'title': '', }
                if 1:
                    # 分布式爬虫
                    p = Scrapy_test(results=results, movie_id=id, csv_file=csv_file, headers=headers, ids=ids)
                    p.start()
                    Thread_list_p.append(p)
                    num += 1
                else:
                    break
            for i in Thread_list_p:
                i.join()
            end = time.time()
            print('爬虫共运行了{}'.format(end - start))
            print('程序共运行了{}'.format(end - start_all))
            print('成功运行了{}个'.format(success_num))
            print('失败了{}个'.format(ids.count_documents({})))
            # 中场休息时间
            sleeptime = 2
            print('睡眠{}s,运行到了{}年第{}页一共{}页'.format(sleeptime, pagenum, testnum + sep, max_page_num))
            time.sleep(sleeptime)
