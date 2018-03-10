#!/usr/bin/python
# -*- coding: utf-8 -*-

'freebuf spider'

import requests
from bs4 import BeautifulSoup
import re
import queue
import threading
import argparse
import logging
import sqlite3
import time


class Spider(threading.Thread):
    def __init__(self, queue_links, lock, key):
        threading.Thread.__init__(self)

        self.queue_links = queue_links
        self.key = key
        self.lock = lock
        self.deep = None
        self.link = None
        self.headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/64.0.3282.186 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
                                  'image/webp,image/apng,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8'}

        self.setDaemon(True)
        self.start()

    # 自动运行的run函数
    def run(self):
        while True:
            try:
                self.link, self.deep = self.queue_links.get()

            except self.queue_links.Empty:
                continue

            if self.deep > 0:
                self.deep -= 1
                links = self.get_links()

                if links:
                    for i in links:
                        if i not in curls:
                            with self.lock:
                                curls.add(i)
                            self.queue_links.put((i, self.deep))

                self.queue_links.put((self.link, 0))

            else:
                # 存储数据
                self.save_to_db()

            self.queue_links.task_done()

    def get_links(self):
        # 当前页面去重链接集合
        pages = set()
        links = []
        logger.debug('当前爬取页面为' + self.link)
        r = requests.get(self.link, headers=self.headers, timeout=5)
        bsobj = BeautifulSoup(r.text, 'lxml')
        # 只要http开头的链接
        for link in bsobj.findAll("a", href=re.compile("^(http|https)://")):
            if 'href' in link.attrs:
                url = link.attrs['href']
                # 初步处理，转换/./ 为/
                nurl = url.replace('/./', '/')
                # 去除最后的/
                if nurl[-1] == '/':
                    nurl = nurl[:-1]
                # 按/ 划分开
                rep = nurl.replace('//', '').split('/')
                logger.debug(type(rep))
                logger.debug(rep)
                # 符合页面规则
                if 'freebuf.com' in rep[0] and 'oauth' not in rep[-1]:
                    # 碰到新页面
                    if nurl not in pages:
                        pages.add(nurl)
                        links.append(nurl)

        return links

    def get_content(self):
        try:
            r = requests.get(self.link, headers=self.headers, timeout=5)
            soup = BeautifulSoup(r.text, 'lxml')
            # 压缩内容
            title = soup.title.get_text("|", strip=True)
            content = soup.get_text("|", strip=True)

        except:
            with self.lock:
                cfail_urls.add(self.link)
            logging.warning(self.link + '爬取失败')
            return

        if self.key:
            if re.search(self.key, content):
                with self.lock:
                    key_urls.add(self.link)
                return (self.link, title, content)

        else:
            return (self.link, title, content)

    def save_to_db(self):

        data = self.get_content()

        if not data:
            return

        conn = sqlite3.connect(dbfile)
        cursor = conn.cursor()

        if not self.key:
            table_name = 'normal'
        else:
            table_name = self.key

        sql = 'insert into ' + table_name + ' values (?,?,?)'

        try:
            cursor.execute(sql, (data[0], data[1], data[2]))
            with self.lock:
                saved_urls.add(self.link)
            logger.debug('插入成功')

        except:
            with self.lock:
                unsaved_urls.add(self.link)
            logger.debug('插入错误')
        finally:
            cursor.close()
            conn.commit()
            conn.close()


class ThreadPool:

    def __init__(self, num, key, lock):
        self.num = num
        self.threads = []
        self.queue = queue.Queue()
        self.key = key
        self.lock = lock

        self.create_thread()

    def create_thread(self):
        for i in range(self.num):
            self.threads.append(Spider(self.queue, self.lock, self.key))

    def put_job(self, job):  # job (link,deep)
        self.queue.put(job)
        self.key = key

    def get_queue(self):
        return self.queue

    def wait(self):
        self.queue.join()


def show_progress():

    while True:
        # 有关键字，任务url为匹配到的url
        if key:
            logger.debug('有关键字' + key)
            urls = len(key_urls)
        else:
            urls = len(curls)

        logger.debug('已经发现的链接' + str(len(curls)))
        logger.debug('需存取任务链接' + str(urls))
        logger.debug('爬取失败的链接' + str(len(cfail_urls)))
        logger.debug('已经存取的链接' + str(len(saved_urls)))
        logger.debug('存取失败的链接' + str(len(unsaved_urls)))

        if key:
            logger.info('关键字页面匹配中……')
            logger.info('已处理的页面：' + str((len(saved_urls) + len(unsaved_urls))))
        else:
            if urls == 0:
                logger.info('链接爬取中……')
                continue
            logger.info('程序运行中……')
            logger.info('当前完成度' + str((len(saved_urls) + len(unsaved_urls)) / urls * 100) + '%')
        time.sleep(10)


def usage():
    parser = argparse.ArgumentParser(description="多线程爬虫 FOR FREEBUF", add_help=True)
    parser.add_argument('-u', metavar='url', default='http://www.freebuf.com', help='入口url，默认为http://www.freebuf.com')
    parser.add_argument('-d', metavar='deep', type=int, default=1, help='爬取深度, 默认为1')
    parser.add_argument('-f', metavar='spider.log', default='spider.log', help='指定logfile文件，默认为当前目录spider.log')
    parser.add_argument('-l', type=int, choices=[1, 2, 3, 4, 5], default=5, help='日志等级，默认为5，越大越详细')
    parser.add_argument('--testself', action='store_true', help='可选参数，程序自测')
    parser.add_argument('--thread', metavar='number', type=int, default=10, help='并发线程数量,默认为10')
    parser.add_argument('--dbfile', metavar='spider.db', default='spider.db', help='指定数据库文件，默认为当前目录spider.db')
    parser.add_argument('--key', metavar='value', help='关键字，默认None，即无关键字完全匹配')

    args = parser.parse_args()

    return args


def init_db(key):

    if not key:
        key = 'normal'


    # 数据表是否存在flag
    flag = 0

    logger.debug(key)

    conn = sqlite3.connect(dbfile)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM sqlite_master WHERE type='table'")

    c = cursor.fetchall()

    logger.debug('数据库中表的数量：' + str(len(c)))

    for table in c:
        # 不存在数据表key
        if key not in table:
            flag += 1

    if flag == len(c):
        sql = 'create table ' + key + ' (url text primary key, title text, content text)'
        try:
            cursor.execute(sql)
            logger.info('创建数据表成功')

        except sqlite3.OperationalError:
            logger.error('创建数据表失败')
    else:
            logger.info('数据表已存在')

    cursor.close()
    conn.commit()
    conn.close()


def init_log(logfile, level):
    logger = logging.getLogger()
    # 控制台 handler
    shandler = logging.StreamHandler()
    shandler.setLevel(logging.INFO)
    # 文件 handler
    fhandler = logging.FileHandler(logfile)
    # 控制日志文件记录等级
    fhandler.setLevel((6 - level) * 10)

    formatter = logging.Formatter(
        '%(asctime)s %(threadName)s %(levelname)-8s %(message)s')

    sformatter = logging.Formatter(
        '%(asctime)s %(message)s')

    shandler.setFormatter(sformatter)
    fhandler.setFormatter(formatter)

    logger.addHandler(shandler)
    logger.addHandler(fhandler)

    # 最小记录等级
    logger.setLevel(logging.DEBUG)

    return logger


def test_self(key):

    if not key:
        key = 'normal'

    conn = sqlite3.connect(dbfile)
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM ' + key)

    # 数据表中记录
    list = cursor.fetchall()

    # 传入set比较大小
    if len(set(list)) == len(list):
        logger.info('数据表中无重复链接')
    else:
        logger.info('数据表中有重复链接')

    cursor.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':

    # 初始化选项菜单
    args = usage()
    # 命令行选项
    link = args.u
    key = args.key
    deep = args.d
    thread_num = args.thread
    loglevel = args.l
    dbfile = args.dbfile
    logfile = args.f

    # 初始化日志记录器
    logger = init_log(logfile, loglevel)

    # 开始爬取
    logger.info('开始爬取')
    logger.info('开始地址：' + link)
    logger.info('指定深度：' + str(deep))
    logger.info('线程池：' + str(thread_num))
    logger.info('数据库：' + dbfile)
    logger.info('日志: ' + logfile + ' 等级：' + str(loglevel))
    if key:
        logger.info('关键字：' + key)

    #初始化数据库
    init_db(key)

    # 加锁确保共享变量线程安全
    lock = threading.Lock()

    # 采集到的全站url
    curls = set()
    # 匹配到关键字的url
    key_urls = set()
    # 已经保存过得url
    saved_urls = set()
    # 爬取失败的url
    cfail_urls = set()
    # 保存失败的url
    unsaved_urls = set()

    # 存入起点url
    curls.add(link)

    # 新建线程池
    pool = ThreadPool(thread_num, key, lock)
    # 放入入口链接和层深
    pool.put_job((link, deep))

    # 进度信息线程
    t = threading.Timer(10, show_progress)
    # 主线程结束子线程也结束
    t.setDaemon(True)
    t.start()

    # 采集过程中
    pool.wait()

    # 倘若此时子线程尚未结束，结束他
    t.cancel()

    for u in cfail_urls:
        logger.warning('爬取失败的链接：' + u)

    for u in unsaved_urls:
        logger.warning('保存出错的链接：' + u)

    logger.info('采集完成' + time.ctime())

    if args.testself:
        test_self(key)







