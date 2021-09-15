import os
import subprocess
from collections import defaultdict
from typing import Dict, Optional, Set
from urllib.parse import urlparse

import pymongo
import redis
from pymongo.collection import Collection
from retrying import retry
from ruamel.yaml import YAML

from constants import Constant


class MongoUtil:
    DB_COL_MAPPING = {}

    def __init__(self, db_addr):
        self.client = pymongo.MongoClient(db_addr)

    def get_collection(self, db_name, collection_name='data'):
        mycol = MongoUtil.DB_COL_MAPPING.get(db_name)
        if mycol:
            return mycol
        else:
            mydb = self.client[db_name]
            mycol = mydb[collection_name]
            mycol_inst = MongoColUtil(mycol)
            MongoUtil.DB_COL_MAPPING.update({db_name: mycol_inst})
            return mycol_inst


class MongoColUtil:
    def __init__(self, col: Collection):
        self.col = col

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def count_documents(self, *args, **kwargs):
        return self.col.count_documents(*args, **kwargs)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def insert_one(self, *args, **kwargs):
        return self.col.insert_one(*args, **kwargs)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def update_one(self, *args, **kwargs):
        return self.col.update_one(*args, **kwargs)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def find_one(self, *args, **kwargs):
        return self.col.find_one(*args, **kwargs)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def delete_one(self, *args, **kwargs):
        return self.col.delete_one(*args, **kwargs)


class UrlLayeredDistributer:
    def __init__(self, client: MongoUtil, init_url: Optional[str] = None) -> None:
        self.col_usr_acc_rec = client.get_collection(Constant.USER_ACCESS_RECORD)
        self.mongo_util = client
        if init_url:
            self.unique_insert_one_url(0, init_url)

    def deposit(self,  url):
        query_result = self.col_usr_acc_rec.find_one({Constant.URL: url})
        if query_result is None:
            self.unique_insert_one_url(0, url)
        else:
            visit_num = query_result[Constant.VISIT]
            self.unique_insert_one_url(visit_num, url)

    def withdraw(self) -> str:
        url = None
        for db_name in sorted([i for i in self.mongo_util.client.list_database_names() if i.startswith(Constant.DB_NAME_PREFIX)]):
            col = self.mongo_util.get_collection(db_name)
            if col.count_documents({}) > 0:
                url = col.find_one()[Constant.URL]
                col.delete_one({Constant.URL: url})
                break

        if url is None:
            print("不可能的情况出现了！！！，取url竟然取到了None！！！")
            exit()
        else:
            self.printout()
            return url

    def settle(self, url):
        self.col_usr_acc_rec.update_one({Constant.URL: url}, {'$inc': {Constant.VISIT: 1}}, upsert=True)
        self.printout()

    def get_db_name(self, visit_num: int) -> str:
        return f'{Constant.DB_NAME_PREFIX}-{visit_num}'

    def unique_insert_one_url(self, visit_num: int, url):
        self.mongo_util.get_collection(
            self.get_db_name(visit_num)
        ).update_one(
            {Constant.URL: url},
            {'$set': {Constant.URL: url}},
            True
        )

    @property
    def nonempty(self) -> bool:
        nonempty = False
        for db_name in [i for i in self.mongo_util.client.list_database_names() if i.startswith(Constant.DB_NAME_PREFIX)]:
            if self.mongo_util.get_collection(db_name).count_documents({}) > 0:
                nonempty = True
                break
        return nonempty

    def printout(self):
        msg = 'url_stack: '
        for db_name in sorted([i for i in self.mongo_util.client.list_database_names() if i.startswith(Constant.DB_NAME_PREFIX)]):
            num = self.mongo_util.get_collection(db_name).count_documents({})
            db_name = db_name.split('-')[-1]
            if num > 0:
                msg += f"[{db_name}]*{num}, "
        print(msg)


class RedisUtil:
    def __init__(self, host, password):
        pool = redis.ConnectionPool(
            host=host,
            port=6379,
            decode_responses=True,
            max_connections=10,
            password=password)
        self.client = redis.StrictRedis(connection_pool=pool)

    def get_inner_key(self, key):
        lkey = 'l_' + key
        hkey = 'h_' + key
        return lkey, hkey

    def non_rep_add(self, key, url, value):
        lkey, hkey = self.get_inner_key(key)
        if self.client.hexists(hkey, url):
            return False
        else:
            self.client.hset(hkey, url, value)
            self.client.rpush(lkey, url)
            return True

    def get_one_from_list(self, key):
        lkey, hkey = self.get_inner_key(key)
        url_info = self.client.blpop(lkey, 0)
        url = url_info[1]
        value = self.client.hget(hkey, url)
        self.client.hdel(hkey, url)
        return url, value


class M3u8Util:
    def __init__(self, url) -> None:
        self.url = self.parse_url(url)
        self.name = self.parse_name(self.url)

    def parse_name(self, url):
        return os.path.splitext(os.path.basename(url))[0]

    def parse_url(self, url):
        p = urlparse(url)
        return "{}://{}{}".format(p.scheme, p.netloc, p.path)


class FFmpegUtil:
    COMMAND_TEMPLATE = 'ffmpeg -http_proxy http://127.0.0.1:10809/ -i "{}" -c copy -y "{}"'
    DOWNLOAD_FOLDER = './download'

    @classmethod
    def ffmpeg_process_m3u8(cls, url, name):
        f_stdout, f_stderr = open('./stdout.log', 'w'), open('./stderr.log', 'w')
        if not os.path.exists(FFmpegUtil.DOWNLOAD_FOLDER):
            os.mkdir(FFmpegUtil.DOWNLOAD_FOLDER)
        command = FFmpegUtil.COMMAND_TEMPLATE.format(url, os.path.join(FFmpegUtil.DOWNLOAD_FOLDER, f'{name}.mp4'))
        print(f"\033[0;31;40m开始下载：{name}\n指令为：{command}\033[0m")
        exit_code = subprocess.call(command, shell=True, stdout=f_stdout, stderr=f_stderr)
        print(f"\033[0;32;40m下载完成，状态码[{exit_code}]: {name}\033[0m")


class CommonUtil:
    @classmethod
    def get_user_name(cls, url):
        if url[-1] == '/':
            url = url[:-1]
        return url.split('/')[-1]

    @classmethod
    def get_following_url(cls, url):
        if url[-1] == '/':
            url = url[:-1]
        return url + '/following'


def load_config(config_path):
    yaml = YAML(typ='safe')
    with open(config_path, 'r', encoding='utf-8') as f:
        conf = yaml.load(f)
    return conf
