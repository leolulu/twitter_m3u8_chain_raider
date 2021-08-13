import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import pymongo
import redis
from retrying import retry
from ruamel.yaml import YAML


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
    def __init__(self, col):
        self.col = col

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def count_documents(self, *args, **kwargs):
        return self.col.count_documents(*args, **kwargs)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def insert_one(self, *args, **kwargs):
        return self.col.insert_one(*args, **kwargs)


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
            self.client.rpush(lkey, url)
            # 调试用
            print('hkey加入', url, value)
            ########
            self.client.hset(hkey, url, value)
            return True

    def get_one_from_list(self, key):
        lkey, hkey = self.get_inner_key(key)
        url_info = self.client.blpop(lkey, 0)  # TODO:remove timeout
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
        if not os.path.exists(FFmpegUtil.DOWNLOAD_FOLDER):
            os.mkdir(FFmpegUtil.DOWNLOAD_FOLDER)
        command = FFmpegUtil.COMMAND_TEMPLATE.format(url, os.path.join(FFmpegUtil.DOWNLOAD_FOLDER, f'{name}.mp4'))
        print("开始下载：", name, "\n指令为：", command)
        subprocess.call(command, shell=True)
        print("下载完成：", name)


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


class DriverUtil:
    pass


def load_config(config_path):
    yaml = YAML(typ='safe')
    with open(config_path, 'r', encoding='utf-8') as f:
        conf = yaml.load(f)
    return conf
