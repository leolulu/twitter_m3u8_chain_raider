import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import pymongo
import redis
from ruamel.yaml import YAML


class MongoUtil:
    DB_COL_MAPPING = {}
    DB_ADDRESS = None

    @classmethod
    def get_collection(cls, db_name, collection_name='data'):
        myclient = pymongo.MongoClient(MongoUtil.DB_ADDRESS)
        mycol = MongoUtil.DB_COL_MAPPING.get(db_name)
        if mycol:
            return mycol
        else:
            mydb = myclient[db_name]
            mycol = mydb[collection_name]
            MongoUtil.DB_COL_MAPPING.update({db_name: mycol})
            return mycol


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
        skey = 's_' + key
        return lkey, skey

    def non_rep_add(self, key, value):
        lkey, skey = self.get_inner_key(key)
        if self.client.sismember(skey, value):
            return False
        else:
            self.client.sadd(skey, value)
            self.client.rpush(lkey, value)
            return True

    def get_one_from_list(self, key):
        lkey, skey = self.get_inner_key(key)
        value = self.client.blpop(lkey, 1)  # TODO:remove timeout
        if value:
            value = value[1]
            self.client.srem(skey, value)
            return value


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
    EXECUTOR = ThreadPoolExecutor(2)

    @classmethod
    def ffmpeg_process_m3u8(cls, url, name):
        def do_process(command, name):
            print("开始下载：", name, "\n指令为：", command)
            subprocess.call(command, shell=True)
            print("下载完成：", name)

        if not os.path.exists(FFmpegUtil.DOWNLOAD_FOLDER):
            os.mkdir(FFmpegUtil.DOWNLOAD_FOLDER)
        command = FFmpegUtil.COMMAND_TEMPLATE.format(url, os.path.join(FFmpegUtil.DOWNLOAD_FOLDER, f'{name}.mp4'))
        FFmpegUtil.EXECUTOR.submit(do_process, command, name)


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
