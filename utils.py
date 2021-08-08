import os
from urllib.parse import urlparse
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pymongo


class MongoUtil:
    DB_COL_MAPPING = {}
    DB_ADDRESS = "mongodb://42.193.43.79:27017/"

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
