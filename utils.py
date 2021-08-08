import pymongo
from urllib.parse import urlparse


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
    @classmethod
    def parse_url(cls, url):
        p = urlparse(url)
        return "{}://{}{}".format(p.scheme, p.netloc, p.path)

class FFmpegUtil:
    pass