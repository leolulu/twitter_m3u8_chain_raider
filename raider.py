import json
import random
import traceback
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from seleniumwire.undetected_chromedriver.v2 import Chrome, ChromeOptions

from constants import Constant
from utils import CommonUtil, FFmpegUtil, M3u8Util, MongoUtil, RedisUtil, load_config


class TwitterReider:
    def __init__(self, init_url, high_res=False, do_download=True) -> None:
        self.high_res = high_res
        self.init_url = init_url
        self.do_download = do_download
        self.prepare()

    def __del__(self):
        self.driver.close()
        self.driver.quit()

    def prepare(self):
        # load config
        self.conf = load_config("config.yaml")
        print('配置已读取...')
        # init utils
        self.mongo_client = MongoUtil(self.conf['mongo']['db_addr'])
        print('mongo已连接...')
        self.redis_client = RedisUtil(self.conf['redis']['host'], self.conf['redis']['password'])
        print('redis已连接...')
        # init driver
        options = {
            'proxy': {
                'http': 'socks5://127.0.0.1:10808',
                'https': 'socks5://127.0.0.1:10808',
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        oc = ChromeOptions()
        oc.add_argument('--log-level=3')
        oc.add_argument('--headless')
        oc.add_argument('--disable-gpu')
        self.driver = Chrome(seleniumwire_options=options, options=oc)
        self.driver.set_page_load_timeout(60)
        print('selenium driver已初始化...')
        # init variables
        self.user_urls_to_parse = set([self.init_url])
        self.user_urls_parsed = set()
        self.if_cookie_loaded = False
        # init downloader
        if self.do_download:
            worker_num = 3
            executor = ThreadPoolExecutor(worker_num)
            for _ in range(worker_num):
                executor.submit(self.download_m3u8_loop_single)
            print('下载器已初始化...')

    def load_cookie(self):
        if not self.if_cookie_loaded:
            print("没有载入cookie，开始载入cookie...")
            with open('cookies.txt', 'r', encoding='utf-8') as f:
                cookies = json.loads(f.read())
            for cookie in cookies:
                if 'sameSite' in cookie and cookie['sameSite'] == 'None':
                    cookie['sameSite'] = 'Strict'
                self.driver.add_cookie(cookie)
            self.driver.refresh()
            self.if_cookie_loaded = True

    def raid_single_user(self, user_page_url):
        def get_m3u8_url(parsed_url_set, user_page_url):
            for request in self.driver.requests:
                m3u8 = M3u8Util(request.url)
                if (
                    request.response
                    and 'm3u8' in m3u8.url
                    and m3u8.url not in parsed_url_set
                ):
                    parsed_url_set.add(m3u8.url)
                    if self.mongo_client.get_collection(Constant.PARSED_M3U8_URL).count_documents({Constant.URL: m3u8.url}) == 0:
                        if (self.high_res and Constant.TAG_SIG in request.url) or ((not self.high_res) and (Constant.TAG_SIG not in request.url)):
                            self.redis_client.non_rep_add(
                                Constant.VIDEO_URL_TO_DOWNLOAD,
                                m3u8.url,
                                CommonUtil.get_user_name(user_page_url) + '_' + m3u8.name
                            )
                        self.mongo_client.get_collection(Constant.PARSED_M3U8_URL).insert_one({Constant.URL: m3u8.url})

        self.driver.requests.clear()
        parsed_url_set = set()
        self.driver.get(user_page_url)
        self.load_cookie()
        sleep(5)

        self.scroll_wrapper(get_m3u8_url, parsed_url_set, user_page_url)
        print("已经滚动到底部了，开始获取关注列表...")

    def download_m3u8_loop_single(self):
        try:
            while True:
                url, name = self.redis_client.get_one_from_list(Constant.VIDEO_URL_TO_DOWNLOAD)
                print(f"下载器取到记录，url：{url}，name：{name}")
                FFmpegUtil.ffmpeg_process_m3u8(url, name)
        except:
            print(traceback.format_exc())

    def scroll_wrapper(self, func, *args, **kwargs):
        SCROLL_PAUSE_TIME = 2.0
        get_scrollTop_command = "return document.documentElement.scrollTop"
        last_height = self.driver.execute_script(get_scrollTop_command)
        height = 0
        reach_end_times = 0
        while True:
            height += 750
            self.driver.execute_script(f"window.scrollTo(0, {height})")
            sleep(SCROLL_PAUSE_TIME)
            new_height = self.driver.execute_script(get_scrollTop_command)
            print("当前滚动高度：", new_height)
            if new_height == last_height:
                reach_end_times += 1
                if reach_end_times > 20:
                    break
            else:
                last_height = new_height
                func(*args, **kwargs)

    def get_following(self, user_page_url):
        def get_user_urls():
            for a_obj in self.driver.find_elements_by_xpath("//section[@role='region']//div[@role='button']/div/div[1]//a[@role='link']"):
                self.user_urls_to_parse.add(a_obj.get_attribute('href'))

        self.driver.get(CommonUtil.get_following_url(user_page_url))
        self.load_cookie()
        get_user_urls()

        self.scroll_wrapper(get_user_urls)
        print("已经滚动到底部了，关注列表获取完毕...")

    def chief_dispatcher(self):
        while self.user_urls_to_parse:
            url = random.sample(self.user_urls_to_parse, 1)[0]
            self.user_urls_to_parse.remove(url)
            if url in self.user_urls_parsed:
                continue
            self.raid_single_user(url)
            self.get_following(url)
            self.user_urls_parsed.add(url)


if __name__ == "__main__":
    init_url = 'https://twitter.com/renjianzhaoze'
    t = TwitterReider(init_url, high_res=True, do_download=True)
    t.chief_dispatcher()
