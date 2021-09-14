import json
import random
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from time import sleep

from seleniumwire.undetected_chromedriver.v2 import Chrome, ChromeOptions
from typing_extensions import runtime

from constants import Constant
from exceptions import NoFetishError, RestartBrowserWarning
from utils import CommonUtil, FFmpegUtil, M3u8Util, MongoUtil, RedisUtil, load_config


class TwitterReider:
    def __init__(self, init_url, high_res=False, do_download=True) -> None:
        self.high_res = high_res
        self.init_url = init_url
        self.do_download = do_download
        self.prepare()

    def __del__(self):
        self.destroy_web_driver()

    def destroy_web_driver(self):
        try:
            self.driver.close()
            self.driver.quit()
        except:
            print(f"销毁web dervier失败：{traceback.format_exc()}")

    def init_web_driver(self):
        options = {
            'proxy': {
                'http': 'socks5://127.0.0.1:10808',
                'https': 'socks5://127.0.0.1:10808',
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        oc = ChromeOptions()
        oc.add_argument('--log-level=3')
        # oc.add_argument('--headless')
        oc.add_argument('--disable-gpu')
        self.driver = Chrome(seleniumwire_options=options, options=oc)
        print('selenium driver已初始化...')

    def prepare(self):
        # manual switch
        self.use_taboos_url = True
        self.use_fetish_title = True
        # load config
        self.conf = load_config("config.yaml")
        print('配置已读取...')
        # init utils
        self.mongo_client = MongoUtil(self.conf['mongo']['db_addr'])
        print('mongo已连接...')
        self.redis_client = RedisUtil(self.conf['redis']['host'], self.conf['redis']['password'])
        print('redis已连接...')
        # init driver
        self.init_web_driver()
        # init variables
        self.user_urls_to_parse = set([self.init_url])
        self.user_urls_parsed = set()
        self.if_cookie_loaded = False
        # init downloader
        if self.do_download:
            worker_num = 2
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
        self.print_title("开始解析用户主页")

        self.scroll_wrapper(get_m3u8_url, parsed_url_set, user_page_url)
        print("已经滚动到底部了，开始获取关注列表...")

    def download_m3u8_loop_single(self):
        try:
            while True:
                url, name = self.redis_client.get_one_from_list(Constant.VIDEO_URL_TO_DOWNLOAD)
                print(f"\033[0;33;40m下载器取到记录，url：{url}，name：{name}\033[0m")
                FFmpegUtil.ffmpeg_process_m3u8(url, name)
        except:
            print(traceback.format_exc())

    def print_title(self, prompt):
        title = None
        attempts = 30
        while (not title) and attempts > 0:
            title = self.driver.title
            attempts -= 0
            sleep(1)
        print(f"{prompt}：{title}")

        if self.use_fetish_title and title:
            has_fetish = False
            for fetish in Constant.FETISHES_TITLE:
                if fetish in title:
                    has_fetish = True
                    break
            if not has_fetish:
                print(f"标题不含有fetish...")
                raise NoFetishError()

    def driver_execute_script(self, script):
        window_focus_command = "window.focus()"
        self.driver.execute_script(window_focus_command)
        return self.driver.execute_script(script)

    def scroll_wrapper(self, func, *args, **kwargs):
        SCROLL_PAUSE_TIME = 0.3
        get_scrollTop_command = "return document.documentElement.scrollTop"
        last_height = self.driver_execute_script(get_scrollTop_command)
        reach_end_times = 0
        while True:
            self.driver_execute_script(f"window.scrollTo(0, {last_height+750})")
            sleep(SCROLL_PAUSE_TIME)
            new_height = self.driver_execute_script(get_scrollTop_command)
            # print(f"[{datetime.now().strftime('%F %X')}]当前滚动高度：", new_height)    //TODO 测试用，展示屏蔽，测试完恢复
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
        self.print_title("开始解析用户好友")
        get_user_urls()

        self.scroll_wrapper(get_user_urls)
        print("已经滚动到底部了，关注列表获取完毕...")

    def skip_url_check(self, url: str) -> bool:
        if not self.use_taboos_url:
            return True
        url = url.lower()
        if url in self.user_urls_parsed:
            return True
        for taboo in Constant.TABOOS_URL:
            if taboo in url:
                return True
        return False

    def chief_dispatcher(self):
        runtime = 0
        url = None
        while self.user_urls_to_parse:
            try:
                url = random.sample(self.user_urls_to_parse, 1)[0]
                self.user_urls_to_parse.remove(url)
                if self.skip_url_check(url):
                    continue
                self.raid_single_user(url)
                self.get_following(url)
                self.user_urls_parsed.add(url)

                runtime += 1
                if runtime > 4:
                    raise RestartBrowserWarning("常规重启web deriver...")
            except NoFetishError:
                if url:
                    self.user_urls_parsed.add(url)
            except Exception as e:
                if isinstance(e, RestartBrowserWarning):
                    print(f"运行了一定时辰了，重启一下...")
                else:
                    print(f"浏览器好像挂掉了，重启：\n{traceback.format_exc()}")
                self.destroy_web_driver()
                runtime = 0
                self.if_cookie_loaded = False
                self.init_web_driver()


if __name__ == "__main__":
    init_url = 'https://twitter.com/mywife8888'
    t = TwitterReider(init_url, high_res=True, do_download=True)
    t.chief_dispatcher()
