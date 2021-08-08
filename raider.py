import json
from time import sleep

from seleniumwire.undetected_chromedriver.v2 import Chrome, ChromeOptions

from constants import Constant
from utils import CommonUtil, FFmpegUtil, MongoUtil, M3u8Util


class TwitterReider:
    def __init__(self, init_url) -> None:
        self.if_cookie_loaded = False
        self.init_url = init_url
        self.init_driver()

    def __del__(self):
        self.driver.close()
        self.driver.quit()

    def init_driver(self):
        options = {
            'proxy': {
                'http': 'socks5://127.0.0.1:10808',
                'https': 'socks5://127.0.0.1:10808',
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        self.driver = Chrome(seleniumwire_options=options)

    def load_cookie(self):
        with open('cookies.txt', 'r', encoding='utf-8') as f:
            cookies = json.loads(f.read())
        for cookie in cookies:
            if 'sameSite' in cookie and cookie['sameSite'] == 'None':
                cookie['sameSite'] = 'Strict'
            self.driver.add_cookie(cookie)
        self.driver.refresh()
        self.if_cookie_loaded = True

    def raid_single_user(self, user_page_url):
        self.driver.requests.clear()
        parsed_url_set = set()
        self.driver.get(user_page_url)
        if not self.if_cookie_loaded:
            print("没有载入cookie，开始载入cookie...")
            self.load_cookie()
        sleep(5)

        SCROLL_PAUSE_TIME = 1.0
        get_scrollTop_command = "return document.documentElement.scrollTop"
        last_height = self.driver.execute_script(get_scrollTop_command)
        height = 0
        while True:
            height += 750
            self.driver.execute_script(f"window.scrollTo(0, {height})")
            sleep(SCROLL_PAUSE_TIME)
            new_height = self.driver.execute_script(get_scrollTop_command)
            print("当前滚动高度：", new_height)
            if new_height == last_height:
                break
            last_height = new_height
            for request in self.driver.requests:
                m3u8 = M3u8Util(request.url)
                if (
                    request.response
                    and 'm3u8' in m3u8.url
                    and m3u8.url not in parsed_url_set
                ):
                    parsed_url_set.add(m3u8.url)
                    if MongoUtil.get_collection(Constant.PARSED_M3U8_URL).count_documents({Constant.URL: m3u8.url}) == 0:
                        FFmpegUtil.ffmpeg_process_m3u8(
                            m3u8.url,
                            CommonUtil.get_user_name(user_page_url) + '_' + m3u8.name
                        )
        print("已经滚动到底部了...")


if __name__ == "__main__":
    # init_url = 'https://twitter.com/stone62855987'
    init_url = 'https://twitter.com/zzh1329825121'
    t = TwitterReider(init_url)
    t.raid_single_user(init_url)
