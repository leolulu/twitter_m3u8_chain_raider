import json
from time import sleep

from seleniumwire.undetected_chromedriver.v2 import Chrome, ChromeOptions

from constants import Constant
from utils import MongoUtil, M3u8Util


class TwitterReider:
    def __init__(self, init_url) -> None:
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
        self.driver.get(self.init_url)
        with open('cookies.txt', 'r', encoding='utf-8') as f:
            cookies = json.loads(f.read())
        for cookie in cookies:
            if 'sameSite' in cookie and cookie['sameSite'] == 'None':
                cookie['sameSite'] = 'Strict'
            self.driver.add_cookie(cookie)
        self.driver.refresh()

    def scroll_user_page(self, user_page_url):
        del self.driver.requests
        parsed_url_set = set()
        self.driver.get(user_page_url)
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
                url = M3u8Util.parse_url(request.url)
                if (
                    request.response
                    and 'm3u8' in url
                    and url not in parsed_url_set
                ):
                    parsed_url_set.add(url)
                    if MongoUtil.get_collection(Constant.PARSED_M3U8_URL).count_documents({Constant.URL: url}) == 0:
                        print(url, request.response.status_code, request.response.headers['Content-Type'])

        print("已经滚动到底部了...")


if __name__ == "__main__":
    init_url = 'https://twitter.com/stone62855987'
    t = TwitterReider(init_url)
    t.scroll_user_page(init_url)
