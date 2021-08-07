# from seleniumwire import webdriver
from seleniumwire.undetected_chromedriver.v2 import Chrome, ChromeOptions
from time import sleep
import json


INIT_URL = 'https://twitter.com/stone62855987'

# load init page
options = {
    'proxy': {
        'http': 'socks5://127.0.0.1:10808',
        'https': 'socks5://127.0.0.1:10808',
        'no_proxy': 'localhost,127.0.0.1'
    }
}
driver = Chrome(seleniumwire_options=options)
driver.get(INIT_URL)
with open('cookies.txt', 'r', encoding='utf-8') as f:
    cookies = json.loads(f.read())
for cookie in cookies:
    if 'sameSite' in cookie and cookie['sameSite'] == 'None':
        cookie['sameSite'] = 'Strict'
    driver.add_cookie(cookie)
driver.refresh()


parsed_url_set = set()

SCROLL_PAUSE_TIME = 1.0
get_scrollTop_command = "return document.documentElement.scrollTop"
last_height = driver.execute_script(get_scrollTop_command)
height = 0
while True:
    height += 750
    driver.execute_script(f"window.scrollTo(0, {height})")
    sleep(SCROLL_PAUSE_TIME)
    new_height = driver.execute_script(get_scrollTop_command)
    print("当前滚动高度：", new_height)
    if new_height == last_height:
        break
    last_height = new_height
    for request in driver.requests:
        if request.response and 'm3u8' in request.url and request.url not in parsed_url_set:
            parsed_url_set.add(request.url)
            print(request.url, request.response.status_code, request.response.headers['Content-Type'])


print("已经滚动到底部了...")

driver.close()
driver.quit()
