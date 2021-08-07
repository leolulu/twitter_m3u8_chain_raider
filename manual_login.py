# from seleniumwire import webdriver
from seleniumwire.undetected_chromedriver.v2 import Chrome, ChromeOptions
from time import sleep
import json


options = {
    'proxy': {
        'http': 'socks5://127.0.0.1:10808',
        'https': 'socks5://127.0.0.1:10808',
        'no_proxy': 'localhost,127.0.0.1'
    }
}
driver = Chrome(seleniumwire_options=options)

driver.get('https://twitter.com/login')


sleep(30)


cookies = driver.get_cookies()
print(cookies)

with open('cookies.txt', 'w', encoding='utf-8') as f:
    f.write(json.dumps(cookies))

driver.close()
driver.quit()
