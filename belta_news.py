import random
import time
from datetime import datetime

import numpy as np
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

useragent = UserAgent()
options = webdriver.ChromeOptions()
options.add_argument(f"user_agent={useragent.random}")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--headless")
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                          options=options)


def error_decorator(func):
    def get_error(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"{func.__name__} provided an error {e}")
            return kwargs.get('default_res')

    return get_error


class NewsEvent:

    def __init__(self, link):
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                       options=options)
        self.driver.get(link)
        self.page_source = self.driver.page_source
        self.doc = BeautifulSoup(self.page_source, features="html.parser")
        self.news_name = self.get_news_name()
        self.news_text = self.get_news_text()
        self.picture = self.get_picture()
        self.news_dt = self.get_news_dt()
        self.driver.close()
        self.driver.quit()

    @error_decorator
    def get_news_name(self, default_res=''):
        res = self.doc.find(['div'], attrs={'class': 'content_margin'}).h1.text
        return res

    @error_decorator
    def get_news_text(self, default_res=''):
        res = ''
        a = self.doc.find(['div'], attrs={'class': 'js-mediator-article'}).find_all('p')
        for el in a:
            res += el.text + "\n"
        return res

    @error_decorator
    def get_picture(self, default_res=''):
        res = self.doc.find(['div'], attrs={'class': 'news_img_slide'}).find('source').get('srcset')
        return res

    @error_decorator
    def get_news_dt(self, default=''):
        dt = self.doc.find(['div'], attrs={'class': 'date_full'}).text


def update_rus_date(dt):
    months = {'января': 'January',
              'февраля': 'February',
              'марта': 'March',
              'апреля': 'April',
              'мая': 'May',
              'июня': 'June',
              'июля': 'July',
              'августа': 'August',
              'сентября': 'September',
              'октября': 'October',
              'ноября': 'November',
              'декабря': 'December'}
    new_dt = f"{dt.split(' ')[0]}/{months.get(dt.split(' ')[1])}/{dt.split(' ')[2]} {dt.split(' ')[3]}"
    return str(datetime.strptime(new_dt, '%d/%B/%Y, %H:%M'))


def get_all_links_for_parsing(url=['https://www.belta.by/culture/', 'https://www.belta.by/events/']):
    res_links = []
    for u in url:
        print(u)
        i = 1
        while True:
            try:
                print(f"Page {i}")
                link = f"{u}page/{i}/"
                time.sleep(np.random.randint(10, 15))
                driver.get(link)
                page_source = driver.page_source
                doc = BeautifulSoup(page_source, features="html.parser")
                doc1 = doc.find_all(attrs={'class': 'rubric_item_title'})
                if len(doc1) > 0:
                    for el in doc1:
                        res_links.append(f"https://www.belta.by{el['href']}")
                    i += 1
                else:
                    break

            except Exception as e:
                print(e)
                break
    return res_links


def collect_event_data(event):
    res = []
    try:
        event_date = update_rus_date(event.news_dt)
    except Exception as e:
        event_date = event.news_dt
        print(f"Error while converting datetimes {e}")
    res.append(
        {
            "news_name": event.news_name,
            "news_pic": event.picture,
            "news_text": event.news_text,
            "news_dt": event_date
        }
    )
    return res


def parse_event_by_link(url_event):
    print(url_event)
    time.sleep(random.randrange(20, 30))
    event = NewsEvent(url_event)
    res = collect_event_data(event)
    return res


def close_driver():
    driver.close()
    driver.quit()
