from datetime import datetime
import random
import re
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

useragent = UserAgent()
options = webdriver.ChromeOptions()
options.add_argument(f"user_agent={useragent.random}")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--headless")
# options.add_argument('--window-size=3840,2160')
options.add_argument('--window-size=7680,4320')
options.add_argument("--disable-gpu")
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


class Event:
    def __init__(self, url):
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                       options=options)
        self.driver.get(url)
        self.page_source = self.driver.page_source
        self.doc = BeautifulSoup(self.page_source, features="html.parser")
        self.event_name = self.get_event_name()
        self.event_pic = self.get_event_pic()
        self.content_panel = self.get_content_panel()
        self.event_datetime = self.get_event_datetime()
        self.event_place = self.get_event_place()
        self.event_price = self.get_event_price()
        self.event_purchase_link = self.get_event_purchase_link()
        self.event_desc = self.get_event_desc()
        self.event_location = self.get_event_location()
        self.event_long = self.get_event_long()
        self.event_lat = self.get_event_lat()
        self.datetime_list = []
        self.event_name_list = []
        self.price_list = []
        self.purchase_link_list = []
        self.helper_dt_list = []
        self.long_schedule = False
        if self.if_long_schedule():
            self.long_schedule = True
            self.schedule_data = self.get_schedule_data()
            self.datetime_list.extend(self.get_datetime_list())
            self.event_name_list.extend(self.get_event_name_list())
            self.price_list.extend(self.get_price_list())
            self.purchase_link_list.extend(self.get_purchase_link_list())
            self.helper_dt_list.extend(self.get_helper_dt_list())
        self.driver.close()
        self.driver.quit()

    @error_decorator
    def get_event_name(self, default_res=''):
        return self.doc.find('div', attrs={'class': 'title'}).text.strip()

    @error_decorator
    def get_event_pic(self, default_res=''):
        return f"https://www.ticketpro.by{self.doc.find(['div'], attrs={'class': 'content-poster'}).img['src']}"

    @error_decorator
    def get_content_panel(self, default_res=[]):
        return self.doc.find_all(['span'], attrs={'class': 'content__panel-title'})

    @error_decorator
    def get_event_datetime(self, default_res=''):
        return self.content_panel[0].text.strip()

    @error_decorator
    def get_event_place(self, default_res=''):
        return self.content_panel[1].a.text.strip()

    @error_decorator
    def get_event_price(self, default_res=''):
        return self.content_panel[2].text.strip()

    @error_decorator
    def if_long_schedule(self, default_res=False):
        if self.doc.find_all(text='Расписание мероприятий'):
            return True

    @error_decorator
    def get_event_purchase_link(self, default_res=''):
        return f"https://www.ticketpro.by{self.doc.find(['a'], attrs={'class': 'btn btn-lg'})['href']}"

    @error_decorator
    def get_event_desc(self, default_res=''):
        return self.doc.find(['div'], attrs={'class': 'content__text'}).text.strip()

    @error_decorator
    def get_event_location(self, default_res=''):
        res = self.doc.find_all(['div'], attrs={'class': 'sidebar-box__text'})[0].text.strip().split('\n')
        return ", ".join([el for el in res if el != ''])

    @error_decorator
    def get_event_long(self, default_res=''):
        return self.doc.find_all(['div'], attrs={'class': 'map'})[0]['data-lan']

    @error_decorator
    def get_event_lat(self, default_res=''):
        return self.doc.find_all(['div'], attrs={'class': 'map'})[0]['data-lat']

    @error_decorator
    def get_schedule_data(self, default_res=([], [], [], [])):
        datetime_l = []
        ev_name_l = []
        price_l = []
        purch_l = []
        div = self.doc.find_all('div', attrs={'class': 'event-group__box'})
        for el in div:
            ev_name_l.append(el.find(['div'], class_='event-group__box-col event-group__box-name').text.strip())
            datetime_l.append(el.find(['div'], attrs={
                'class': 'event-group__box-col event-group__box-date'}).text.strip().replace(
                "\n", ", "))
            price_l.append(el.find(['div'], class_='event-group__box-col event-group__box-price').text.strip())
            purch_l.append(
                f"https://www.ticketpro.by{el.find(['div'], class_='event-group__box-col event-group__box-action').a['href']}")
        datetime_l_chb, ev_name_l_chb, price_l_chb, purch_l_chb = self.get_checkbox_data()
        datetime_l.extend(datetime_l_chb)
        ev_name_l.extend(ev_name_l_chb)
        price_l.extend(price_l_chb)
        purch_l.extend(purch_l_chb)
        helper_dt = []
        for el in self.doc.find_all(class_='checkbox'):
            helper_dt.append(el.find(class_='event-group__date-label').b.text)
        return datetime_l, ev_name_l, price_l, purch_l, helper_dt

    def get_checkbox_data(self):
        datetime_l = []
        ev_name_l = []
        price_l = []
        purch_l = []
        chb_i = 2
        while True:
            checkbox_xpath = f'/html/body/div[2]/main/main/div/div[2]/div[2]/div[1]/div[2]/div[1]/div/div/div[{chb_i}]/div/div'
            chb_i += 1
            try:
                time.sleep(random.randrange(15))
                self.driver.find_element(by=By.XPATH, value=checkbox_xpath).click()
                time.sleep(random.randrange(25))
                doc = BeautifulSoup(self.driver.page_source, features="html.parser")
                div = doc.find_all('div', attrs={'class': 'event-group__box'})
                for el in div:
                    ev_name_l.append(el.find(['div'],
                                             class_='event-group__box-col event-group__box-name').text.strip())
                    datetime_l.append(
                        el.find(['div'], attrs={
                            'class': 'event-group__box-col event-group__box-date'}).text.strip().replace(
                            "\n", ", "))
                    price_l.append(el.find(['div'],
                                           class_='event-group__box-col event-group__box-price').text.strip())
                    purch_l.append(
                        f"https://www.ticketpro.by{el.find(['div'], class_='event-group__box-col event-group__box-action').a['href']}")
            except Exception as e:
                print(f"get_checkbox_data {e}")
                break
        return datetime_l, ev_name_l, price_l, purch_l

    @error_decorator
    def get_datetime_list(self, default_res=''):
        return self.schedule_data[0]

    @error_decorator
    def get_event_name_list(self, default_res=''):
        return self.schedule_data[1]

    @error_decorator
    def get_price_list(self, default_res=''):
        return self.schedule_data[2]

    @error_decorator
    def get_purchase_link_list(self, default_res=''):
        return self.schedule_data[3]

    @error_decorator
    def get_helper_dt_list(self, default_res=''):
        return self.schedule_data[4]


def get_main_links(url):
    driver.get(url)
    doc_main = BeautifulSoup(driver.page_source, features="html.parser")
    div = doc_main.find(class_='header__menu')
    main_links = div.find_all(['a'], attrs={'class': ''})
    res = set([f"https://www.ticketpro.by{el['href']}" for el in main_links])
    return list(res)


def get_event_links(url_event_group, pages):
    event_list = []
    for i in range(1, int(pages) + 2):
        url_event_group_p = f"{url_event_group}?page={i}"
        time.sleep(random.randrange(25, 35))
        driver.get(url_event_group_p)
        doc_event_group = BeautifulSoup(driver.page_source, features="html.parser")
        try:
            div = doc_event_group.find(class_='event-other')
            event_list.extend(div.find_all(class_='event-box__head'))
        except Exception as e:
            print(f"get_event_links {e}")
    return event_list


def get_event_group_links(main_links):
    event_list = []
    for url_event_group in main_links:
        time.sleep(random.randrange(25, 35))
        driver.get(url_event_group)
        doc_event_group = BeautifulSoup(driver.page_source, features="html.parser")
        try:
            div = doc_event_group.find(class_='pagination text-center')
            pages = div.find(['a'], attrs={'class': 'page-last'})['data-page']
        except Exception as e:
            print(f"get_event_group_links {e}, seems it's only 1 page of events here")
            pages = 0
        finally:
            event_list.extend(get_event_links(url_event_group, pages))
    return list(set(event_list))


def update_rus_date(dt):
    months = {'Января': 'January',
              'Февраля': 'February',
              'Марта': 'March',
              'Апреля': 'April',
              'Мая': 'May',
              'Июня': 'June',
              'Июля': 'July',
              'Августа': 'August',
              'Сентября': 'September',
              'Октября': 'October',
              'Ноября': 'November',
              'Декабря': 'December'}
    new_dt = f"{dt.split(' ')[0]}/{months.get(dt.split(' ')[1])}/{datetime.now().year}"
    return str(datetime.strptime(new_dt, '%d/%B/%Y'))


def prettify_string(s):
    s = s.replace('\n', '')
    s = s.replace('\t', '')
    return re.sub(' +', ' ', s)


def collect_event_data(event):
    res = []
    if event.long_schedule:
        ln = len(event.datetime_list)
        event_name = event.event_name_list
        event_datetime = event.datetime_list
        event_price = event.price_list
        event_purchase_link = event.purchase_link_list
        helper_dt = event.helper_dt_list
        if len(helper_dt) < ln:
            helper_dt = ln * helper_dt
    else:
        ln = 1
        event_name = [event.event_name]
        event_datetime = [event.event_datetime]
        event_price = [event.event_price]
        event_purchase_link = [event.event_purchase_link]
        helper_dt = ['']
    try:
        helper_dt = [update_rus_date(el) for el in helper_dt]
        event_datetime = [str(datetime.strptime(el, '%d.%m.%Y, %H:%M')) for el in event_datetime]
    except Exception as e:
        print(f"Error while convertinf datetimes {e}")
    event_name = [prettify_string(el) for el in event_name]
    for i in range(ln):
        res.append(
            {
                "name": event_name[i],
                "event_pic": event.event_pic,
                "event_desc": event.event_desc,
                "event_place": prettify_string(event.event_place),
                "event_price": event_price[i],
                "event_dt": event_datetime[i],
                "event_purchase_link": event_purchase_link[i],
                "event_location": event.event_location,
                "event_long": event.event_long,
                "event_lat": event.event_lat,
                "helper_date": helper_dt[i]
            }
        )
    return res


def get_all_links_for_parsing(url='https://www.ticketpro.by/'):
    event_links = []
    try:
        main_links = get_main_links(url)
        event_links = get_event_group_links(main_links)
    except Exception as e:
        print(e)
    return event_links


def parse_event_by_link(event_link):
    url_event = f"https://www.ticketpro.by{event_link.a['href']}"
    print(url_event)
    time.sleep(random.randrange(10, 20))
    event = Event(url_event)
    res = collect_event_data(event)
    return res


def close_driver():
    driver.close()
    driver.quit()

# if __name__ == '__main__':
#     parse_event_by_link('https://www.ticketpro.by/bilety-v-teatr/palnka/')
#     close_driver()
    # main()
