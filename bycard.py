import random
import time

import pandas as pd
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
    def __init__(self, page_html):
        self.doc = BeautifulSoup(page_html, features="html.parser")
        self.event_name = self.get_event_name()
        self.event_pic = self.get_event_pic()
        self.event_desc_line = self.get_event_desc_line()
        self.event_desc = self.get_event_desc()
        self.num_events = self.get_number_in_schedule()
        self.event_dts_list = []
        self.event_prices_list = []
        self.event_tags_list = []
        self.event_places_list = []
        for checkbox_ind in range(1, self.num_events + 1):
            self.schedule_table = self.get_schedule_table(checkbox_ind)
            for ind in range(len(self.schedule_table)):
                self.tickets_header = self.get_tickets_header_list(ind)
                self.event_places_list.extend(self.get_event_places_list(ind))
                self.event_dts_list.extend(self.get_event_dates_list(ind))
                self.event_prices_list.extend(self.get_event_prices_list(ind))
                self.event_tags_list.extend(self.get_event_tags_list(ind))

    @error_decorator
    def get_event_name(self, default_res=''):
        return self.doc.find(['div'], class_='event-page__header').h1.text.strip()

    @error_decorator
    def get_event_pic(self, default_res=''):
        return self.doc.find(['picture'], class_='poster').img['src']

    @error_decorator
    def get_event_desc_line(self, default_res=''):
        datetime_data = self.doc.find_all(['div'], class_='table-about__text')
        line = ''
        for el in datetime_data:
            if el:
                line += el.text + ", "
        return line[:-2]

    def get_event_desc(self):
        try:
            event_desc = self.doc.find(['div'], 'event-page__description').text
        except Exception as e:
            print(f"{e}\nTrying to parse another description")
            try:
                event_desc = self.doc.find('div', class_='event-page_desc').text
            except Exception as ex:
                print(f"get_event_desc {ex}")
                event_desc = ''
        return event_desc

    @error_decorator
    def get_number_in_schedule(self, default_res=0):
        tmp = self.doc.find_all(['div'], class_='date-content')
        return len(tmp)

    # @error_decorator
    def get_schedule_table(self, checkbox_ind, default_res=pd.DataFrame()):
        try:
            checkbox_xpath = f'/html/body/div/div/center/section/main/div/section/div/div[2]/div/div[2]/section[1]/div[2]/div[2]/div/div[{checkbox_ind}]'
            element = driver.find_element(by=By.XPATH, value=checkbox_xpath)
            driver.execute_script("arguments[0].click();", element)
            driver.find_element(by=By.XPATH, value=checkbox_xpath).click()
            time.sleep(random.randrange(40, 55))
            doc = BeautifulSoup(driver.page_source, "html.parser")
            res = doc.find_all(class_='tickets__table')
        except Exception as e:
            print(f"get_schedule_table error - {e}")
            res = pd.DataFrame()
        return res

    @error_decorator
    def get_tickets_header_list(self, ind, default_res=pd.DataFrame()):
        return self.schedule_table[ind].find_all(class_='tickets__table_header')

    def get_event_places_list(self, ind):
        pl = []
        for ind1 in range(len(self.tickets_header)):
            try:
                pl_new = self.schedule_table[ind].find_all(class_='tickets__table_header')[ind1].a.text.strip()
                schedule = self.schedule_table[ind].find_all(class_='tickets__table_values')[ind1]
                pl.extend([pl_new] * len(schedule))
            except Exception as e:
                print(f"get_event_places_list {e}")
                pl_new = '-'
                pl.extend([pl_new])
        return pl

    def get_event_dates_list(self, ind):
        res = []
        for ind1 in range(len(self.tickets_header)):
            try:
                schedule = self.schedule_table[ind].find_all(class_='tickets__table_values')[ind1]
                for ind2 in range(len(schedule)):
                    try:
                        res.append(schedule.find_all('time')[ind2]['datetime'])
                    except Exception as e:
                        print(f"get_event_dates_list1 {e}")
                        res.append('')
            except Exception as e:
                res = [''] * len(self.tickets_header)
                print(f"get_event_dates_list2 {e}")
        return res

    def get_event_prices_list(self, ind):
        res = []
        for ind1 in range(len(self.tickets_header)):
            try:
                schedule = self.schedule_table[ind].find_all(class_='tickets__table_values')[ind1]
                for ind2 in range(len(schedule)):
                    try:
                        res.append(schedule.find_all(class_='tooltip')[ind2].text)
                    except Exception as e:
                        print(f"get_event_prices_list1 {e}")
                        res.append('')
            except Exception as e:
                res = [''] * len(self.tickets_header)
                print(f"get_event_prices_list2 {e}")
        return res

    def get_event_tags_list(self, ind):
        res = []
        for ind1 in range(len(self.tickets_header)):
            try:
                schedule = self.schedule_table[ind].find_all(class_='tickets__table_values')[ind1]
                for ind2 in range(len(schedule)):
                    try:
                        res.append(schedule.find_all('span', class_='tag')[ind2].text)
                    except Exception as e:
                        print(f"get_event_tags_list1 {e}")
                        res.append('')
            except Exception as e:
                res = ['']*len(self.tickets_header)
                print(f"get_event_tags_list {e}")
        return res


def get_main_links(url):
    driver.get(url)
    doc = BeautifulSoup(driver.page_source, features="html.parser")
    main_links = []
    tmp = doc.find_all(['div'], class_='events-filter')
    for el in tmp:
        tmp1 = el.find_all(class_='button button_secondary-round uppercase semi-bold')
        main_links.append(
            f"{str(url)[:-1]}{el.find(class_='button button_secondary-round uppercase semi-bold active')['href']}")
        for el1 in tmp1:
            main_links.append(f"{str(url)[:-1]}{el1['href']}")
    return main_links


def scroll_page_to_update_html():
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.randrange(20, 35))
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    return BeautifulSoup(driver.page_source, features="html.parser")


def get_all_events_links(url, main_links):
    links = []
    for m_link in main_links[:2]: #main_links[::-1]:
        time.sleep(random.randrange(35, 45))
        driver.get(m_link)
        doc_group_event = scroll_page_to_update_html()
        tmp = doc_group_event.find_all(class_='capsule')
        for t in tmp:
            links.append(f"{str(url)[:-1]}{t['href']}")
    return links


def collect_event_data(event):
    res = []
    if not event.event_dts_list:
        return None
    for i in range(len(event.event_places_list)):
        res.append(
            {
                "name": event.event_name,
                "event_pic": event.event_pic,
                "desc_line": event.event_desc_line,
                "event_desc": event.event_desc,
                "event_place": event.event_places_list[i],
                "event_price": event.event_prices_list[i],
                "event_tag": event.event_tags_list[i],
                "event_dt": event.event_dts_list[i]
            }
        )
    return res


def get_all_links_for_parsing(url='https://bycard.by/'):
    links = []
    url_main = url
    print(url)
    try:
        main_links = get_main_links(url_main)
        print("Main links collected")
        links = get_all_events_links(url_main, main_links)
    except Exception as e:
        print(e)
    return links


def parse_event_by_link(link):
    driver.get(link)
    time.sleep(random.randrange(50, 65))
    event = Event(driver.page_source)
    res = collect_event_data(event)
    return res


def close_driver():
    driver.close()
    driver.quit()


# if __name__ == '__main__':
#     main()
