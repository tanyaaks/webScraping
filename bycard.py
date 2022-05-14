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

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                          options=options)


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

    def get_event_name(self):
        try:
            event_name = self.doc.find(['div'], class_='event-page__header').h1.text.strip()
        except Exception as e:
            print(f"get_event_name {e}")
            event_name = ''
        return event_name

    def get_event_pic(self):
        try:
            event_pic = self.doc.find(['picture'], class_='poster').img['src']
        except Exception as e:
            print(f"get_event_pic {e}")
            event_pic = ''
        return event_pic

    def get_event_desc_line(self):
        try:
            datetime_data = self.doc.find_all(['div'], class_='table-about__text')
            line = ''
            for el in datetime_data:
                line += el.text + ", "
        except Exception as e:
            print(f"get_event_desc_line {e}")
            line = ''
        return line

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

    def get_number_in_schedule(self):
        cnt = 0
        try:
            tmp = self.doc.find_all(['div'], class_='date-content')
            for _ in tmp:
                cnt += 1
        except Exception as e:
            print(f"get_number_in_schedule {e}")
        return cnt

    def get_schedule_table(self, checkbox_ind):
        try:
            checkbox_xpath = f'/html/body/div/div/center/section/main/div/section/div/div[2]/div/div[2]/section[1]/div[2]/div[2]/div/div[{checkbox_ind}]'
            element = driver.find_element(by=By.XPATH, value=checkbox_xpath)
            driver.execute_script("arguments[0].click();", element)
            driver.find_element(by=By.XPATH, value=checkbox_xpath).click()
            time.sleep(random.randrange(40, 55))
            doc = BeautifulSoup(driver.page_source, "html.parser")
            res = doc.find_all(class_='tickets__table')
        except Exception as e:
            print(f"get_schedule_table {e}")
            res = pd.DataFrame()
        return res

    def get_tickets_header_list(self, ind):
        try:
            res = self.schedule_table[ind].find_all(class_='tickets__table_header')
        except Exception as e:
            print(f"get_tickets_header_list {e}")
            res = pd.DataFrame()
        return res

    def get_event_places_list(self, ind):
        pl = []
        for ind1 in range(len(self.tickets_header)):
            try:
                pl_new = self.schedule_table[ind].find_all(class_='tickets__table_header')[ind1].a.text.strip()
                schedule = self.schedule_table[ind].find_all(class_='tickets__table_values')[ind1]
                pl.extend([pl_new] * len(schedule))
            except Exception as e:
                print(f"get_event_places_list {e}")
                pl_new = ''
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
                print(f"get_event_tags_list2 {e}")
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
    for m_link in main_links[::-1]:
        time.sleep(random.randrange(35, 45))
        driver.get(m_link)
        doc_group_event = scroll_page_to_update_html()
        tmp = doc_group_event.find_all(class_='capsule')
        for t in tmp:
            links.append(f"{str(url)[:-1]}{t['href']}")
    return links


def collect_event_data(event):
    ln = 1
    if len(event.event_dts_list) > 0:
        ln = len(event.event_dts_list)
    res = pd.DataFrame({
        'event_name': [event.event_name] * ln,
        'event_pic': [event.event_pic] * ln,
        'desc_line': [event.event_desc_line] * ln,
        'event_desc': [event.event_desc] * ln,
        'event_place': event.event_places_list,
        'event_dt': event.event_dts_list,
        'event_price': event.event_prices_list,
        'event_tag': event.event_tags_list
    })
    return res


def main():
    res_df = pd.DataFrame({
        'event_name': [],
        'event_pic': [],
        'desc_line': [],
        'event_desc': [],
        'event_place': [],
        'event_dt': [],
        'event_price': [],
        'event_tag': []
    })

    url_main = 'https://bycard.by/'
    cnt = 0
    try:
        main_links = get_main_links(url_main)
        links = get_all_events_links(url_main, main_links)
        for link in links:
            print(link)
            cnt += 1
            time.sleep(random.randrange(50, 65))
            driver.get(link)
            event = Event(driver.page_source)
            res_df = res_df.append(collect_event_data(event))
            if cnt % 50 == 0:
                res_df.to_csv(f"output/bycard/bycard_data_{cnt}.csv")
    except Exception as e:
        print(e)
    finally:
        res_df.to_csv(f"output/bycard/bycard_data_fin.csv")
        driver.close()
        driver.quit()


if __name__ == '__main__':
    main()
