import time
import pandas as pd
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import random

useragent = UserAgent()
options = webdriver.ChromeOptions()
options.add_argument(f"user_agent={useragent.random}")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--headless")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                          options=options)


def parse_all_tickets_info(doc):
    days_l = []
    date_l = []
    month_l = []
    df = pd.DataFrame({'place': [], 'dates': [], 'prices': [], 'tags': []})
    tmp = doc.find_all(['div'], class_='date-content')
    for el in tmp:
        try:
            days_l.append(el.find(class_='day').text)
            date_l.append(el.find(class_='date').text)
            month_l.append(el.find(class_='mounth').text)
        except Exception as e:
            print(f"Smth's wrong with dates in datecontent {e}")
    for cb_i in range(1, len(days_l) + 1):
        try:
            checkbox_xpath = f'/html/body/div/div/center/section/main/div/section/div/div[2]/div/div[2]/section[1]/div[2]/div[2]/div/div[{cb_i}]'
            element = driver.find_element(by=By.XPATH, value=checkbox_xpath)
            driver.execute_script("arguments[0].click();", element)
            driver.find_element(by=By.XPATH, value=checkbox_xpath).click()
            time.sleep(random.randrange(40, 55))
            doc = BeautifulSoup(driver.page_source, "html.parser")
            tmp = doc.find_all(class_='tickets__table')
        except Exception as e:
            print(f"Error when parsing schedule table {e}")
            tmp = pd.DataFrame()
        for el in tmp:
            try:
                tmp1 = el.find_all(class_='tickets__table_header')
            except Exception as e:
                print(f"Error in tickets table header {e}")
                tmp1 = pd.DataFrame()
            for ind in range(len(tmp1)):
                try:
                    pl = el.find_all(class_='tickets__table_header')[ind].a.text.strip()
                except Exception as e:
                    print(f"Error in tickets table header - can't find name {e}")
                    pl = ''
                l_dts = []
                l_tags = []
                l_prices = []
                small_list = el.find_all(class_='tickets__table_values')[ind]
                for ind2 in range(len(small_list)):
                    try:
                        l_dts.append(small_list.find_all('time')[ind2]['datetime'])
                    except Exception as e:
                        print(f"{e} - OK exception")
                        l_dts.append('')
                    try:
                        l_prices.append(small_list.find_all(class_='tooltip')[ind2].text)
                    except Exception as e:
                        print(f"{e} - OK exception")
                        l_prices.append('')
                    try:
                        l_tags.append(small_list.find_all('span', class_='tag')[ind2].text)
                    except Exception as e:
                        print(f"{e} - OK exception")
                        l_tags.append('')
                place_list = [pl] * len(l_dts)
                df = df.append(pd.DataFrame(
                    {'place': place_list, 'dates': l_dts, 'prices': l_prices, 'tags': l_tags}))
    return df


def get_page_info(link):
    time.sleep(random.randrange(50, 65))
    driver.get(link)
    doc = BeautifulSoup(driver.page_source, features="html.parser")
    html = doc.prettify("utf-8")
    with open(f"html_pages/bycard/{str(link).replace('/', '_')}.html", "wb") as file:
        file.write(html)
    try:
        event_name = doc.find(['div'], class_='event-page__header').h1.text.strip()
    except Exception as e:
        print(f"Smth's wrong with event name {e}")
        event_name = ''
    try:
        event_pic = doc.find(['picture'], class_='poster').img['src']
    except Exception as e:
        print(f"Smth's wrong with event picture {e}")
        event_pic = ''
    try:
        datetime_data = doc.find_all(['div'], class_='table-about__text')
        line = ''
        for el in datetime_data:
            line += el.text + ", "
        # event_datetime_tot = datetime_data[0].span.text.strip()
        # event_duration = datetime_data[1].text.strip()
        # event_age = datetime_data[3].text.strip()
        # event_price_tot = datetime_data[2].text.strip()
    except Exception as e:
        print(f"Smth's wrong with event datetimes {e}")
        # event_datetime_tot = ''
        # event_duration = ''
        # event_age = ''
        # event_price_tot = ''
        line = ''
    try:
        event_desc = doc.find(['div'], 'event-page__description').text
    except Exception as e:
        try:
            event_desc = doc.find('div', class_='event-page_desc').text
        except Exception as ex:
            print(ex)
            print(f"Smth's wrong with event description {e}")
            event_desc = ''
    full_data = parse_all_tickets_info(doc)
    if len(full_data) != 0:
        ln = len(full_data)
    else:
        ln = 1
        full_data = pd.DataFrame({'place': ['-'], 'dates': ['-'], 'prices': ['-'], 'tags': ['-']})
    res_df = pd.DataFrame({
        'event_name': [event_name] * ln,
        'event_pic': [event_pic] * ln,
        'desc_line': [line] * ln,
        # 'event_datetime_tot': [event_datetime_tot] * ln,
        # 'event_duration': [event_duration] * ln,
        # 'event_age': [event_age] * ln,
        # 'event_price_tot': [event_price_tot] * ln,
        'event_desc': [event_desc] * ln,
        'event_place': full_data.place,
        'event_dt': full_data.dates,
        'event_price': full_data.prices,
        'event_tag': full_data.tags
    })
    return res_df


def main():
    res_df = pd.DataFrame({
        'event_name': [],
        'event_pic': [],
        'desc_line': [],
        # 'event_datetime_tot': [],
        # 'event_duration': [],
        # 'event_age': [],
        # 'event_price_tot': [],
        'event_desc': [],
        'event_place': [],
        'event_dt': [],
        'event_price': [],
        'event_tag': []
    })

    url_main = 'https://bycard.by/'
    cnt = 0
    try:
        driver.get(url_main)
        doc_main = BeautifulSoup(driver.page_source, features="html.parser")
        main_links = []
        tmp = doc_main.find_all(['div'], class_='events-filter')
        for el in tmp:
            tmp1 = el.find_all(class_='button button_secondary-round uppercase semi-bold')
            main_links.append(
                f"{str(url_main)[:-1]}{el.find(class_='button button_secondary-round uppercase semi-bold active')['href']}")
            for el1 in tmp1:
                main_links.append(f"{str(url_main)[:-1]}{el1['href']}")

        for m_link in main_links[::-1]:
            print(f"MAIN LINK {m_link}")
            time.sleep(random.randrange(35, 45))
            driver.get(m_link)
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.randrange(20, 35))

                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            doc_group_event = BeautifulSoup(driver.page_source, features="html.parser")

            links = []
            tmp = doc_group_event.find_all(class_='capsule')
            for t in tmp:
                links.append(f"{str(url_main)[:-1]}{t['href']}")
            for link in links:
                print(link)
                cnt += 1
                res_df = res_df.append(get_page_info(link))
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
