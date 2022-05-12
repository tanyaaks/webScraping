import time
import random
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


def main():
    parsed_links_set = set()
    results_df = pd.DataFrame(data={'event_link': [],
                                    'event_name': [],
                                    'event_pic': [],
                                    'event_datetime': [],
                                    'event_place': [],
                                    'event_price': [],
                                    'event_purchase_link': [],
                                    'event_desc': [],
                                    'event_location': [],
                                    'event_long': [],
                                    'event_lat': []})
    events_count = 0
    url = 'https://www.ticketpro.by/'

    try:
        driver.get(url)
        doc_main = BeautifulSoup(driver.page_source, features="html.parser")
        div = doc_main.find(class_='header__menu')
        main_links = div.find_all(['a'], attrs={'class': ''})
        main_links = set([f"https://www.ticketpro.by{el['href']}" for el in main_links])
        for url_event_group in main_links:
            print(f"Event Group - {url_event_group}")
            time.sleep(random.randrange(40, 60))
            driver.get(url_event_group)
            doc_event_group = BeautifulSoup(driver.page_source, features="html.parser")
            # getting # of pages
            try:
                div = doc_event_group.find(class_='pagination text-center')
                pages = div.find(['a'], attrs={'class': 'page-last'})['data-page']
            except Exception as e:
                print(f"{e}\nThere's only 1 page")
                pages = 0
            finally:
                for i in range(1, int(pages) + 2):
                    print(f"Page {i}\n {url_event_group}?page={i}")
                    url_event_group_p = f"{url_event_group}?page={i}"
                    if len(parsed_links_set.intersection([url_event_group_p])) == 0:
                        parsed_links_set = parsed_links_set.union([url_event_group_p])
                        time.sleep(random.randrange(50, 70))
                        driver.get(url_event_group_p)
                        doc_event_group = BeautifulSoup(driver.page_source, features="html.parser")
                        # looping over all events on the page
                        try:
                            div = doc_event_group.find(class_='event-other')
                            event_list = div.find_all(class_='event-box__head')
                        except Exception as e:
                            print(f"No events are listed on the page\n{e}")
                        else:
                            print(f"{len(event_list)} events are found on the page")
                            for j in range(len(event_list)):
                                url_event = f"https://www.ticketpro.by{event_list[j].a['href']}"
                                print(url_event)
                                results_df = results_df.append(parse_page(url_event))
                                events_count += 1
                results_df.to_csv(f"output/ticketpro/ticketpro_data_{events_count}.csv")
    except Exception as e:
        print(e)
    finally:
        results_df.to_csv(f"output/ticketpro/ticketpro_data_fin.csv")
        driver.close()
        driver.quit()


def parse_page(url_event):
    fl = False
    time.sleep(random.randrange(45, 60))
    driver.get(url_event)
    doc = BeautifulSoup(driver.page_source, features="html.parser")

    html = doc.prettify("utf-8")
    with open(f"html_pages/ticketpro/{str(url_event).replace('/', '_')}.html", "wb") as file:
        file.write(html)

    try:
        event_name = doc.find('div', attrs={'class': 'title'}).text.strip()
    except Exception as e:
        print(f"Smth's wrong with event_name\n{e}")
        event_name = None
    try:
        event_pic = f"https://www.ticketpro.by{doc.find(['div'], attrs={'class': 'content-poster'}).img['src']}"
    except Exception as e:
        print(f"Smth's wrong with event_pic\n{e}")
        event_pic = None
    try:
        event_data = doc.find_all(['span'], attrs={'class': 'content__panel-title'})
        event_datetime = event_data[0].text.strip()
        event_place = event_data[1].a.text.strip()
        event_price = event_data[2].text.strip()
    except Exception as e:
        print(f"Smth's wrong with event_data\n{e}")
        event_datetime, event_place, event_price = None, None, None
    try:
        if len(doc.find_all(text='Расписание мероприятий')) != 0:
            fl = True
            datetime_l, ev_name_l, price_l, purch_l = parse_long_schedule(doc)
        else:
            event_purchase_link = f"https://www.ticketpro.by{doc.find(['a'], attrs={'class': 'btn btn-lg'})['href']}"
    except Exception as e:
        print(f"Smth's wrong with event_purchase_link\n{e}")
        event_purchase_link = None
    try:
        event_desc = doc.find(['div'], attrs={'class': 'content__text'}).text.strip()
    except Exception as e:
        print(f"Smth's wrong with event_desc\n{e}")
        event_desc = None
    try:
        event_location = doc.find_all(['div'], attrs={'class': 'sidebar-box__text'})[
            0].text.strip().split('\n')
        event_location = ", ".join([el for el in event_location if el != ''])
    except Exception as e:
        print(f"Smth's wrong with event_location\n{e}")
        event_location = None
    try:
        event_long = doc.find_all(['div'], attrs={'class': 'map'})[0]['data-lan']
        event_lat = doc.find_all(['div'], attrs={'class': 'map'})[0]['data-lat']
    except Exception as e:
        print(f"Smth's wrong with event_long or event_lan\n{e}")
        event_long, event_lat = None, None

    if fl:
        ln = len(datetime_l)
        event_name = ev_name_l
        event_datetime = datetime_l
        event_price = price_l
        event_purchase_link = purch_l
    else:
        ln = 1
        event_name = [event_name]
        event_datetime = [event_datetime]
        event_price = [event_price]
        event_purchase_link = [event_purchase_link]

    res_df = pd.DataFrame(data={'event_link': [url_event] * ln,
                                'event_name': event_name,
                                'event_pic': [event_pic] * ln,
                                'event_datetime': event_datetime,
                                'event_place': [event_place] * ln,
                                'event_price': event_price,
                                'event_purchase_link': event_purchase_link,
                                'event_desc': [event_desc] * ln,
                                'event_location': [event_location] * ln,
                                'event_long': [event_long] * ln,
                                'event_lat': [event_lat] * ln})
    return res_df


def parse_long_schedule(doc):
    div = doc.find_all('div', attrs={'class': 'event-group__box'})
    datetime_l = []
    ev_name_l = []
    price_l = []
    purch_l = []
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

    chb_i = 2
    while True:
        checkbox_xpath = f'/html/body/div[2]/main/main/div/div[2]/div[2]/div[1]/div[2]/div[1]/div/div/div[{chb_i}]/div/div'

        chb_i += 1
        try:
            element = driver.find_element(by=By.XPATH, value=checkbox_xpath)
            driver.execute_script("arguments[0].click();", element)
            time.sleep(random.randrange(40, 55))
            doc = BeautifulSoup(driver.page_source, "html.parser")
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
            print(f"No more radiobuttons\n{e}")
            break
    return datetime_l, ev_name_l, price_l, purch_l


if __name__ == '__main__':
    main()
