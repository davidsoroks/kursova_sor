import sqlite3
import requests
from bs4 import BeautifulSoup
import re
import time

DB_NAME = 'air_losses.db'
BASE_URL = "https://index.minfin.com.ua"
START_URL = "https://index.minfin.com.ua/ua/russian-invading/casualties/"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS air_losses
                   (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       report_date DATE UNIQUE,
                       planes INTEGER DEFAULT 0,
                       helicopters INTEGER DEFAULT 0,
                       uav INTEGER DEFAULT 0,
                       cruise_missiles INTEGER DEFAULT 0,
                       ballistic_missiles INTEGER DEFAULT 0
                   )
                   ''')
    conn.commit()
    conn.close()
    print("[INFO] База даних перевірена.")


def get_existing_dates():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT report_date FROM air_losses")
        existing_dates = {row[0] for row in cursor.fetchall()}
        conn.close()
        return existing_dates
    except Exception:
        return set()


def parse_soup_data(soup, existing_dates):
    results = []

    ul_containers = soup.find_all('ul', attrs={'class': 'see-also'})

    if not ul_containers:
        return results

    all_li_items = []
    for ul in ul_containers:
        all_li_items.extend(ul.find_all('li', attrs={'class': 'gold'}))

    for item in all_li_items:
        daily_data = {
            'report_date': None,
            'planes': 0, 'helicopters': 0, 'uav': 0,
            'cruise_missiles': 0, 'ballistic_missiles': 0
        }

        try:
            date_span = item.find('span', class_='black')
            if date_span:
                date_text = date_span.get_text(strip=True)
                match_date = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_text)
                if match_date:
                    d, m, y = match_date.groups()
                    daily_data['report_date'] = f"{y}-{m}-{d}"
        except Exception:
            continue

        if not daily_data['report_date']:
            continue

        if daily_data['report_date'] in existing_dates:
            continue

        casualties_div = item.find('div', class_='casualties')
        if casualties_div:
            loss_lines = casualties_div.find_all('li')
            for line in loss_lines:
                text = line.get_text(" ", strip=True).lower()
                match_plus = re.search(r'\+\s*(\d+)', text)
                count = int(match_plus.group(1)) if match_plus else 0

                if 'літаки' in text:
                    daily_data['planes'] = count
                elif 'гелікоптери' in text:
                    daily_data['helicopters'] = count
                elif 'бпла' in text:
                    daily_data['uav'] = count
                elif 'крилаті' in text and 'ракети' in text:
                    daily_data['cruise_missiles'] = count
                elif 'балістичні' in text:
                    daily_data['ballistic_missiles'] = count

        results.append(daily_data)
        existing_dates.add(daily_data['report_date'])

    return results


def get_all_data_with_archive():
    headers = {'User-Agent': 'Mozilla/5.0'}

    existing_dates = get_existing_dates()
    print(f"[INFO] В базі вже є записів: {len(existing_dates)}")

    all_new_data = []

    print(f"[PROCESS] Завантаження головної сторінки...")
    try:
        response = requests.get(START_URL, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')

            current_data = parse_soup_data(soup, existing_dates)
            all_new_data.extend(current_data)
            print(f"   -> Знайдено {len(current_data)} нових записів на головній.")

            archive_links = soup.select('div.ajaxmonth h4 a')

            print(f"[PROCESS] Знайдено архівних місяців: {len(archive_links)}")

            for link in archive_links:
                href = link.get('href')
                if not href: continue

                full_url = BASE_URL + href
                month_name = link.get_text(strip=True)

                print(f"[CRAWLER] Обробка: {month_name}...")

                try:
                    r_month = requests.get(full_url, headers=headers)
                    if r_month.status_code == 200:
                        s_month = BeautifulSoup(r_month.content, 'html.parser')

                        month_data = parse_soup_data(s_month, existing_dates)

                        if month_data:
                            all_new_data.extend(month_data)
                            print(f"+ Додано {len(month_data)} записів.")
                        else:
                            print(f". Нових даних немає.")

                        time.sleep(0.5)

                except Exception as e:
                    print(f"[ERROR] Не вдалося завантажити {full_url}: {e}")

    except Exception as e:
        print(f"[CRITICAL ERROR] Збій на головній сторінці: {e}")

    return all_new_data


def save_to_db(data_list):
    if not data_list:
        print("[INFO] Немає нових даних для збереження.")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print(f"[INFO] Запис {len(data_list)} рядків у БД...")

    sql = '''
          INSERT \
          OR IGNORE INTO air_losses 
        (report_date, planes, helicopters, uav, cruise_missiles, ballistic_missiles)
        VALUES (:report_date, :planes, :helicopters, :uav, :cruise_missiles, :ballistic_missiles) \
          '''

    try:
        cursor.executemany(sql, data_list)
        conn.commit()
        print(f"[SUCCESS] Всі дані успішно збережено!")
    except Exception as e:
        print(f"[ERROR] Помилка SQL: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    full_data = get_all_data_with_archive()
    save_to_db(full_data)