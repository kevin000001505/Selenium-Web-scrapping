from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import psycopg2

class EarthquakeScrapper:

    def __init__(self):
        self.url = "https://www.cwa.gov.tw/V8/C/E/index.html"
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(options=chrome_options)
        
        self.existing_data = []

        self.db_config = {
        'dbname': 'Earthquake',
        'user': 'postgres',
        'password': '0105',
        'host': 'localhost',
        'port': '5432'  
        }

        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def insert_data_into_db(self, data):

        insert_query = """
        INSERT INTO earthquakes (place, date, time, maximum, depth, scale, web_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        
        for entry in data:
            
            self.cursor.execute(insert_query, (
                entry['Place'], 
                entry['Date'], 
                entry['Time'], 
                entry['Maximum'], 
                entry['Depth'], 
                entry['Scale'], 
                entry['URL']
            ))
        self.conn.commit()
    

    def scrape_earthquake_data(self):
        self.driver.get(self.url)
        #time.sleep(1)

        earthquake_data = []
        rows = self.driver.find_elements(By.XPATH, "//tr[contains(@class, 'eq-row')]")

        for row in rows:
            
            place = row.find_element(By.XPATH, ".//li[@style='word-break:normal;']").text.split('\n')[1:2]
            maximum = row.find_element(By.CSS_SELECTOR, "td[headers='maximum']").text

            date_element = row.find_element(By.XPATH, ".//div[@class='eq-detail']/span[1]")
            depth_element = row.find_element(By.XPATH, ".//div[@class='eq-detail']/ul/li[2]")
            scale_element = row.find_element(By.XPATH, ".//div[@class='eq-detail']/ul/li[3]")
            url_element = row.find_element(By.XPATH, ".//div[@class='eq-infor']/a")

            date = date_element.text.split(' ')[0].strip()
            timing = date_element.text.split(' ')[1].strip()
            depth = depth_element.text.split("深度")[1].strip() if "深度" in depth_element.text else ""
            scale = scale_element.text.split("地震規模")[1].strip() if "地震規模" in scale_element.text else ""
            web_url = url_element.get_attribute("href")

            earthquake_data.append({
                "Place": place,
                'Date': date,
                "Time": timing,
                "Maximum": maximum,
                "Depth": depth,
                "Scale": scale,
                "URL": web_url
            })
        print(earthquake_data)
        return earthquake_data
    
    def find_new_data(self, new_data):
        existing_urls = {data['URL'] for data in self.existing_data}
        new_entries = [data for data in new_data if data['URL'] not in existing_urls]
        return new_entries
    
    def update_dataset(self, new_entries):
        if new_entries:
            self.existing_data.extend(new_entries)
            self.insert_data_into_db(new_entries)
    
    def run(self):
        while True:
            new_data = self.scrape_earthquake_data()
            new_entries = self.find_new_data(new_data)
            self.update_dataset(new_entries)

            if new_entries:
                print(f"Find {len(new_entries)} new entries")
            
            time.sleep(300)
        
    def close(self):
        self.driver.quit()
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    scraper = EarthquakeScrapper()
    try:
        scraper.run()
    except KeyboardInterrupt:
        print("Stopping the scraper.")
    finally:
        scraper.close()

