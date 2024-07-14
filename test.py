import requests
import json
import schedule
import time
import mysql.connector
from urllib.parse import urlparse
from urllib.request import urlopen
from mysql.connector import Error

# MySQL database configuration
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'jazil_usr',
    'password': 'Jazil@1233',
    'database': 'apple'
}

# Connect to MySQL
conn = mysql.connector.connect(**DATABASE_CONFIG)
cursor = conn.cursor()

# Cookies for Wegmans API (you can update this if needed)
COOKIES = {
    'cookie': '_pin_unauth=dWlkPU9HVmxPR000T1dFdE1XSmhPQzAwWlRFMExXSTVOemd0WW1VNFl6UTFZMk0xWm1VMQ; ajs_anonymous_id=17424575-2cdc-4a00-9b44-6d6a94201f1b; sa-user-id=s%253A0-11fb6219-bcb6-5b55-6cce-2117a7964893.u417Suaqk0UIkJogLRkqE9rm1Zbg3oGH%252BZRzIQIrifo; sa-user-id-v2=s%253AEftiGby2W1VsziEXp5ZIkzEvxIg.1NH1fpuxpTQu%252B0sbglgKu8ECbY8nrOVrJHe%252Bw5PqSMQ; sa-user-id-v3=s%253AAQAKIDBXs133MqCZcxdQRnaQYcwgdDJ-VTbugywCLk-InUMSEAEYAyC4_PisBjABOgROQQ4MQgQ86QaL.H%252BhBC%252Bnn%252FV0tLyv9cZPBg2pVa5mwKZkFmH96PmS7A20; _pin_unauth=dWlkPU9HVmxPR000T1dFdE1XSmhPQzAwWlRFMExXSTVOemd0WW1VNFl6UTFZMk0xWm1VMQ; __stripe_mid=8b4e5716-8b8b-4012-8f31-fabb94c485bc2f9d85; ajs_anonymous_id=17424575-2cdc-4a00-9b44-6d6a94201f1b; __cf_bm=F90KiNcMrtuf_.mKsHRO7X.zK7RBh24pDg.WUc79fSU-1720933077-1.0.1.1-KUY6HyU4iiUxlWoJzMnLUD.QSYWpH9K4jQSXa3Hu8u3IU.qJ9nRiNjhQIPZtnhuQ8hU52KSPacse36ySxhlXtw; AMCVS_68B620B35350F1650A490D45%40AdobeOrg=1; kndctr_68B620B35350F1650A490D45_AdobeOrg_identity=CiY4MDUzNzA5NzIyNTA1MjU2NTQwMzYyNDU3OTk3MjEzODA3ODQ4MVIRCP7hpb_XMRgBKgRJTkQxMAPwAYHN9_yKMg==; kndctr_68B620B35350F1650A490D45_AdobeOrg_cluster=ind1; _fbp=fb.1.1720933279862.525935617162648537; AMCV_68B620B35350F1650A490D45%40AdobeOrg=179643557%7CMCIDTS%7C19919%7CMCMID%7C80537097225052565403624579972138078481%7CMCAAMLH-1721538079%7C12%7CMCAAMB-1721538079%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1720940479s%7CNONE%7CMCSYNCSOP%7C411-19926%7CMCCIDH%7C0%7CvVersion%7C5.5.0; _gcl_au=1.1.615475732.1720933283; wfm.tracking.sessionStart=1720933283378; wfmStoreId=16; at_check=true; inRedirectYogurtAudience=1; inChampagneRedirectAudience=1; inRedirectTestAudience=1; inRedirectGlutenFreeAudience=1; inRedirectGoldPanAudience=1; inRedirectVitaminsAudience=1; wegmans.chatbot.closed=1; wfm.tracking.s10=1; dotcomSearchId=597ceb1d-d03f-40ba-a290-6d37fc1cb4ac; lux_uid=172093330446873215; wfm.tracking.x2p=1; at_check=true; __stripe_sid=103aedf3-6988-498e-9204-7d7223f4dc9261f5bd; s_gpv=Search%20Results:%20apple%20|%20Wegmans; _uetsid=1e4e93e0419e11ef970ec1cebac375a9; _uetvid=23022dc0c3ee11ee838d136dcf19cf78; session-prd-weg=.eJwdjstygjAARf8la-vw7BSWRWVCCYiACWwYhFgjLyWghk7_vZkuzuYs7j0_oDiPlF-AfS5bTleguNGxK3vaT8CexlkaTjlnQ19MQ0N7YAMqvMvJrVjIPJguUA2YZ62lVCvtKCRLpbWPU2vdcge-w2v0Ct28RUlk-MmhQQmags12yZhi5F368pNIIDc1kLs1Ed61AYMc9sclJ965xBELr5GJlnSRiDB-sgwfphKb_19Eaxt4vc01fnHfkVGdNVOsPmqCWNgfRI1TDrv2UssOlFQCbRq5A42AKGs1JvtmyD7F3X-esn3mfMT3L6gMSvMdm29dvtcJgWiH3c0WrMDM6ViwGtiGpZu6bmrW7x-QzWoA.GXTtYw.CiT6BXJiczkPJBiQGP-UG9xuhf8; _dd_s=rum=0&expire=1720934247783; mbox=session#69cd08c3c44945f8a947a78f6a13c78c#1720935209|PC#69cd08c3c44945f8a947a78f6a13c78c.41_0#1784178125'
}

# API URL for Wegmans
API_URL = 'https://shop.wegmans.com/api/v2/store_products?fulfillment_type=instore&ads_enabled=true&ads_pagination_improvements=true&limit=60&offset=0&page=1&prophetScorer=frecency&sort=rank&allow_autocorrect=true&search_is_autocomplete=false&search_provider=ic&search_term=apple&secondary_results=true&unified_search_shadow_test_enabled=false'

# File path for saving JSON data
JSON_FILE_PATH = 'data.json'

# Function to fetch and save data
def fetch_and_save_data():
    try:
        response = requests.get(API_URL, cookies=COOKIES)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        
        # Save data to JSON file
        with open(JSON_FILE_PATH, 'w') as f:
            json.dump(data, f)
        
        # Extract and store in MySQL
        extract_and_store_data(data)
        
        print("Data fetched, saved to JSON, and stored in database successfully.")

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")

# Function to extract and store data in MySQL
def extract_and_store_data(data):
    try:
        for item in data.get('results', []):
            name = item.get('name')
            price = item['price'].get('regular') if 'price' in item else None
            url = item.get('url')
            
            # Insert into MySQL
            query = "INSERT INTO products (name, price, url) VALUES (%s, %s, %s)"
            cursor.execute(query, (name, price, url))
        
        conn.commit()
        print("Data extracted and stored in MySQL.")

    except Error as e:
        print(f"Error storing data in MySQL: {e}")

# Function to check URL status and update MySQL
def check_url_status():
    try:
        cursor.execute("SELECT id, url FROM products")
        products = cursor.fetchall()
        
        for product in products:
            url_id, url = product
            
            try:
                # Check if URL is reachable
                code = urlopen(url).getcode()
                active = True if code == 200 else False
                
                # Update active status in MySQL
                update_query = "UPDATE products SET active = %s WHERE id = %s"
                cursor.execute(update_query, (active, url_id))
                conn.commit()
            
            except Exception as e:
                print(f"Error checking URL {url}: {e}")
    
    except Error as e:
        print(f"Error fetching products from MySQL: {e}")

# Schedule tasks
schedule.every().hour.at(":00").do(fetch_and_save_data)
# Schedule to run daily at 1 AM
schedule.every().day.at("01:00").do(check_url_status)
# Main loop
while True:
    schedule.run_pending()
    time.sleep(1)
