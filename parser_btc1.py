import requests
import sqlite3
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import multiprocessing
import time
import random
import backoff
import re

lock = multiprocessing.Lock()



def create_database():
    conn = sqlite3.connect('bitcoin_addresses.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS addresses (address TEXT PRIMARY KEY)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS progress (id INTEGER PRIMARY KEY, last_block INTEGER)''')
    if cursor.execute('''SELECT COUNT(*) FROM progress''').fetchone()[0] == 0:
        cursor.execute('''INSERT INTO progress (last_block) VALUES (1)''')
    conn.commit()
    conn.close()

def save_address_to_db(address):
    if validate_bitcoin_address(address):
        with lock:
            conn = sqlite3.connect('bitcoin_addresses.db', check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('''INSERT OR IGNORE INTO addresses (address) VALUES (?)''', (address,))
            if cursor.rowcount > 0:
                print(f"Address saved: {address}")
            conn.commit()
            conn.close()

def validate_bitcoin_address(address):
    pattern = re.compile(r'^(1|3|bc1)[a-zA-HJ-NP-Z0-9]{25,39}$')
    return pattern.match(address) is not None

def get_last_processed_block():
    conn = sqlite3.connect('bitcoin_addresses.db', check_same_thread=False)
    cursor = conn.cursor()
    last_block = cursor.execute('''SELECT last_block FROM progress WHERE id = 1''').fetchone()[0]
    conn.close()
    return last_block

def update_last_processed_block(block_height):
    with lock:
        conn = sqlite3.connect('bitcoin_addresses.db', check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''UPDATE progress SET last_block = ? WHERE id = 1''', (block_height,))
        conn.commit()
        conn.close()

def get_session():
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=120, factor=2)
def fetch_block_data(session, block_height):
    base_url = "https://blockchain.info/rawblock/"
    try:
        response = session.get(base_url + str(block_height), timeout=5)
        if response.status_code == 200:
            data = response.json()
            unique_addresses = set()
            for tx in data['tx']:
                for out in tx['out']:
                    if 'addr' in out:
                        unique_addresses.add(out['addr'])
            for address in unique_addresses:
                save_address_to_db(address)
            update_last_processed_block(block_height)
        elif response.status_code == 429:
            print(f"Rate limit reached for block {block_height}, retrying after delay.")
            time.sleep(random.uniform(5, 15))
        else:
            print(f"Failed to fetch data for block {block_height}: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")

def worker(block_height_range):
    session = get_session()
    for block_height in block_height_range:
        fetch_block_data(session, block_height)

def get_bitcoin_addresses():
    start_block_height = get_last_processed_block()
    latest_block_height = 800000

    block_height_range = range(start_block_height, latest_block_height + 1)
    num_workers = min(multiprocessing.cpu_count() * 2, 32)
    chunk_size = max(len(block_height_range) // num_workers, 1)


    block_height_chunks = [block_height_range[i:i + chunk_size] for i in range(0, len(block_height_range), chunk_size)]

    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.map(worker, block_height_chunks)

if __name__ == "__main__":
    multiprocessing.freeze_support()
    create_database()
    get_bitcoin_addresses()
