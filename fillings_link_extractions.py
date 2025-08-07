import os
import requests
import pandas as pd
from urllib.parse import urlparse
from dotenv import load_dotenv
import psycopg
import time
from tqdm import tqdm  # pip install tqdm if not installed

# Load environment variables
load_dotenv()

# PostgreSQL connection
PG_CONN = psycopg.connect(
    host=os.environ['PG_HOST'],
    port=os.environ['PG_PORT'],
    dbname=os.environ['PG_DATABASE'],
    user=os.environ['PG_USER'],
    password=os.environ['PG_PASSWORD'],
    autocommit=True
)
headers = {
    "User-Agent": "MyCompanyName-ResearchBot/1.0 (contact: guddu.kumar@aitoxr.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.9"
}

# Create folder if not exists
def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Extract filename from URL
def extract_filename_from_url(url):
    return os.path.basename(urlparse(url).path)

# Step 1: Fetch unique symbol rows with latest filing_date
def fetch_latest_10k_filings():
    with PG_CONN.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (f.symbol)
                f.symbol,
                f.filing_date,
                f.form_type,
                f.final_link
            FROM filings.filings f
            WHERE f.form_type = '10-K' AND f.final_link IS NOT NULL
            ORDER BY f.symbol, f.filing_date DESC
        """)
        rows = cur.fetchall()
        columns = ['symbol', 'filing_date', 'form_type', 'final_link']
        return pd.DataFrame(rows, columns=columns)

# Step 2: Download HTMLs and update dataframe



def download_htmls_and_update_df(df: pd.DataFrame, base_dir='downloads'):
    statuses = []
    count = 0

    for index, row in tqdm(df.iterrows(), total=len(df), desc="Downloading HTMLs"):
        symbol = row['symbol']
        url = row['final_link']
        
        if not isinstance(url, str) or not url.startswith('http'):
            print(f"[SKIP] Invalid URL for symbol {symbol}: {url}")
            statuses.append('')
            continue

        file_name = os.path.basename(url)
        folder_path = os.path.join(base_dir, symbol)
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, file_name)

        if os.path.exists(file_path):
            print(f"[SKIP] Already exists: {file_path}")
            statuses.append('done')
            count += 1
        else:
            try:
                print(f"[TRY] Downloading {symbol}: {url}")
                response = requests.get(url, headers=headers, timeout=15)
                if response.status_code == 200:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    print(f"[DONE] Saved: {file_path}")
                    statuses.append('done')
                else:
                    print(f"[FAIL] {symbol} - Status {response.status_code} for {url}")
                    statuses.append('')
            except Exception as e:
                print(f"[ERROR] {symbol} - Failed to download {url}: {e}")
                statuses.append('')

            count += 1
            if count % 10 == 0:
                print(f"[WAIT] Downloaded {count}. Sleeping 1 second...")
                time.sleep(1)

    df['htm_downloaded'] = statuses
    return df
# Run it
if __name__ == "__main__":
    #df = fetch_latest_10k_filings()
    df = pd.read_csv('latest_10k_filings.csv')
    #df.to_csv('all_symbol_fetch_unique_from_db.csv', index=False)  # Save to CSV for later use
   
    df = download_htmls_and_update_df(df)
    print(df.head())  # Preview result
    df.to_csv('latest_10k_filings_status.csv', index=False)  # Save to CSV

