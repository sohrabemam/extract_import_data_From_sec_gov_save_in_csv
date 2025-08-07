import pandas as pd
import os
from bs4 import BeautifulSoup
import warnings
from bs4 import XMLParsedAsHTMLWarning
from concurrent.futures import ProcessPoolExecutor

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# --- Helper function ---
def normalize(text):
    return " ".join(text.replace('\xa0', ' ').split()).strip().lower()

# --- Extraction logic for HTML ---
def extract_item_1_section_from_htm(file_path):
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()

        soup = BeautifulSoup(html, "lxml")
        tags = soup.find_all(["div", "p", "span"])

        start_idx = end_idx = -1

        for i, tag in enumerate(tags):
            if tag.find("a"):
                continue  # Skip TOC links like <a href="#..."></a>

            text = tag.get_text(strip=True)
            norm = normalize(text)

            if start_idx == -1 and ("item 1. business" in norm or "item 1. description of business" in norm):
                start_idx = i
            elif start_idx != -1 and ("item 1a." in norm or "item 1a. risk factors" in norm):
                end_idx = i
                break

        if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
            return '', ''

        extracted_tags = tags[start_idx:end_idx]
        item_soup = BeautifulSoup(''.join(str(tag) for tag in extracted_tags), 'lxml')

        for p in item_soup.find_all('p'):
            style = p.get('style', '').lower()
            align = p.get('align', '').lower()
            if 'font-size:8.5pt' in style and 'center' in align:
                p.decompose()

        html_content = str(item_soup)
        text_content = item_soup.get_text(separator="\n", strip=True)

        return text_content.strip(), html_content.strip()

    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")
        return '', ''

# --- TXT Extraction ---
def extract_item_1_section_from_txt(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()

        clean_text = text
        upper_text = text.upper()

        patterns = ["ITEM 1. BUSINESS", "ITEM 1. DESCRIPTION OF BUSINESS"]
        end_pattern = "ITEM 1A."

        start_idx = -1
        for pattern in patterns:
            idx = upper_text.find(pattern)
            if idx != -1:
                start_idx = idx
                break

        if start_idx == -1:
            return '', ''

        end_idx = upper_text.find(end_pattern, start_idx)
        if end_idx == -1:
            return '', ''

        extracted = clean_text[start_idx:end_idx]
        return extracted.strip(), ''

    except Exception as e:
        print(f"❌ Error processing TXT file {file_path}: {e}")
        return '', ''

# --- Unified extractor ---
def extract_item_1_content(file_path):
    if file_path.lower().endswith('.txt'):
        return extract_item_1_section_from_txt(file_path)
    else:
        return extract_item_1_section_from_htm(file_path)

# --- Processing logic ---
def process_row(row, base_dir='downloads'):
    symbol = row['symbol']
    final_link = row['final_link']
    file_name = os.path.basename(final_link)
    file_path = os.path.join(base_dir, symbol, file_name)
    content, html = extract_item_1_content(file_path)
    return symbol, content, html

# --- Parallel processing logic ---
def parallel_process(df, base_dir='downloads', max_workers=8):
    data = df.to_dict(orient='records')
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_row, row, base_dir) for row in data]
        for future in futures:
            results.append(future.result())

    symbols, contents, htmls = zip(*results)
    df['content'] = contents
    df['htm_content'] = htmls
    return df

# --- Entry Point ---
if __name__ == "__main__":
    input_csv = 'latest_10k_filings_status.csv'
    output_csv = 'final_updated_with_item1.csv'

    print("📥 Loading CSV...")
    df = pd.read_csv(input_csv)

    print("⚡ Starting multiprocessing extraction...")
    df = parallel_process(df, base_dir='downloads', max_workers=8)

    df.to_csv(output_csv, index=False)
    print("✅ Extraction completed and saved to:", output_csv)
