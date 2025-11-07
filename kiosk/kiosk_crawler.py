import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import random

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 503, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def extract_page(session, page_index):
    try:
        url = f"https://franchise.ftc.go.kr/mnu/00013/program/userRqst/list.do?searchCondition=&searchKeyword=&column=brd&selUpjong=21&selIndus=&pageUnit=10&pageIndex={page_index}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = session.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch webpage. Status Code: {response.status_code}")
        
        time.sleep(random.uniform(0.5, 1.5))
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # thead -> colomn list
        thead = soup.find('thead')
        tbody = soup.find('tbody')
        
        if not thead or not tbody:
            return None
        
        columns = [th.get_text(strip=True) for th in thead.find_all('th')]
        
        # tbody -> tr
        rows = tbody.find_all('tr')
        
        # tr -> td
        all_rows_data = []
        for row in rows:
            cols = row.find_all('td')
            row_data = [col.get_text().strip() for col in cols]
            all_rows_data.append(row_data)
        
        page_df = pd.DataFrame(all_rows_data, columns=columns)
        
        page_df['page_index'] = page_index
        
        return page_df
    
    except Exception as e:
        print(f"Error on page {page_index}: {e}")
        return None


def main():
    start_page = 1
    end_page = 973
    page_range = range(start_page, end_page + 1)
    
    file_name = "kiosk_외식.csv"
    
    all_dfs = []
    
    # ThreadPoolExecutor로 병렬 데이터 수집
    with ThreadPoolExecutor(max_workers=10) as executor:
        session = requests_retry_session()
        
        future_to_page = {executor.submit(extract_page, session, page): page for page in page_range}
        
        for future in tqdm(as_completed(future_to_page), total=len(page_range), desc="Crawling pages"):
            result_df = future.result()
            if result_df is not None and not result_df.empty:
                all_dfs.append(result_df)
    
    print(f"수집된 페이지 개수: {len(all_dfs)}")
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print("크롤링 성공.")
        print(final_df.head())
        print(f"page_index 유니크 값: {final_df['page_index'].nunique()}")
        print(f"행 개수: {len(final_df)}")

        final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print("데이터 저장 완료.")
        
    else:
        print("크롤링 실패.")      
    
if __name__ == "__main__":
    main()