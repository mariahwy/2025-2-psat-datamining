import pandas as pd
import requests
import time
import json # JSONDecodeError를 위해 추가


def search_place(key, query):
    """
    영업표지로 검색하여 서울시 'bbox' 영역 내의 영업점 검색 결과를 
    (영업점 이름, x, y 좌표) 튜플의 리스트로 반환
    (VWorld Search 2.0 API, Pagination 구현)
    
    Args:
        key (str): VWorld API 키
        query (str): 검색할 영업표지 (예: "스타벅스")

    Returns:
        list: [(title, x, y), (title, x, y), ...] 형태의 튜플 리스트
    """
    all_items = []  # 모든 페이지의 결과를 누적할 리스트
    base_url = "https://api.vworld.kr/req/search"
    current_page = 1
    total_pages = 1  # 우선 1페이지로 초기화

    # [수정됨] 서울시 BBOX 좌표 (EPSG:4326 기준)
    seoul_bbox = "126.734086,37.413294,127.269311,37.715133"

    # API 요청 파라미터 설정
    params = {
        'service': 'search',
        'request': 'search',
        'version': '2.0',
        'query': query,
        'type': 'place',
        'format': 'json',
        'errorformat': 'json',
        'key': key,
        
        'bbox': seoul_bbox,
        
        'crs': 'EPSG:4326',  # BBOX 및 응답 좌표계 (WGS84 위경도)
        'size': 30,          # 한 페이지 당 최대 결과 수
        'page': current_page
    }

    print(f"--- [{query}] 서울시(BBOX) 내 영업점 검색 시작 ---")

    while current_page <= total_pages:
        params['page'] = current_page
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # HTTP 오류가 발생하면 예외 발생
            data = response.json()

            # API 응답 상태 확인
            status = data.get('response', {}).get('status')
            if status != 'OK':
                # 'NOT_FOUND'는 오류가 아니라 단순히 결과가 없는 것이므로 별도 처리
                if status == 'NOT_FOUND':
                    print(f"검색 결과 없음 (Page {current_page}).")
                    break
                
                error_msg = data.get('response', {}).get('error', {}).get('text', 'Unknown API Error')
                print(f"API 오류 (Page {current_page}): {status} - {error_msg}")
                break  # 오류 발생 시 중단

            # [1] 첫 페이지 요청 시, 전체 페이지 수 확인
            if current_page == 1:
                page_info = data.get('response', {}).get('page', {})
                total_pages = int(page_info.get('total', 1))
                total_results = int(data.get('response', {}).get('record', {}).get('total', 0))
                
                if total_results == 0:
                    print("총 0개 결과. 검색을 중단합니다.")
                    break
                    
                print(f"총 {total_results}개 결과, {total_pages} 페이지 확인.")
            
            # [2] 결과 아이템 추출
            result = data.get('response', {}).get('result')
            if not result or 'items' not in result:
                print(f"데이터 없음 (Page {current_page}).")
                current_page += 1
                continue

            # --- [수정됨] 상세 로깅을 위한 카운터 3종 ---
            page_item_count_added = 0      # 1. 실제 추가된 개수
            page_item_count_filtered_seoul = 0 # 2. '서울'이 아니라서 필터링된 개수
            page_item_count_filtered_atm = 0   # 3. [신규] 'ATM'이 포함되어 필터링된 개수
            # -------------------------------------

            for item in result['items']:
                title = item.get('title', 'N/A')
                
                # --- [신규] 1순위 필터: title에 "ATM" 포함 시 제외 ---
                # 대소문자 구분 없이("atm", "Atm" 등) 확인하기 위해 .upper() 사용
                if "ATM" in title.upper():
                    page_item_count_filtered_atm += 1
                    continue # 이 item을 건너뛰고 다음 item으로
                # ---------------------------------------------
                
                # --- 2순위 필터: '서울' 주소 확인 ---
                road_address = item.get('address', {}).get('road', '') 
                
                if "서울" in road_address:
                    # 3. '서울'이 맞을 경우에만 좌표 추출 시도
                    try:
                        point = item['point']
                        x = point['x'] # 경도 (Longitude)
                        y = point['y'] # 위도 (Latitude)
                        
                        all_items.append((title, x, y))
                        page_item_count_added += 1
                        
                    except KeyError:
                        # 주소는 '서울'이지만 좌표값이 없는 경우
                        print(f"경고: '{title}' (서울 주소, Page {current_page})의 좌표 정보가 없습니다. 건너뜁니다.")
                else:
                    # BBOX엔 포함됐으나 주소가 '서울'이 아닌 경우
                    page_item_count_filtered_seoul += 1
                # --- 필터링 로직 끝 ---

            # [수정됨] 로그 메시지를 더 상세하게 변경
            print(f"Page {current_page}/{total_pages} 처리 완료. {page_item_count_added}개 추가 (필터링: 서울주소 {page_item_count_filtered_seoul}개, ATM {page_item_count_filtered_atm}개, 총 {len(all_items)}개)")

            current_page += 1
            
            if current_page <= total_pages:
                time.sleep(0.1) 

        except requests.exceptions.RequestException as e:
            print(f"API 요청 중 네트워크 오류 발생 (Page {current_page}): {e}")
            break
        except json.JSONDecodeError:
            print(f"API 응답이 유효한 JSON이 아닙니다 (Page {current_page}).")
            break
        except Exception as e:
            print(f"알 수 없는 오류 발생 (Page {current_page}): {e}")
            break

    print(f"--- [{query}] 검색 완료: 총 {len(all_items)}개의 영업점을 수집했습니다. ---")
    return all_items


def main():
    try:
        with open('key.txt', 'r', encoding='utf-8') as file:
            key = file.read().strip()
    except Exception as e:
        print(f"key.txt를 읽을 수 없습니다. : {e}")
    
    bank_list = ["우리은행", "농협은행", "신한은행", "하나은행", "기업은행", "국민은행"]
    
    all_results_list = []
    output_csv_filename = "bank_location.csv"
    
    try:
        for query in bank_list:
            print(f"\n---{query} 검색 시작---")
            
            locations = search_place(key, query)
            
            for title, x, y in locations:
                all_results_list.append((query, title, x, y))
    except Exception as e:
        print(f"query 검색 실패 : {e}")
        
    if not all_results_list:
        print("\n최종 결과 0개로 저장 실패.")
    else:
        print(f"\n--- 모든 검색 완료. 총 {len(all_results_list)}개의 지점 수집 ---")
        try:
            # 리스트를 Pandas DataFrame으로 변환
            # QGIS에서 인식하기 쉽도록 x, y 대신 longitude, latitude로 명명
            results_df = pd.DataFrame(
                all_results_list, 
                columns=['bank', 'title', 'longitude', 'latitude']
            )
            
            # CSV 파일로 저장
            results_df.to_csv(output_csv_filename, index=False, encoding='utf-8-sig')
            
            print(f"모든 결과가 '{output_csv_filename}' 파일로 저장되었습니다.")
            
        except Exception as e:
            print(f"CSV 파일 저장 중 오류 발생: {e}")


if __name__ == "__main__":
    main()