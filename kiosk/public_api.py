import pandas as pd
import requests
import time
import json

def fetch_all_kiosk_data(key):
    """
    서울시 무인민원발급기 정보를 API의 모든 페이지에서 수집합니다.
    (TbKioskInfo 서비스)
    
    Args:
        key (str): 서울시 열린데이터 광장 API 키

    Returns:
        list: 수집된 모든 데이터 행(row)의 리스트
    """
    all_data_rows = []
    service_name = "TbKioskInfo"
    page_size = 1000  # API가 한 번에 1000개까지 허용
    start_index = 1
    
    print("--- 서울시 무인민원발급기 데이터 수집 시작 ---")

    while True:
        end_index = start_index + page_size - 1
        
        # API 요청 URL 구성
        url = f"http://openapi.seoul.go.kr:8088/{key}/json/{service_name}/{start_index}/{end_index}/"
        
        print(f"데이터 요청 중... (행: {start_index} ~ {end_index})")
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # HTTP 오류가 발생하면 예외 발생
            data = response.json()

            # 1. API 응답에 서비스 키(TbKioskInfo)가 있는지 확인
            if service_name not in data:
                # 1-1. 서비스 키가 없다면, 오류(RESULT) 또는 데이터 없음(INFO-200) 확인
                if "RESULT" in data:
                    error_code = data["RESULT"].get("CODE", "N/A")
                    error_msg = data["RESULT"].get("MESSAGE", "알 수 없는 오류")
                    
                    if error_code == "INFO-200":
                        print(f"데이터 없음 (INFO-200): {error_msg}. 수집을 종료합니다.")
                    else:
                        print(f"API 오류: {error_code} - {error_msg}")
                else:
                    print("알 수 없는 API 응답입니다.", data)
                break # 오류 또는 데이터 없음이므로 루프 중단

            # 2. 정상 응답에서 데이터(row) 추출
            result = data[service_name]
            rows = result.get("row", [])
            
            # 3. 'row'가 비어있으면 마지막 페이지였으므로 중단
            if not rows:
                print("수집할 데이터가 더 이상 없습니다. 수집 완료.")
                break
                
            # 4. 수집된 데이터를 리스트에 추가
            all_data_rows.extend(rows)
            
            # 5. 다음 페이지 요청을 위해 시작 인덱스 변경
            start_index += page_size
            
            # (선택) API 과부하 방지를 위한 짧은 지연
            time.sleep(0.1) 

        except requests.exceptions.RequestException as e:
            print(f"HTTP 요청 중 오류 발생: {e}")
            break
        except json.JSONDecodeError:
            print(f"API 응답이 유효한 JSON이 아닙니다 (응답: {response.text[:100]}...)")
            break
        except Exception as e:
            print(f"알 수 없는 오류 발생: {e}")
            break
            
    return all_data_rows


def main():
    try:
        with open('public_key.txt', 'r', encoding='utf-8') as file:
            api_key = file.read().strip()
    except Exception as e:
        print(f"key.txt 파일을 읽을 수 없습니다: {e}")
        return # 키가 없으면 종료

    # 1. API에서 모든 데이터 수집
    kiosk_data_list = fetch_all_kiosk_data(api_key)
    
    if not kiosk_data_list:
        print("수집된 데이터가 없어 CSV 파일을 생성하지 않습니다.")
        return
        
    print(f"\n--- 총 {len(kiosk_data_list)} 건의 데이터 수집 완료 ---")

    # 2. Pandas DataFrame으로 변환
    try:
        df = pd.DataFrame(kiosk_data_list)
        
        # 3. 요청한 4개의 컬럼만 선택
        required_columns = [
            "MGTNO",        # 관리번호
            "OPNSFTEAMNM",  # 시군구명
            "KIOSKNM",      # 무인민원발급기명
            "ESBPLCADDR"    # 설치장소주소
        ]
        
        # 혹시 모를 상황 대비, 실제 존재하는 컬럼만 필터링
        available_columns = [col for col in required_columns if col in df.columns]
        
        if not available_columns:
            print("데이터에 요청한 컬럼이 하나도 없습니다. 수집된 원본 데이터를 확인하세요.")
            return

        print(f"선택된 컬럼: {available_columns}")
        df_selected = df[available_columns]
        
        # 4. CSV 파일로 저장
        output_filename = "seoul_kiosk_list.csv"
        
        df_selected.to_csv(output_filename, index=False, encoding='utf-8-sig')
        
        print(f"성공! 데이터가 '{output_filename}' 파일로 저장되었습니다.")

    except Exception as e:
        print(f"DataFrame 생성 또는 CSV 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    main()