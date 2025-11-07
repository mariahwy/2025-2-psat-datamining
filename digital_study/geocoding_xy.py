import pandas as pd
import requests
import time
import json # JSONDecodeError를 위해 추가

def get_coordinate(key, address_query):
    """
    하나의 주소를 VWorld 주소 변환(Geocoding) API를 사용해 좌표로 변환합니다.
    'ROAD' 타입 시도 후, 실패 시 'PARCEL' 타입으로 자동 재시도합니다.
    (VWorld Address API - getCoord)
    
    Args:
        key (str): VWorld API 키
        address_query (str): 검색할 주소 문자열

    Returns:
        tuple: (x, y) 좌표 튜플. 실패 시 (None, None)
    """
    base_url = "https://api.vworld.kr/req/address"
    
    # 주소값이 비어있거나(None) NaN(Not a Number)인 경우 건너뜀
    if not address_query or pd.isna(address_query):
        print(f"--- [SKIP] 주소 문자열이 비어있습니다.")
        return None, None

    # VWorld API는 'ROAD'와 'PARCEL' 타입을 명시해야 함
    # 'ROAD'로 먼저 시도하고, 'NOT_FOUND' 시 'PARCEL'로 재시도
    for address_type in ['ROAD', 'PARCEL']:
        params = {
            'service': 'address',
            'request': 'getcoord',
            'version': '2.0',
            'key': key,
            'format': 'json',
            'errorFormat': 'json',
            'type': address_type,
            'address': address_query,
            'refine': 'true',
            'simple': 'false',
            'crs': 'EPSG:5179' 
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # HTTP 오류가 발생하면 예외 발생
            data = response.json()

            # API 응답 상태 확인
            status = data.get('response', {}).get('status')
            
            if status == 'OK':
                point = data.get('response', {}).get('result', {}).get('point', {})
                x = point.get('x') # 경도 (longitude)
                y = point.get('y') # 위도 (latitude)
                
                if x and y:
                    print(f"--- [SUCCESS] ({address_type}) -> (x:{x}, y:{y})")
                    return x, y # 성공 시 좌표 반환
                else:
                    print(f"--- [FAIL] ({address_type}) 상태는 OK지만, 좌표가 없습니다.")
            
            elif status == 'NOT_FOUND':
                print(f"--- [INFO] ({address_type}) 타입으로 주소를 찾지 못했습니다.")
            
            else: # ERROR
                error_msg = data.get('response', {}).get('error', {}).get('text', 'Unknown API Error')
                print(f"--- [API_ERROR] ({address_type}) -> {status}: {error_msg}")
                return None, None # API 에러 시 재시도 없이 중단

        except requests.exceptions.RequestException as e:
            print(f"--- [NET_ERROR] '{address_query}' -> 네트워크 오류: {e}")
            return None, None
        except json.JSONDecodeError:
            print(f"--- [JSON_ERROR] '{address_query}' -> JSON 파싱 오류")
            return None, None
        except Exception as e:
            print(f"--- [SYS_ERROR] '{address_query}' -> 알 수 없는 오류: {e}")
            return None, None

    # 'ROAD'와 'PARCEL' 모두 실패한 경우
    print(f"--- [FINAL_FAIL] '{address_query}' -> 모든 타입으로 좌표 변환에 실패했습니다.")
    return None, None


def main():
    try:
        with open('key.txt', 'r', encoding='utf-8') as file:
            key = file.read().strip()
    except Exception as e:
        print(f"key.txt를 읽을 수 없습니다. : {e}")
        return
    
    input_filename = "주소목록.csv"
    output_filename = "주소목록_geocoded.csv"
    
    try:
        df = pd.read_csv(input_filename, encoding='utf-8')
        print(f"'{input_filename}' 파일 읽기 성공. (총 {len(df)}개 주소 변환 시도)")
    except Exception as e:
        print(f"'{input_filename}' 파일을 읽을 수 없습니다. : {e}")
        return
    
    all_results_list = [] # 모든 원본 데이터 + 좌표를 저장할 리스트
    
    original_columns = list(df.columns)
    
    for index, row in df.iterrows():
        address = row.get('주소')
        
        print(f"\n--- [ {index + 1} / {len(df)} ] 주소 검색: {address} ---")
        
        x, y = get_coordinate(key, address)
        
        original_data = list(row)

        all_results_list.append(original_data + [x, y])
        
        time.sleep(0.1) 
            
    if not all_results_list:
        print("\n최종 결과가 비어있어 CSV 파일을 생성하지 않습니다.")
    else:
        print(f"\n--- 모든 주소 변환 완료. 총 {len(all_results_list)}건 처리 ---")
        try:
            # [수정] 새 DataFrame의 컬럼명 설정
            # (원본 컬럼명 리스트 + 새 좌표 컬럼명)
            new_columns = original_columns + ['longitude', 'latitude']
            
            results_df = pd.DataFrame(
                all_results_list, 
                columns=new_columns # [수정] 동적으로 생성된 컬럼명 사용
            )
            
            results_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            
            print(f"성공! 모든 결과가 '{output_filename}' 파일로 저장되었습니다.")
            
        except Exception as e:
            print(f"CSV 파일 저장 중 오류 발생: {e}")


if __name__ == "__main__":
    main()