# db_utils.py
import os
import sqlite3
import chromadb
from chromadb.utils import embedding_functions
import uuid
import time
import re
import glob
from datetime import datetime, timedelta
from typing import Dict, Any, List

def save_to_sqlite(data_list: List[Dict], db_path: str = "./data/fda_recalls.db"):
    """
    데이터 리스트를 SQLite DB에 직접 저장
    paste-3.txt 로직 기반, JSON 파일 없이 data_list 직접 처리
    """
    
    print(f"🔄 SQLite 저장 시작: {len(data_list)}개 레코드")
    
    # 데이터 폴더 생성
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # SQLite 연결 및 테이블 생성
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 테이블 생성 (존재하지 않는 경우)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS recalls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        document_type TEXT,
        url TEXT UNIQUE,
        company_announcement_date DATE,
        fda_publish_date DATE,
        company_name TEXT,
        brand_name TEXT,
        recall_reason TEXT,
        recall_reason_detail TEXT,
        product_type TEXT,
        content TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_sql)
    
    # 인덱스 생성 (검색 성능 향상)
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_company_name ON recalls(company_name)",
        "CREATE INDEX IF NOT EXISTS idx_brand_name ON recalls(brand_name)",
        "CREATE INDEX IF NOT EXISTS idx_recall_reason ON recalls(recall_reason)",
        "CREATE INDEX IF NOT EXISTS idx_fda_publish_date ON recalls(fda_publish_date)",
        "CREATE INDEX IF NOT EXISTS idx_product_type ON recalls(product_type)",
        "CREATE INDEX IF NOT EXISTS idx_url ON recalls(url)"
    ]
    
    for index_sql in indexes:
        cursor.execute(index_sql)
    
    # 데이터 삽입 SQL
    insert_sql = """
    INSERT OR REPLACE INTO recalls (
        document_type, url, company_announcement_date, fda_publish_date,
        company_name, brand_name, recall_reason, recall_reason_detail,
        product_type, content
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    converted_count = 0
    for i, record in enumerate(data_list):
        try:
            # 필드 매핑 및 정제
            cleaned_data = clean_record_for_sqlite(record)
            
            data = (
                cleaned_data['document_type'],
                cleaned_data['url'],
                cleaned_data['company_announcement_date'],
                cleaned_data['fda_publish_date'],
                cleaned_data['company_name'],
                cleaned_data['brand_name'],
                cleaned_data['recall_reason'],
                cleaned_data['recall_reason_detail'],
                cleaned_data['product_type'],
                cleaned_data['content']
            )
            
            cursor.execute(insert_sql, data)
            converted_count += 1
            
        except Exception as e:
            print(f"  ⚠️ 레코드 {i} SQLite 저장 오류: {e}")
            print(f"     URL: {record.get('url', 'N/A')}")
            continue
    
    conn.commit()
    conn.close()
    
    print(f"✅ SQLite 저장 완료: {converted_count}/{len(data_list)}개 레코드")
    return converted_count

def clean_record_for_sqlite(record: Dict[str, Any]) -> Dict[str, Any]:
    """SQLite용 레코드 정제"""
    
    cleaned = {}
    
    # 현재 JSON 구조 필드들을 직접 매핑
    cleaned['document_type'] = record.get('document_type', 'recall')
    cleaned['url'] = record.get('url', '')
    cleaned['company_announcement_date'] = record.get('company_announcement_date', None)
    cleaned['fda_publish_date'] = record.get('fda_publish_date', None)
    cleaned['company_name'] = record.get('company_name', '')
    cleaned['brand_name'] = record.get('brand_name', '')
    cleaned['recall_reason'] = record.get('recall_reason', '')
    cleaned['recall_reason_detail'] = record.get('recall_reason_detail', '')
    cleaned['product_type'] = record.get('product_type', '')
    cleaned['content'] = record.get('content', '')
    
    # 빈 문자열이나 None 값 정리
    for key, value in cleaned.items():
        if value in ['', 'N/A', 'null', None]:
            cleaned[key] = None if key in ['company_announcement_date', 'fda_publish_date'] else ''
    
    # 텍스트 필드 길이 제한 (SQLite 성능 고려)
    if cleaned['content'] and len(cleaned['content']) > 15000:
        cleaned['content'] = cleaned['content'][:15000] + '...'
    
    return cleaned

def save_to_chromadb(data_list: List[Dict], 
                    collection_name: str = "FDA_recalls",
                    db_path: str = "./data/chroma_db_recall"):
    """
    데이터 리스트를 ChromaDB에 직접 저장
    paste-2.txt 로직 기반, JSON 파일 없이 data_list 직접 처리
    """
    
    print(f"🔍 ChromaDB 저장 시작: {len(data_list)}개 문서")
    
    # ChromaDB 클라이언트 초기화
    chroma_client = chromadb.PersistentClient(path=db_path)
    
    # OpenAI 임베딩 함수 설정
    openai_api_key = os.getenv('OPENAI_API_KEY')
    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY 환경변수가 설정되지 않았습니다")
    
    basic_ef = embedding_functions.OpenAIEmbeddingFunction(
        api_key=openai_api_key,
        model_name="text-embedding-3-small"
    )
    
    # 컬렉션 가져오기 또는 생성
    try:
        collection = chroma_client.get_collection(
            name=collection_name,
            embedding_function=basic_ef
        )
        print(f"✅ 기존 컬렉션 '{collection_name}' 연결됨")
    except:
        collection = chroma_client.create_collection(
            name=collection_name,
            embedding_function=basic_ef,
            metadata={"description": "FDA 리콜 사례 데이터 - 증분 업데이트"}
        )
        print(f"🆕 새 컬렉션 '{collection_name}' 생성됨")
    
    # 배치 처리 설정
    BATCH_SIZE = 30
    total_chunks = 0
    processed_items = 0
    
    for batch_start in range(0, len(data_list), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(data_list))
        batch_data = data_list[batch_start:batch_end]
        
        ids = []
        documents = []
        metadatas = []
        
        for i, item in enumerate(batch_data, batch_start):
            try:
                # URL을 고유 ID로 사용
                base_url = item.get("url", f"recall_{i}")
                
                # content 필드에서 텍스트 추출
                content_text = item.get("content", "")
                
                # 유효성 체크
                if not content_text or len(content_text.strip()) < 20:
                    print(f"❗ {i}번 문서 스킵됨 (내용 없음): {base_url}")
                    continue
                
                # 문단 기준 청킹 적용
                chunks = chunk_content_by_paragraphs(content_text, max_chunk_size=1500, overlap=150)
                
                # 각 청크마다 별도 문서로 저장
                for chunk_idx, chunk_content in enumerate(chunks):
                    if len(chunk_content.strip()) < 30:  # 너무 짧은 청크 제외
                        continue
                    
                    # 청크별 고유 ID 생성
                    chunk_id = f"{base_url}_chunk_{chunk_idx}" if len(chunks) > 1 else base_url
                    
                    # 기존 문서 확인 (중복 방지)
                    try:
                        existing = collection.get(ids=[chunk_id])
                        if existing['ids']:
                            print(f"🔄 기존 문서 업데이트: {chunk_id}")
                            # 기존 문서 삭제 후 새로 추가
                            collection.delete(ids=[chunk_id])
                    except:
                        pass  # 새 문서
                    
                    # 메타데이터 구성
                    raw_metadata = {
                        "document_type": item.get("document_type", "recall"),
                        "url": item.get("url", ""),
                        "company_announcement_date": item.get("company_announcement_date", ""),
                        "fda_publish_date": item.get("fda_publish_date", ""),
                        "company_name": item.get("company_name", ""),
                        "brand_name": item.get("brand_name", ""),
                        "recall_reason": item.get("recall_reason", ""),
                        "recall_reason_detail": item.get("recall_reason_detail", ""),
                        "product_type": item.get("product_type", ""),
                        
                        # 청킹 관련 메타데이터
                        "chunk_index": chunk_idx,
                        "total_chunks": len(chunks),
                        "is_chunked": len(chunks) > 1
                    }
                    
                    # None 값 필터링
                    metadata = filter_none_values(raw_metadata)
                    
                    ids.append(chunk_id)
                    documents.append(chunk_content)
                    metadatas.append(metadata)
                
                processed_items += 1
                
            except Exception as e:
                print(f"항목 {i} ChromaDB 처리 중 오류: {e}")
                continue
        
        # 컬렉션에 추가
        if ids:
            try:
                collection.add(ids=ids, documents=documents, metadatas=metadatas)
                total_chunks += len(ids)
                print(f"배치 {batch_start // BATCH_SIZE + 1}: {len(ids)}개 청크 추가")
                time.sleep(1)  # API 부하 방지
            except Exception as e:
                print(f"배치 {batch_start // BATCH_SIZE + 1} ChromaDB 저장 오류: {e}")
                continue
    
    print(f"✅ ChromaDB 저장 완료:")
    print(f"   - 처리된 문서: {processed_items}/{len(data_list)}개")
    print(f"   - 생성된 청크: {total_chunks}개")
    
    return total_chunks

def chunk_content_by_paragraphs(content_text, max_chunk_size=1500, overlap=200):
    """
    문단(\n\n) 기준으로 콘텐츠를 청킹하는 함수 
    paste-2.txt에서 가져옴
    """
    if not content_text or len(content_text.strip()) < 50:
        return [content_text]
    
    # 문단 분리 (\n\n 기준)
    paragraphs = re.split(r'\n\s*\n', content_text.strip())
    
    # 빈 문단 제거
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    
    if len(paragraphs) <= 1:
        # 문단이 하나뿐이면 문장 기준으로 분리
        sentences = re.split(r'(?<=[.!?])\s+', content_text)
        paragraphs = sentences
    
    chunks = []
    current_chunk = ""
    
    for paragraph in paragraphs:
        # 현재 청크에 문단을 추가했을 때 크기 확인
        potential_chunk = current_chunk + "\n\n" + paragraph if current_chunk else paragraph
        
        if len(potential_chunk) <= max_chunk_size:
            current_chunk = potential_chunk
        else:
            # 현재 청크가 있으면 저장
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            # 새 청크 시작 (오버랩 고려)
            if overlap > 0 and current_chunk:
                overlap_text = current_chunk[-overlap:].strip()
                current_chunk = overlap_text + "\n\n" + paragraph
            else:
                current_chunk = paragraph
            
            # 단일 문단이 너무 큰 경우 강제 분할
            if len(current_chunk) > max_chunk_size:
                # 문장 단위로 재분할
                long_sentences = re.split(r'(?<=[.!?])\s+', current_chunk)
                temp_chunk = ""
                
                for sentence in long_sentences:
                    if len(temp_chunk + sentence) <= max_chunk_size:
                        temp_chunk += " " + sentence if temp_chunk else sentence
                    else:
                        if temp_chunk:
                            chunks.append(temp_chunk.strip())
                        temp_chunk = sentence
                
                current_chunk = temp_chunk
    
    # 마지막 청크 추가
    if current_chunk.strip():
        chunks.append(current_chunk.strip())
    
    return chunks if chunks else [content_text]

def filter_none_values(metadata_dict):
    """None 값과 빈 문자열을 필터링하는 함수"""
    filtered = {}
    for key, value in metadata_dict.items():
        if value is not None:
            # 빈 문자열도 체크
            if isinstance(value, str) and value.strip():
                filtered[key] = value
            elif not isinstance(value, str):
                filtered[key] = value
        # None이거나 빈 문자열인 경우 해당 키는 제외
    return filtered

def get_recent_json_file_count():
    """최근 JSON 파일에서 실제 추가된 데이터 개수 확인"""
    try:
        data_dir = "./data"
        if not os.path.exists(data_dir):
            return 0
        
        import json
        import glob
        from datetime import datetime, timedelta  # ✅ import 추가
        
        # 최근 7일 이내 JSON 파일들 찾기
        json_pattern = os.path.join(data_dir, "realtime_recalls_*.json")
        json_files = glob.glob(json_pattern)
        
        if not json_files:
            print("📂 realtime_recalls_*.json 파일 없음")
            return 0
        
        # 가장 최근 파일 선택
        latest_json = max(json_files, key=os.path.getmtime)
        json_time = os.path.getmtime(latest_json)
        json_datetime = datetime.fromtimestamp(json_time)
        
        # 7일 이내 파일인지 확인
        days_ago = (datetime.now() - json_datetime).days
        print(f"📅 최신 JSON 파일: {os.path.basename(latest_json)} ({days_ago}일 전)")
        
        if days_ago > 7:
            print(f"⏰ {days_ago}일 전 파일이므로 최근 데이터 아님")
            return 0
        
        # JSON 파일 내용 읽어서 실제 개수 확인
        try:
            with open(latest_json, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                if isinstance(json_data, list):
                    print(f"✅ JSON 파일에서 {len(json_data)}건 확인")
                    return len(json_data)
                else:
                    return 0
        except Exception as e:
            print(f"❌ JSON 파일 읽기 오류: {e}")
            return 0
            
    except Exception as e:
        print(f"JSON 파일 개수 확인 오류: {e}")
        return 0

def get_conservative_recent_data_count(db_path: str = "./data/fda_recalls.db"):
    """보수적인 최근 데이터 계산 (현실적인 접근)"""
    try:
        # 1. JSON 파일 기준 확인
        json_count = get_recent_json_file_count()
        if json_count > 0:
            return json_count
        
        # 2. JSON 파일이 없으면 0으로 가정 (보수적 접근)
        # 이유: 실제로 새 크롤링이 없었다면 새 데이터도 없음
        return 0
        
    except:
        return 0

def get_chromadb_stats(db_path: str = "./data/chroma_db_recall", collection_name: str = "FDA_recalls"):
    """ChromaDB에서 문서 수 확인"""
    try:
        chroma_client = chromadb.PersistentClient(path=db_path)
        collection = chroma_client.get_collection(collection_name)
        total_documents = collection.count()
        return total_documents
    except Exception as e:
        print(f"ChromaDB 조회 오류: {e}")
        return 0
    
def get_realistic_recall_stats(db_path: str = "./data/fda_recalls.db"):
    """실제 상황에 맞는 리콜 통계 데이터 추출 - 최종 수정판"""
    
    if not os.path.exists(db_path):
        return {
            'total_recalls': 0,
            'recent_added': 0,
            'baseline_data': 0,
            'recent_period': '이번 주',
            'last_update': '정보 없음',
            'update_method': '수동',
            'has_new_data': False,
            'days_since_update': 999
        }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 🔧 핵심 수정: 고유 URL 수로 실제 리콜 사례 계산
        cursor.execute("SELECT COUNT(DISTINCT url) FROM recalls")
        actual_recall_cases = cursor.fetchone()[0]  # 734건
        
        # 🔧 최근 추가 데이터 계산 - JSON 파일 기반으로만 판단
        recent_added = get_recent_json_file_count()
        
        # 🚨 JSON 파일이 없으면 최근 추가는 0으로 가정 (보수적 접근)
        if recent_added == 0:
            print("📋 JSON 파일 없음 - 모든 데이터를 기존 데이터로 분류")
        
        # 기존 데이터 = 전체 - 최근 추가
        baseline_data = actual_recall_cases - recent_added
        
        # 📅 실제 마지막 크롤링 시간 확인
        last_update_info = get_last_crawling_time()
        
        # 📊 새로운 데이터 여부 체크
        has_new_data = recent_added > 0 and last_update_info['days_ago'] <= 7
        
        conn.close()
        
        return {
            'total_recalls': actual_recall_cases,  # 734건 (실제 사례)
            'recent_added': recent_added,           # 0건 (JSON 기반)
            'baseline_data': baseline_data,         # 734건 (전체 - 최근)
            'recent_period': '이번 주',
            'last_update': last_update_info['datetime'],
            'update_method': last_update_info['method'],
            'has_new_data': has_new_data,
            'days_since_update': last_update_info['days_ago'],
            # 🆕 추가 정보 (선택적 표시용)
            'chromadb_chunks': get_chromadb_stats(),  # 1,270개 (검색용)
            'chunk_ratio': round(get_chromadb_stats() / actual_recall_cases, 2) if actual_recall_cases > 0 else 0
        }
        
    except Exception as e:
        print(f"DB 통계 조회 오류: {e}")
        return {
            'total_recalls': 0,
            'recent_added': 0,
            'baseline_data': 0,
            'recent_period': '이번 주',
            'last_update': '오류',
            'update_method': '알 수 없음',
            'has_new_data': False,
            'days_since_update': 999
        }

def get_last_crawling_time():
    """실제 마지막 크롤링 시간 확인 (JSON 파일 + DB 조합)"""
    try:
        from datetime import datetime, timedelta  # ✅ import 추가
        data_dir = "./data"
        
        # 1. JSON 파일에서 최근 크롤링 시간 확인
        if os.path.exists(data_dir):
            import glob
            import re
            json_pattern = os.path.join(data_dir, "realtime_recalls_*.json")
            json_files = glob.glob(json_pattern)
            
            if json_files:
                # 최신 JSON 파일 선택
                latest_json = max(json_files, key=os.path.getmtime)
                json_time = os.path.getmtime(latest_json)
                json_datetime = datetime.fromtimestamp(json_time)
                
                # 파일명에서 타임스탬프 추출 (더 정확함)
                filename = os.path.basename(latest_json)
                # realtime_recalls_20250116_1430.json 형식
                timestamp_match = re.search(r'(\d{8})_(\d{4})', filename)
                if timestamp_match:
                    date_str, time_str = timestamp_match.groups()
                    # 20250116_1430 → 2025-01-16 14:30
                    formatted_datetime = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}"
                    try:
                        crawl_datetime = datetime.strptime(formatted_datetime, "%Y-%m-%d %H:%M")
                    except:
                        crawl_datetime = json_datetime
                else:
                    crawl_datetime = json_datetime
                
                days_ago = (datetime.now() - crawl_datetime).days
                
                return {
                    'datetime': crawl_datetime.strftime("%Y-%m-%d %H:%M"),
                    'method': '주간 자동' if days_ago <= 7 else '수동',
                    'days_ago': days_ago,
                    'source': 'json_file'
                }
        
        # 2. JSON 파일이 없으면 기본값
        return {
            'datetime': '정보 없음',
            'method': '수동',
            'days_ago': 999,
            'source': 'none'
        }
        
    except Exception as e:
        print(f"크롤링 시간 확인 오류: {e}")
        return {
            'datetime': '오류',
            'method': '알 수 없음',
            'days_ago': 999,
            'source': 'error'
        }

def get_improved_visualization_data():
    """개선된 시각화용 데이터 반환 - 수정판"""
    try:
        # 실제 상황에 맞는 통계
        realistic_stats = get_realistic_recall_stats()
        
        # 🔧 ChromaDB 카운트는 별도 정보로만 저장 (덮어쓰지 않음)
        chromadb_count = get_chromadb_stats()
        
        # ✅ 수정: 실제 사례 수 유지하고 ChromaDB는 참고용으로만
        realistic_stats['chromadb_chunks'] = chromadb_count  # 이미 포함되어 있음
        
        return {
            'stats': realistic_stats,
            'has_data': realistic_stats['total_recalls'] > 0,
            'data_source': 'sqlite_primary_chroma_reference'
        }
        
    except Exception as e:
        print(f"개선된 시각화 데이터 생성 오류: {e}")
        return {
            'stats': {
                'total_recalls': 0,
                'recent_added': 0,
                'baseline_data': 0,
                'recent_period': '이번 주',
                'last_update': '오류',
                'update_method': '알 수 없음',
                'has_new_data': False
            },
            'has_data': False
        }


def check_recent_data_update():
    """새로운 실시간 데이터가 있는지 확인"""
    try:
        data_dir = "./data"
        if not os.path.exists(data_dir):
            return False
            
        # 최근 1시간 내 생성된 JSON 파일 확인
        import glob
        json_pattern = os.path.join(data_dir, "realtime_recalls_*.json")
        json_files = glob.glob(json_pattern)
        
        current_time = datetime.now()
        for json_file in json_files:
            file_time = datetime.fromtimestamp(os.path.getmtime(json_file))
            if (current_time - file_time).total_seconds() < 3600:  # 1시간
                return True
        return False
    except:
        return False