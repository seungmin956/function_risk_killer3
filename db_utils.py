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

def get_recall_stats_from_db(db_path: str = "./data/fda_recalls.db"):
    """SQLite DB에서 리콜 통계 데이터 추출"""
    
    if not os.path.exists(db_path):
        return {
            'total_recalls': 0,
            'realtime_recalls': 0,
            'database_recalls': 0,
            'realtime_ratio': 0,
            'latest_crawl': '없음'
        }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 총 리콜 건수
        cursor.execute("SELECT COUNT(*) FROM recalls")
        total_recalls = cursor.fetchone()[0]
        
        # 최근 3일간 추가된 데이터 (실시간으로 간주)
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT COUNT(*) FROM recalls 
            WHERE DATE(created_at) >= ?
        """, (three_days_ago,))
        realtime_recalls = cursor.fetchone()[0]
        
        # 기존 DB 데이터
        database_recalls = total_recalls - realtime_recalls
        
        # 실시간 비율
        realtime_ratio = (realtime_recalls / total_recalls * 100) if total_recalls > 0 else 0
        
        # 최근 업데이트 시간
        cursor.execute("""
            SELECT MAX(created_at) FROM recalls
        """)
        latest_result = cursor.fetchone()[0]
        latest_crawl = latest_result if latest_result else '없음'
        
        conn.close()
        
        return {
            'total_recalls': total_recalls,
            'realtime_recalls': realtime_recalls,
            'database_recalls': database_recalls,
            'realtime_ratio': realtime_ratio,
            'latest_crawl': latest_crawl
        }
        
    except Exception as e:
        print(f"DB 통계 조회 오류: {e}")
        return {
            'total_recalls': 0,
            'realtime_recalls': 0,
            'database_recalls': 0,
            'realtime_ratio': 0,
            'latest_crawl': '오류'
        }

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

def get_visualization_data():
    """시각화용 통합 데이터 반환"""
    try:
        # SQLite 통계
        sqlite_stats = get_recall_stats_from_db()
        
        # ChromaDB 통계
        chromadb_count = get_chromadb_stats()
        
        # ChromaDB 수가 더 정확할 수 있으므로 업데이트
        if chromadb_count > 0:
            sqlite_stats['total_recalls'] = chromadb_count
        
        # JSON 파일 확인 (최근 크롤링 여부)
        data_dir = "./data"
        json_files = []
        if os.path.exists(data_dir):
            import glob
            json_pattern = os.path.join(data_dir, "realtime_recalls_*.json")
            json_files = glob.glob(json_pattern)
            json_files.sort(reverse=True)  # 최신 파일 우선
        
        # 최근 JSON 파일이 있으면 실시간 데이터로 카운트
        if json_files:
            latest_json = json_files[0]
            json_time = os.path.getmtime(latest_json)
            json_datetime = datetime.fromtimestamp(json_time)
            
            # 최근 24시간 내 파일이면 실시간으로 간주
            if (datetime.now() - json_datetime).total_seconds() < 86400:  # 24시간
                sqlite_stats['latest_crawl'] = json_datetime.strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            'stats': sqlite_stats,
            'has_data': sqlite_stats['total_recalls'] > 0
        }
        
    except Exception as e:
        print(f"시각화 데이터 생성 오류: {e}")
        return {
            'stats': {
                'total_recalls': 0,
                'realtime_recalls': 0,
                'database_recalls': 0,
                'realtime_ratio': 0,
                'latest_crawl': '오류'
            },
            'has_data': False
        }

def check_new_realtime_data():
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