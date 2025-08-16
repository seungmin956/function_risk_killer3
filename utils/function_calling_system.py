# utils/function_calling_system.py

import os
import json
import sqlite3
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.tools import tool
from functools import lru_cache
from utils.prompts.recall_prompts import RecallPrompts

load_dotenv()

# 전역 변수 - 시스템 컴포넌트들
_sqlite_conn = None
_vectorstore = None  
_logical_processor = None
_db_initialized = False

def initialize_sqlite_db(db_path="./data/fda_recalls.db"):
    """SQLite 데이터베이스 연결 초기화 (스레드 안전)"""
    try:
        if not os.path.exists(db_path):
            print(f"❌ SQLite 데이터베이스가 존재하지 않습니다: {db_path}")
            return None
        
        # 🔧 스레드 안전 설정
        conn = sqlite3.connect(
            db_path, 
            check_same_thread=False,  # 스레드 안전성 해제
            timeout=30.0  # 타임아웃 설정
        )
        conn.row_factory = sqlite3.Row
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM recalls")
        total_records = cursor.fetchone()['count']
        print(f"✅ SQLite 연결 성공: {total_records}개 레코드")
        
        return conn
        
    except Exception as e:
        print(f"❌ SQLite 연결 실패: {e}")
        return None

def initialize_recall_vectorstore():
    """ChromaDB 벡터스토어 초기화"""
    persist_dir = "./data/chroma_db_recall"
    
    if os.path.exists(persist_dir) and os.listdir(persist_dir):
        try:
            print("기존 리콜 벡터스토어를 로드합니다...")
            embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
            
            vectorstore = Chroma(
                persist_directory=persist_dir,
                embedding_function=embeddings,
                collection_name="FDA_recalls"
            )
            
            collection = vectorstore._collection
            doc_count = collection.count()
            print(f"✅ 리콜 벡터스토어 로드 완료 ({doc_count}개 문서)")
            return vectorstore
                
        except Exception as e:
            print(f"⚠️ 벡터스토어 로드 실패: {e}")
            return None
    else:
        print("⚠️ 벡터스토어 폴더가 존재하지 않습니다")
        return None
    
def parse_relative_dates(period_text: str) -> str:
    """상대적 날짜 표현을 절대 연도로 변환 (2025년 기준)"""
    import datetime
    
    current_year = datetime.datetime.now().year  # 2025
    
    # 🔧 올바른 한국어 표현 매핑
    korean_mappings = {
        "올해": str(current_year),           # 2025 ✅
        "작년": str(current_year - 1),       # 2024 ✅  
        "재작년": str(current_year - 2),     # 2023 ✅
        "이번년": str(current_year),         # 2025
        "현재": str(current_year),           # 2025
        "지난해": str(current_year - 1),     # 2024 ✅
        "전년": str(current_year - 1),       # 2024 ✅
        "금년": str(current_year),           # 2025
        "작년도": str(current_year - 1),     # 2024
        "올해년도": str(current_year),       # 2025
    }
    
    period_lower = period_text.lower().strip()
    
    # 한국어 매핑 확인
    for korean, year in korean_mappings.items():
        if korean in period_lower:
            print(f"🔧 날짜 매핑: '{period_text}' → {year}년 (현재: {current_year})")
            return year
    
    # 숫자인 경우 그대로 반환
    if period_text.isdigit() and len(period_text) == 4:
        return period_text
    
    # 인식하지 못한 경우 현재 연도 반환
    print(f"⚠️ 날짜 인식 실패: '{period_text}' → 기본값 {current_year}년 사용")
    return str(current_year)

@lru_cache(maxsize=512) # 동일 키워드가 반복 호출될 때 속도/비용 줄일 수 있음
def translate_to_english(korean_text: str) -> str:
    """한국어 텍스트를 영어로 번역하는 함수"""
    from langchain_openai import ChatOpenAI
    
    try:
        translator = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0
        )
        
        translation_prompt = f"""
다음 한국어 텍스트를 영어로 정확히 번역해주세요. 
식품, 리콜, 알레르겐 관련 전문 용어는 FDA 표준 용어를 사용하세요.

한국어: {korean_text}
영어:"""
        
        response = translator.invoke([{"role": "user", "content": translation_prompt}])
        english_text = response.content.strip()
        
        print(f"🔄 번역: '{korean_text}' → '{english_text}'")
        return english_text
        
    except Exception as e:
        print(f"번역 오류: {e}")
        # 번역 실패 시 기존 키워드 매핑 사용
        return korean_text

# 상세 오염원/병원체 감지용 헬퍼 추가
_DETAIL_TERMS = {
    "salmonella", "리스테리아", "listeria", "listeria monocytogenes",
    "e. coli", "ecoli", "escherichia", "norovirus", "노로바이러스",
    "campylobacter", "shigella", "clostridium", "botulinum"
}

def _looks_like_detail(value: Optional[str]) -> bool:
    if not value:
        return False
    v = value.lower()
    v_en = translate_to_english(value).lower()
    return any(k in v or k in v_en for k in _DETAIL_TERMS)

def get_recall_vectorstore():
    """tab_recall.py 호환용 함수"""
    return initialize_recall_vectorstore()

def _get_system_components():
    global _sqlite_conn, _vectorstore, _db_initialized
    
    if not _db_initialized:
        _sqlite_conn = initialize_sqlite_db()
        _vectorstore = initialize_recall_vectorstore()
        _db_initialized = True
    
    return _sqlite_conn, _vectorstore, None  

# 스마트 필드 매핑 함수 (질문 유형에 따른 자동 필드 선택)
def smart_count_recalls(query: str, **filters) -> Dict[str, Any]:
    """
    질문 유형을 분석해서 적절한 필드로 자동 매핑하는 래퍼 함수
    
    사용 예시:
    - "계란 관련 리콜 총 몇 건?" → keyword="계란" (모든 필드 검색)
    - "2024년 살모넬라 건수는?" → year="2024", recall_detail="살모넬라"
    - "몬델리즈 회사 리콜 몇 건?" → company="몬델리즈"
    """
    
    query_lower = query.lower()
    
    # 🎯 키워드 패턴 분석
    specific_contaminants = {
        "살모넬라": "salmonella",
        "리스테리아": "listeria", 
        "대장균": "e.coli",
        "클로스트리듐": "clostridium"
    }
    
    allergen_keywords = {
        "우유": "milk",
        "계란": "egg", 
        "견과류": "tree nuts",
        "땅콩": "peanut",
        "콩": "soy",
        "밀": "wheat"
    }
    
    product_categories = {
        "과자": "snacks",
        "유제품": "dairy",
        "해산물": "seafood", 
        "육류": "meat",
        "채소": "vegetables"
    }
    
    # 자동 필드 매핑
    auto_filters = filters.copy()
    
    # 1. 구체적인 오염물질 감지
    for ko_term, en_term in specific_contaminants.items():
        if ko_term in query or en_term in query_lower:
            auto_filters["recall_reason_detail"] = ko_term
            break
    
    # 2. 알레르겐 감지 (알레르겐 관련 질문)
    for ko_term, en_term in allergen_keywords.items():
        if ko_term in query or en_term in query_lower:
            if "알레르겐" in query or "allergen" in query_lower:
                auto_filters["recall_reason_detail"] = f"{ko_term} 알레르겐"
            else:
                auto_filters["keyword"] = ko_term  # 통합 검색
            break
    
    # 3. 제품 카테고리 감지
    for ko_term, en_term in product_categories.items():
        if ko_term in query or en_term in query_lower:
            auto_filters["product_type"] = ko_term
            break
    
    # 4. 연도 추출
    import re
    year_match = re.search(r'(20\d{2})', query)
    if year_match and not auto_filters.get("year"):
        auto_filters["year"] = year_match.group(1)
    
    print(f"🧠 스마트 매핑: '{query}' → {auto_filters}")
    
    return count_recalls(**auto_filters)

# 스마트 순위 분석 함수 (smart_count_recalls 스타일)
def smart_rank_by_field(query: str, limit: int = 10, **filters) -> Dict[str, Any]:
    """
    질문을 분석해서 적절한 필드로 자동 순위 분석
    
    사용 예시:
    - "복합 가공식품 주요 리콜 사유 4가지" → field="recall_reason", product_type="복합 가공식품", limit=4
    - "2024년 상위 회사 5곳" → field="company", year="2024", limit=5  
    - "알레르겐 관련 주요 브랜드" → field="brand", keyword="알레르겐"
    """
    
    query_lower = query.lower()
    
    # 🎯 필드 타입 자동 감지
    field_patterns = {
        "회사": "company",
        "company": "company", 
        "기업": "company",
        
        "브랜드": "brand",
        "brand": "brand",
        "상표": "brand",
        
        "제품": "product_type",
        "product": "product_type",
        "식품": "product_type",
        
        "사유": "recall_reason", 
        "원인": "recall_reason",
        "reason": "recall_reason",
        
        "오염물질": "recall_detail",
        "세균": "recall_detail",
        "알레르겐": "recall_detail",
        "contaminant": "recall_detail"
    }
    
    # 자동 필드 감지
    detected_field = "recall_reason"  # 기본값
    for pattern, field_type in field_patterns.items():
        if pattern in query_lower:
            detected_field = field_type
            break
    
    # 숫자 패턴에서 limit 추출
    import re
    number_matches = re.findall(r'(\d+)', query)
    if number_matches:
        detected_limit = min(int(number_matches[0]), 20)  # 최대 20개
        if detected_limit > 0:
            limit = detected_limit
    
    # 제품 카테고리 감지
    product_categories = {
        "복합": "processed",
        "가공식품": "processed foods", 
        "processed": "processed foods",
        "과자": "snacks",
        "유제품": "dairy", 
        "해산물": "seafood",
        "육류": "meat"
    }
    
    auto_filters = filters.copy()
    for ko_term, en_term in product_categories.items():
        if ko_term in query:
            auto_filters["product_type"] = ko_term
            break
    
    # 연도 추출
    year_match = re.search(r'(20\d{2})', query)
    if year_match and not auto_filters.get("year"):
        auto_filters["year"] = year_match.group(1)
    
    # 키워드 추출 (알레르겐, 오염물질 등)
    keyword_patterns = ["알레르겐", "allergen", "세균", "bacterial", "오염", "contamination"]
    for pattern in keyword_patterns:
        if pattern in query_lower and not auto_filters.get("keyword"):
            auto_filters["keyword"] = pattern
            break
    
    print(f"🧠 스마트 순위 분석: '{query}' → field='{detected_field}', limit={limit}, filters={auto_filters}")
    
    return rank_by_field(field=detected_field, limit=limit, **auto_filters)

def calculate_filter_statistics(cursor, include_terms: Optional[List[str]], exclude_terms: List[str]) -> Dict[str, int]:
    """필터링 통계 계산"""
    
    try:
        # 전체 데이터 수
        cursor.execute("SELECT COUNT(*) as total FROM recalls")
        total_count = cursor.fetchone()["total"]
        
        # 포함 조건 매칭 수 (include_terms가 있는 경우)
        include_count = total_count
        if include_terms:
            include_sql = "SELECT COUNT(*) as count FROM recalls WHERE 1=1"
            include_params = []
            
            include_conditions = []
            for term in include_terms:
                english_term = translate_to_english(term)
                search_terms = [term, english_term] if english_term != term else [term]
                
                for search_term in search_terms:
                    include_conditions.append("""(
                        LOWER(company_name) LIKE LOWER(?) OR
                        LOWER(brand_name) LIKE LOWER(?) OR
                        LOWER(product_type) LIKE LOWER(?) OR
                        LOWER(recall_reason) LIKE LOWER(?) OR
                        LOWER(recall_reason_detail) LIKE LOWER(?) OR
                        LOWER(content) LIKE LOWER(?)
                    )""")
                    include_params.extend([f"%{search_term}%"] * 6)
            
            include_sql += f" AND ({' OR '.join(include_conditions)})"
            cursor.execute(include_sql, include_params)
            include_count = cursor.fetchone()["count"]
        
        # 제외 조건 매칭 수
        exclude_count = 0
        if exclude_terms:
            exclude_sql = "SELECT COUNT(*) as count FROM recalls WHERE 1=1"
            exclude_params = []
            
            exclude_conditions = []
            for term in exclude_terms:
                english_term = translate_to_english(term)
                search_terms = [term, english_term] if english_term != term else [term]
                
                term_conditions = []
                for search_term in search_terms:
                    term_conditions.append("""(
                        LOWER(company_name) LIKE LOWER(?) OR
                        LOWER(brand_name) LIKE LOWER(?) OR
                        LOWER(product_type) LIKE LOWER(?) OR
                        LOWER(recall_reason) LIKE LOWER(?) OR
                        LOWER(recall_reason_detail) LIKE LOWER(?) OR
                        LOWER(content) LIKE LOWER(?)
                    )""")
                    exclude_params.extend([f"%{search_term}%"] * 6)
                
                exclude_conditions.append(f"({' OR '.join(term_conditions)})")
            
            exclude_sql += f" AND ({' OR '.join(exclude_conditions)})"
            cursor.execute(exclude_sql, exclude_params)
            exclude_count = cursor.fetchone()["count"]
        
        # 최종 필터링 결과 수 계산
        final_count = include_count - exclude_count if exclude_count > 0 else include_count
        
        return {
            "total_records": total_count,
            "include_matches": include_count,
            "exclude_matches": exclude_count, 
            "final_filtered": max(0, final_count),
            "exclusion_rate": round((exclude_count / include_count * 100), 1) if include_count > 0 else 0
        }
        
    except Exception as e:
        return {
            "total_records": 0,
            "include_matches": 0,
            "exclude_matches": 0,
            "final_filtered": 0,
            "error": str(e)
        }

# ======================
# Function Calling 도구들
# ======================

REASON_CATEGORIES = {
    "allergens", "illness", "labeling", "contaminants",
    "microbiological", "foreign material", "quality",
    "packaging", "undetermined", "other"
}

@tool
def count_recalls(
    company: Optional[str] = None,
    product_type: Optional[str] = None,
    brand: Optional[str] = None,
    recall_reason: Optional[str] = None,            # 리콜 대분류(카테고리)
    recall_reason_detail: Optional[str] = None,     # 리콜 세부 원인(살모넬라 등)
    year: Optional[str] = None,
    keyword: Optional[str] = None) -> Dict[str, Any]:
    """리콜 건수를 세는 함수 (SQLite 기반) - 현재 JSON 구조 맞춤"""

    sqlite_conn, _, _ = _get_system_components()
    if not sqlite_conn:
        return {"error": "SQLite 데이터베이스 연결 실패"}

    try:
        # LLM이 실수로 recall_reason="Salmonella"처럼 넘겨도 자동 보정
        if recall_reason and not recall_reason_detail and _looks_like_detail(recall_reason):
            recall_reason_detail = recall_reason
            recall_reason = None

        sql = "SELECT COUNT(*) as count FROM recalls WHERE 1=1"
        params = []

        # 통합 키워드 검색 (모든 주요 필드에서 검색)
        if keyword:
            english_keyword = translate_to_english(keyword)
            search_terms = [keyword, english_keyword] if english_keyword != keyword else [keyword]
            
            print(f"🔍 통합 검색어: {search_terms}")
            
            search_conditions = []
            for term in search_terms:
                search_conditions.append("""(
                    LOWER(company_name) LIKE LOWER(?) OR 
                    LOWER(brand_name) LIKE LOWER(?) OR
                    LOWER(product_type) LIKE LOWER(?) OR
                    LOWER(recall_reason) LIKE LOWER(?) OR
                    LOWER(recall_reason_detail) LIKE LOWER(?) OR
                    LOWER(content) LIKE LOWER(?)
                )""")
                params.extend([f"%{term}%"] * 6)
            sql += f" AND ({' OR '.join(search_conditions)})"
				
				# 개별 필터들 (현재 JSON 구조 맞춤)
        if company:
            english_company = translate_to_english(company)
            company_terms = [company, english_company] if english_company != company else [company]
            company_conditions = []
            for term in company_terms:
                company_conditions.append("LOWER(company_name) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(company_conditions)})"

        if brand:
            english_brand = translate_to_english(brand)
            brand_terms = [brand, english_brand] if english_brand != brand else [brand]
            brand_conditions = []
            for term in brand_terms:
                brand_conditions.append("LOWER(brand_name) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(brand_conditions)})"

        if product_type:
            english_product_type = translate_to_english(product_type)
            product_type_terms = [product_type, english_product_type] if english_product_type != product_type else [product_type]
            product_type_conditions = []
            for term in product_type_terms:
                product_type_conditions.append("LOWER(product_type) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(product_type_conditions)})"

        # 대분류
        if recall_reason:
            english_recall_reason = translate_to_english(recall_reason)
            recall_reason_terms = [recall_reason, english_recall_reason] if english_recall_reason != recall_reason else [recall_reason]
            recall_reason_conditions = []
            for term in recall_reason_terms:
                recall_reason_conditions.append("LOWER(recall_reason) = LOWER(?)")
                params.append(term)
            sql += f" AND ({' OR '.join(recall_reason_conditions)})"

        # 상세 리콜 사유 검색 (살모넬라, 리스테리아 등 구체적 오염물질)
        if recall_reason_detail:
            english_recall_detail = translate_to_english(recall_reason_detail)
            recall_detail_terms = [recall_reason_detail, english_recall_detail] if english_recall_detail != recall_reason_detail else [recall_reason_detail]
            recall_detail_conditions = []
            for term in recall_detail_terms:
                recall_detail_conditions.append("LOWER(recall_reason_detail) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(recall_detail_conditions)})"

				# 날짜 필터 (fda_publish_date 사용)
        if year:
            if len(year) == 4: # 연도만
                sql += " AND strftime('%Y', fda_publish_date) = ?"
                params.append(year)
            elif len(year) == 7: # YYYY-MM 형태
                sql += " AND strftime('%Y-%m', fda_publish_date) = ?"
                params.append(year)

        print(f"🔧 SQL 쿼리: {sql}")
        print(f"🔧 파라미터: {params}")

        cursor = sqlite_conn.cursor()
        cursor.execute(sql, params)
        result = cursor.fetchone()

        return {
            "count": result["count"],
            "filters": {
                "company": company,
                "brand": brand,
                "product_type": product_type,
                "recall_reason": recall_reason,
                "recall_reason_detail": recall_reason_detail,
                "year": year,
                "keyword": keyword
            },
            "search_fields": "multiple" if keyword else "specific",
            "query_type": "unified_count",
            "database_fields_used": [
                "company_name", "brand_name", "product_type",
                "recall_reason", "recall_reason_detail", "fda_publish_date", "content"
            ]
        }

    except Exception as e:
        return {"error": f"SQL 카운팅 오류: {e}"}

@tool
def rank_by_field(field: str, limit: int = 10, 
                 company: Optional[str] = None,
                 product_type: Optional[str] = None,  # food_type → product_type
                 brand: Optional[str] = None,         # 브랜드 필터
                 year: Optional[str] = None,
                 keyword: Optional[str] = None) -> Dict[str, Any]:  # 키워드 필터
    """필드별 순위 분석 (현재 JSON 구조 맞춤 + 스마트 매핑)"""
    
    sqlite_conn, _, _ = _get_system_components()
    
    if not sqlite_conn:
        return {"error": "SQLite 데이터베이스 연결 실패"}
    
    try:
        cursor = sqlite_conn.cursor()
        
        # 현재 JSON 구조에 맞는 필드 매핑
        field_mapping = {
            "company": "company_name",
            "brand": "brand_name", 
            "product_type": "product_type",
            "product": "product_type",  # 별칭
            "recall_reason": "recall_reason",
            "reason": "recall_reason",  # 별칭
            "recall_detail": "recall_reason_detail",
            "detail": "recall_reason_detail",  # 별칭
            "contaminant": "recall_reason_detail",  # 오염물질은 상세사유에서
            "allergen": "recall_reason_detail"  # 알레르겐도 상세사유에서
        }
        
        # 필드 정규화
        normalized_field = field.lower().replace("_", "").replace(" ", "")
        db_field = None
        
        # 정확한 매칭 우선
        if field.lower() in field_mapping:
            db_field = field_mapping[field.lower()]
        # 부분 매칭
        else:
            for key, value in field_mapping.items():
                if key in normalized_field or normalized_field in key:
                    db_field = value
                    break
        
        # 기본값
        if not db_field:
            db_field = "recall_reason"  # 기본적으로 리콜 사유
        
        print(f"🔧 필드 매핑: '{field}' → '{db_field}'")
        
        # SQL 쿼리 구성
        sql = f"""
            SELECT {db_field} as name, COUNT(*) as count 
            FROM recalls 
            WHERE {db_field} IS NOT NULL 
            AND {db_field} != '' 
            AND {db_field} != 'N/A'
        """
        params = []
        
        # 키워드 필터 (여러 필드에서 통합 검색)
        if keyword:
            english_keyword = translate_to_english(keyword)
            search_terms = [keyword, english_keyword] if english_keyword != keyword else [keyword]
            
            keyword_conditions = []
            for term in search_terms:
                keyword_conditions.append("""(
                    LOWER(company_name) LIKE LOWER(?) OR 
                    LOWER(brand_name) LIKE LOWER(?) OR
                    LOWER(product_type) LIKE LOWER(?) OR
                    LOWER(recall_reason) LIKE LOWER(?) OR
                    LOWER(recall_reason_detail) LIKE LOWER(?)
                )""")
                params.extend([f"%{term}%"] * 5)
            
            sql += f" AND ({' OR '.join(keyword_conditions)})"
        
        # 개별 필터들
        if company and db_field != "company_name":
            english_company = translate_to_english(company)
            company_terms = [company, english_company] if english_company != company else [company]
            company_conditions = []
            for term in company_terms:
                company_conditions.append("LOWER(company_name) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(company_conditions)})"
            
        if brand and db_field != "brand_name":
            english_brand = translate_to_english(brand)
            brand_terms = [brand, english_brand] if english_brand != brand else [brand]
            brand_conditions = []
            for term in brand_terms:
                brand_conditions.append("LOWER(brand_name) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(brand_conditions)})"
            
        if product_type and db_field != "product_type":
            english_product_type = translate_to_english(product_type)
            product_type_terms = [product_type, english_product_type] if english_product_type != product_type else [product_type]
            product_type_conditions = []
            for term in product_type_terms:
                product_type_conditions.append("LOWER(product_type) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(product_type_conditions)})"
            
        if year:
            if len(year) == 4:  # 연도만
                sql += " AND strftime('%Y', fda_publish_date) = ?"
                params.append(year)
            elif len(year) == 7:  # YYYY-MM 형태
                sql += " AND strftime('%Y-%m', fda_publish_date) = ?"
                params.append(year)

        sql += f" GROUP BY {db_field} ORDER BY count DESC LIMIT ?"
        params.append(limit)
        
        print(f"🔧 순위 분석 SQL: {sql}")
        print(f"🔧 파라미터: {params}")
        
        cursor.execute(sql, params)
        results = [{"name": row["name"], "count": row["count"]} for row in cursor.fetchall()]
        
        return {
            "ranking": results,
            "field": field,
            "db_field": db_field,  # 실제 사용된 DB 필드
            "total_items": len(results),
            "filters": {
                "company": company,
                "brand": brand,
                "product_type": product_type, 
                "year": year,
                "keyword": keyword
            },
            "query_type": "field_ranking"
        }
        
    except Exception as e:
        return {"error": f"순위 분석 오류: {e}"}
    
    
@tool 
def get_monthly_trend(months: int = 12,
                     product_type: Optional[str] = None,  # food_type → product_type
                     company: Optional[str] = None,
                     brand: Optional[str] = None,         # 브랜드 필터 추가
                     recall_reason: Optional[str] = None, # 리콜 사유 필터 추가
                     keyword: Optional[str] = None,       # 키워드 필터 추가
                     date_field: str = "fda") -> Dict[str, Any]:  # 날짜 필드 선택
    """월별 리콜 트렌드 분석 (현재 JSON 구조 맞춤 + 다양한 필터 지원)"""
    
    sqlite_conn, _, _ = _get_system_components()
    
    if not sqlite_conn:
        return {"error": "SQLite 데이터베이스 연결 실패"}
    
    try:
        # 날짜 필드 선택 (FDA 발표일 vs 회사 발표일)
        if date_field.lower() in ["fda", "fda_publish"]:
            date_column = "fda_publish_date"
        elif date_field.lower() in ["company", "company_announcement"]:
            date_column = "company_announcement_date"
        else:
            date_column = "fda_publish_date"  # 기본값
        
        sql = f"""
            SELECT strftime('%Y-%m', {date_column}) as month, COUNT(*) as count
            FROM recalls 
            WHERE {date_column} IS NOT NULL
        """
        params = []
        
        # 키워드 통합 검색
        if keyword:
            english_keyword = translate_to_english(keyword)
            search_terms = [keyword, english_keyword] if english_keyword != keyword else [keyword]
            
            keyword_conditions = []
            for term in search_terms:
                keyword_conditions.append("""(
                    LOWER(company_name) LIKE LOWER(?) OR 
                    LOWER(brand_name) LIKE LOWER(?) OR
                    LOWER(product_type) LIKE LOWER(?) OR
                    LOWER(recall_reason) LIKE LOWER(?) OR
                    LOWER(recall_reason_detail) LIKE LOWER(?)
                )""")
                params.extend([f"%{term}%"] * 5)
            
            sql += f" AND ({' OR '.join(keyword_conditions)})"
        
        # 개별 필터들 (현재 JSON 구조)
        if product_type:
            english_product_type = translate_to_english(product_type)
            product_type_terms = [product_type, english_product_type] if english_product_type != product_type else [product_type]
            product_type_conditions = []
            for term in product_type_terms:
                product_type_conditions.append("LOWER(product_type) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(product_type_conditions)})"
            
        if company:
            english_company = translate_to_english(company)
            company_terms = [company, english_company] if english_company != company else [company]
            company_conditions = []
            for term in company_terms:
                company_conditions.append("LOWER(company_name) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(company_conditions)})"
            
        if brand:
            english_brand = translate_to_english(brand)
            brand_terms = [brand, english_brand] if english_brand != brand else [brand]
            brand_conditions = []
            for term in brand_terms:
                brand_conditions.append("LOWER(brand_name) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(brand_conditions)})"
            
        if recall_reason:
            english_recall_reason = translate_to_english(recall_reason)
            recall_reason_terms = [recall_reason, english_recall_reason] if english_recall_reason != recall_reason else [recall_reason]
            recall_reason_conditions = []
            for term in recall_reason_terms:
                recall_reason_conditions.append("LOWER(recall_reason) LIKE LOWER(?)")
                params.append(f"%{term}%")
            sql += f" AND ({' OR '.join(recall_reason_conditions)})"
        
        sql += " GROUP BY month ORDER BY month DESC LIMIT ?"
        params.append(months)
        
        print(f"🔧 트렌드 분석 SQL: {sql}")
        print(f"🔧 파라미터: {params}")
        
        cursor = sqlite_conn.cursor()
        cursor.execute(sql, params)
        results = [{"month": row["month"], "count": row["count"]} for row in cursor.fetchall()]
        
        return {
            "trend": results,
            "months": months,
            "date_field": date_column,
            "filters": {
                "product_type": product_type,
                "company": company,
                "brand": brand,
                "recall_reason": recall_reason,
                "keyword": keyword
            },
            "query_type": "monthly_trend"
        }
        
    except Exception as e:
        return {"error": f"트렌드 조회 오류: {e}"}
    

@tool
def compare_periods(period1: str, period2: str, 
                   metric: str = "count",
                   include_reasons: bool = False,       # 사유별 분석 포함
                   product_type: Optional[str] = None,  # 제품 유형 필터
                   company: Optional[str] = None,       # 회사 필터
                   brand: Optional[str] = None,         # 브랜드 필터
                   keyword: Optional[str] = None,       # 키워드 필터
                   date_field: str = "fda") -> Dict[str, Any]:  # 날짜 필드 선택
    """기간별 비교 분석 함수 (현재 JSON 구조 맞춤 + 다양한 필터 지원)"""
    
    sqlite_conn, _, _ = _get_system_components()
    
    if not sqlite_conn:
        return {"error": "SQLite 데이터베이스 연결 실패"}
    
    try:
        # 상대적 날짜 표현을 절대 연도로 변환
        actual_period1 = parse_relative_dates(period1)
        actual_period2 = parse_relative_dates(period2)
        
        print(f"🔧 날짜 변환: '{period1}' → {actual_period1}, '{period2}' → {actual_period2}")
        
        # 날짜 필드 선택
        if date_field.lower() in ["fda", "fda_publish"]:
            date_column = "fda_publish_date"
        elif date_field.lower() in ["company", "company_announcement"]:
            date_column = "company_announcement_date"
        else:
            date_column = "fda_publish_date"  # 기본값
        
        cursor = sqlite_conn.cursor()
        
        def get_period_data(period: str):
            """특정 기간의 데이터 조회 (현재 JSON 구조 맞춤)"""
            
            # 날짜 필터 설정
            if len(period) == 4:  # 연도 (YYYY)
                date_filter = f"strftime('%Y', {date_column}) = ?"
            elif len(period) == 7:  # 연월 (YYYY-MM)
                date_filter = f"strftime('%Y-%m', {date_column}) = ?"
            else:
                return None
            
            result_data = {}
            
            # 기본 WHERE 절 구성
            base_where = f"WHERE {date_filter}"
            base_params = [period]
            
            # 추가 필터들 적용
            additional_conditions = []
            additional_params = []
            
            # 키워드 통합 검색
            if keyword:
                english_keyword = translate_to_english(keyword)
                search_terms = [keyword, english_keyword] if english_keyword != keyword else [keyword]
                
                keyword_conditions = []
                for term in search_terms:
                    keyword_conditions.append("""(
                        LOWER(company_name) LIKE LOWER(?) OR 
                        LOWER(brand_name) LIKE LOWER(?) OR
                        LOWER(product_type) LIKE LOWER(?) OR
                        LOWER(recall_reason) LIKE LOWER(?) OR
                        LOWER(recall_reason_detail) LIKE LOWER(?)
                    )""")
                    additional_params.extend([f"%{term}%"] * 5)
                
                additional_conditions.append(f"({' OR '.join(keyword_conditions)})")
            
            # 개별 필터들
            if product_type:
                english_product_type = translate_to_english(product_type)
                product_type_terms = [product_type, english_product_type] if english_product_type != product_type else [product_type]
                product_type_conditions = []
                for term in product_type_terms:
                    product_type_conditions.append("LOWER(product_type) LIKE LOWER(?)")
                    additional_params.append(f"%{term}%")
                additional_conditions.append(f"({' OR '.join(product_type_conditions)})")
                
            if company:
                english_company = translate_to_english(company)
                company_terms = [company, english_company] if english_company != company else [company]
                company_conditions = []
                for term in company_terms:
                    company_conditions.append("LOWER(company_name) LIKE LOWER(?)")
                    additional_params.append(f"%{term}%")
                additional_conditions.append(f"({' OR '.join(company_conditions)})")
                
            if brand:
                english_brand = translate_to_english(brand)
                brand_terms = [brand, english_brand] if english_brand != brand else [brand]
                brand_conditions = []
                for term in brand_terms:
                    brand_conditions.append("LOWER(brand_name) LIKE LOWER(?)")
                    additional_params.append(f"%{term}%")
                additional_conditions.append(f"({' OR '.join(brand_conditions)})")
            
            # 최종 WHERE 절
            final_where = base_where
            final_params = base_params.copy()
            
            if additional_conditions:
                final_where += " AND " + " AND ".join(additional_conditions)
                final_params.extend(additional_params)
            
            # 메트릭별 쿼리 실행
            if metric == "count":
                sql = f"SELECT COUNT(*) as value FROM recalls {final_where}"
            elif metric == "companies":
                sql = f"SELECT COUNT(DISTINCT company_name) as value FROM recalls {final_where} AND company_name IS NOT NULL AND company_name != ''"
            elif metric == "brands":  # 브랜드 수 메트릭
                sql = f"SELECT COUNT(DISTINCT brand_name) as value FROM recalls {final_where} AND brand_name IS NOT NULL AND brand_name != ''"
            elif metric == "product_types":  # 제품 유형 수 메트릭  
                sql = f"SELECT COUNT(DISTINCT product_type) as value FROM recalls {final_where} AND product_type IS NOT NULL AND product_type != ''"
            else:
                sql = f"SELECT COUNT(*) as value FROM recalls {final_where}"
            
            cursor.execute(sql, final_params)
            result = cursor.fetchone()
            result_data["total"] = result["value"] if result else 0
            
            # 리콜 사유별 분석 (현재 JSON 구조)
            if include_reasons or "원인" in str(period) or "사유" in str(period):
                reason_sql = f"""
                    SELECT recall_reason, COUNT(*) as count 
                    FROM recalls 
                    {final_where} AND recall_reason IS NOT NULL AND recall_reason != ''
                    GROUP BY recall_reason 
                    ORDER BY count DESC 
                    LIMIT 5
                """
                cursor.execute(reason_sql, final_params)
                reasons = [{"reason": row["recall_reason"], "count": row["count"]} for row in cursor.fetchall()]
                result_data["top_reasons"] = reasons
            
            # 상세 사유별 분석 (오염물질, 알레르겐 등)
            if include_reasons:
                detail_sql = f"""
                    SELECT recall_reason_detail, COUNT(*) as count 
                    FROM recalls 
                    {final_where} AND recall_reason_detail IS NOT NULL AND recall_reason_detail != ''
                    GROUP BY recall_reason_detail 
                    ORDER BY count DESC 
                    LIMIT 5
                """
                cursor.execute(detail_sql, final_params)
                details = [{"detail": row["recall_reason_detail"], "count": row["count"]} for row in cursor.fetchall()]
                result_data["top_details"] = details
            
            return result_data
        
        # 각 기간 데이터 조회
        data1 = get_period_data(actual_period1)
        data2 = get_period_data(actual_period2)
        
        if data1 is None or data2 is None:
            return {"error": "잘못된 기간 형식입니다. YYYY 또는 YYYY-MM 형식을 사용하세요."}
        
        # 변화율 계산 및 분석
        value1 = data1.get("total", 0)  # 2024년: 240
        value2 = data2.get("total", 0)  # 2025년: 125
        change = value2 - value1        # 125 - 240 = -115
        change_percent = (change / value1 * 100) if value1 > 0 else 0

        if change_percent > 10:
            trend = "significant_increase"
            trend_description = "크게 증가"
        elif change_percent > 3:
            trend = "moderate_increase"  
            trend_description = "약간 증가"
        elif change_percent < -10:
            trend = "significant_decrease"
            trend_description = "크게 감소"
        elif change_percent < -3:
            trend = "moderate_decrease"
            trend_description = "약간 감소"
        else:
            trend = "stable"
            trend_description = "비슷한 수준"

        # 🔧 디버깅을 위해 print
        print(f"🔍 value1 (2024): {value1}")
        print(f"🔍 value2 (2025): {value2}")  
        print(f"🔍 change: {change}")
        print(f"🔍 change_percent: {change_percent}")

        return {
            "period1": {"period": f"{period1}({actual_period1})", "data": data1},
            "period2": {"period": f"{period2}({actual_period2})", "data": data2},
            "comparison": {
                "change": change,
                "change_percent": round(change_percent, 1),  # 🔧 소수점 반올림 확인
                "trend": trend,
                "trend_description": trend_description
            },
            "filters": {
                "metric": metric,
                "product_type": product_type,
                "company": company,
                "brand": brand,
                "keyword": keyword,
                "date_field": date_column
            },
            "query_type": "enhanced_period_comparison",
            "actual_periods": [actual_period1, actual_period2]
        }
        
    except Exception as e:
        return {"error": f"기간 비교 오류: {e}"}
    

@tool
def search_recall_cases(query: str, limit: int = 5) -> Dict[str, Any]:
    """ChromaDB 기반 의미적 검색 (현재 JSON 구조 맞춤 + 한영 번역 지원)"""
    
    _, vectorstore, _ = _get_system_components()
    
    if not vectorstore:
        return {"error": "ChromaDB 벡터스토어 연결 실패"}
    
    try:
        # 향상된 검색어 확장 전략
        search_queries = []
        search_queries.append(query)  # 원본 질문
        
        # 핵심 키워드 매핑 (현재 데이터에 맞춤)
        enhanced_translations = {
            # 오염물질/세균
            "살모넬라": ["Salmonella", "salmonella contamination"],
            "리스테리아": ["Listeria", "Listeria monocytogenes"],
            "대장균": ["E.coli", "E. coli", "Escherichia coli"],
            "클로스트리듐": ["Clostridium", "clostridium botulinum"],
            
            # 알레르겐
            "우유": ["milk", "dairy", "undeclared milk"],
            "계란": ["egg", "eggs", "undeclared egg"],
            "견과류": ["tree nuts", "nuts", "undeclared nuts"],
            "땅콩": ["peanut", "peanuts", "undeclared peanut"],
            "콩": ["soy", "soybean", "undeclared soy"],
            "밀": ["wheat", "gluten", "undeclared wheat"],
            
            # 제품 카테고리
            "복합 가공식품": ["processed foods", "processed products"],
            "소스 복합식품": ["sauce processed food", "sauce products"],
            "과자": ["snacks", "crackers", "cookies"],
            "유제품": ["dairy products", "milk products"],
            "해산물": ["seafood", "fish products"],
            "육류": ["meat products", "meat"],
            
            # 일반 용어
            "알레르겐": ["allergen", "undeclared allergen"],
            "오염": ["contamination", "contaminated"],
            "리콜": ["recall", "voluntary recall"],
            "사례": ["cases", "incidents"]
        }
        
        # 키워드별 번역 및 확장
        for ko_term, en_terms in enhanced_translations.items():
            if ko_term in query:
                search_queries.extend(en_terms)
        
        # 전체 쿼리 번역
        english_query = translate_to_english(query)
        if english_query != query and english_query not in search_queries:
            search_queries.append(english_query)
        
        # 중복 제거 및 빈 문자열 필터링
        search_queries = list(dict.fromkeys([q.strip() for q in search_queries if q.strip()]))
        
        print(f"🔍 확장된 검색어: {search_queries}")
        
        all_docs = []
        seen_urls = set()
        
        # 각 검색어로 검색 실행 (가중치 적용)
        for i, search_query in enumerate(search_queries):
            try:
                # 검색 결과 수를 검색어 우선순위에 따라 조정
                search_limit = limit * 3 if i == 0 else limit * 2  # 원본 쿼리에 더 높은 가중치
                
                docs = vectorstore.similarity_search(
                    search_query, 
                    k=search_limit,
                    filter={"document_type": "recall"}  # 리콜 문서만 검색
                )
                
                for doc in docs:
                    url = doc.metadata.get("url", "")
                    if url and url not in seen_urls:
                        all_docs.append(doc)
                        seen_urls.add(url)
                        
            except Exception as search_error:
                print(f"검색어 '{search_query}' 처리 중 오류: {search_error}")
                continue
        
        # 관련성 기반 정렬 (원본 쿼리와의 유사도 우선)
        if all_docs:
            # 상위 결과 선택
            selected_docs = all_docs[:limit]
        else:
            selected_docs = []
        
        # 결과 포맷팅 (현재 JSON 구조 맞춤)
        cases = []
        for doc in selected_docs:
            # 현재 ChromaDB 메타데이터 구조에 맞춤
            case_data = {
                "company": doc.metadata.get("company_name", "정보 없음"),
                "brand": doc.metadata.get("brand_name", "정보 없음"),
                "product_type": doc.metadata.get("product_type", "정보 없음"),
                "recall_reason": doc.metadata.get("recall_reason", "정보 없음"),
                "recall_detail": doc.metadata.get("recall_reason_detail", "정보 없음"),
                "fda_date": doc.metadata.get("fda_publish_date", "정보 없음"),
                "company_date": doc.metadata.get("company_announcement_date", "정보 없음"),
                "url": doc.metadata.get("url", ""),
                "content_preview": doc.page_content[:300] + "..." if len(doc.page_content) > 300 else doc.page_content,
                "chunk_info": {
                    "chunk_index": doc.metadata.get("chunk_index", 0),
                    "total_chunks": doc.metadata.get("total_chunks", 1),
                    "is_chunked": doc.metadata.get("is_chunked", False)
                }
            }
            
            cases.append(case_data)
        
        # 검색 품질 평가
        search_quality = evaluate_search_quality(query, cases)
        
        return {
            "cases": cases,
            "total_found": len(cases),
            "original_query": query,
            "search_queries": search_queries,
            "search_method": "enhanced_multilingual_search",
            "search_quality": search_quality,
            "data_structure": "current_json_format"
        }
        
    except Exception as e:
        return {"error": f"검색 오류: {e}"}

def evaluate_search_quality(query: str, cases: list) -> Dict[str, Any]:
    """검색 결과 품질 평가"""
    
    if not cases:
        return {"score": 0, "assessment": "no_results"}
    
    try:
        query_lower = query.lower()
        
        # 키워드 매칭 점수 계산
        keyword_matches = 0
        total_cases = len(cases)
        
        search_keywords = [
            "살모넬라", "salmonella", "리스테리아", "listeria", 
            "대장균", "e.coli", "알레르겐", "allergen",
            "우유", "milk", "계란", "egg", "견과류", "nuts"]
        
        for case in cases:
            case_text = " ".join([
                str(case.get("recall_reason", "")),
                str(case.get("recall_detail", "")),
                str(case.get("content_preview", ""))
            ]).lower()
            
            for keyword in search_keywords:
                if keyword in query_lower and keyword in case_text:
                    keyword_matches += 1
                    break
        
        # 품질 점수 계산 (0-100)
        match_ratio = keyword_matches / total_cases if total_cases > 0 else 0
        quality_score = min(100, int(match_ratio * 100 + (total_cases * 10)))
        
        # 평가 등급
        if quality_score >= 80:
            assessment = "excellent"
        elif quality_score >= 60:
            assessment = "good"
        elif quality_score >= 40:
            assessment = "fair"
        else:
            assessment = "poor"
        
        return {
            "score": quality_score,
            "assessment": assessment,
            "keyword_matches": keyword_matches,
            "total_results": total_cases,
            "match_ratio": round(match_ratio, 2)
        }
        
    except Exception as e:
        return {"score": 0, "assessment": "evaluation_error", "error": str(e)}
    


@tool
def filter_exclude_conditions(exclude_terms: List[str],
                             include_terms: Optional[List[str]] = None,
                             limit: int = 10) -> Dict[str, Any]:
    """특정 조건을 제외한 데이터 필터링 (SQLite 기반, 현재 JSON 구조 맞춤)
    
    Args:
        exclude_terms: 제외할 조건들 (예: ["살모넬라", "우유"])
        include_terms: 포함할 조건들 (선택사항, 예: ["알레르겐", "세균"])
        limit: 반환할 최대 결과 수
    """
    
    sqlite_conn, _, _ = _get_system_components()
    
    if not sqlite_conn:
        return {"error": "SQLite 데이터베이스 연결 실패"}
    
    try:
        cursor = sqlite_conn.cursor()
        
        # 기본 쿼리 구성 (모든 주요 필드 포함)
        sql = """
            SELECT company_name, brand_name, product_type, recall_reason, 
                   recall_reason_detail, fda_publish_date, url
            FROM recalls 
            WHERE 1=1
        """
        params = []
        
        # 포함 조건 처리 (OR 조건)
        if include_terms:
            include_conditions = []
            for term in include_terms:
                english_term = translate_to_english(term)
                search_terms = [term, english_term] if english_term != term else [term]
                
                for search_term in search_terms:
                    include_conditions.append("""(
                        LOWER(company_name) LIKE LOWER(?) OR
                        LOWER(brand_name) LIKE LOWER(?) OR
                        LOWER(product_type) LIKE LOWER(?) OR
                        LOWER(recall_reason) LIKE LOWER(?) OR
                        LOWER(recall_reason_detail) LIKE LOWER(?) OR
                        LOWER(content) LIKE LOWER(?)
                    )""")
                    params.extend([f"%{search_term}%"] * 6)
            
            sql += f" AND ({' OR '.join(include_conditions)})"
        
        # 제외 조건 처리 (AND NOT 조건)
        if exclude_terms:
            exclude_conditions = []
            for term in exclude_terms:
                english_term = translate_to_english(term)
                search_terms = [term, english_term] if english_term != term else [term]
                
                term_conditions = []
                for search_term in search_terms:
                    term_conditions.append("""(
                        LOWER(company_name) LIKE LOWER(?) OR
                        LOWER(brand_name) LIKE LOWER(?) OR
                        LOWER(product_type) LIKE LOWER(?) OR
                        LOWER(recall_reason) LIKE LOWER(?) OR
                        LOWER(recall_reason_detail) LIKE LOWER(?) OR
                        LOWER(content) LIKE LOWER(?)
                    )""")
                    params.extend([f"%{search_term}%"] * 6)
                
                exclude_conditions.append(f"({' OR '.join(term_conditions)})")
            
            sql += f" AND NOT ({' OR '.join(exclude_conditions)})"
        
        sql += " ORDER BY fda_publish_date DESC LIMIT ?"
        params.append(limit)
        
        print(f"🔧 제외 필터링 SQL: {sql}")
        print(f"🔧 파라미터 수: {len(params)}")
        
        # 메인 쿼리 실행
        cursor.execute(sql, params)
        filtered_results = cursor.fetchall()
        
        # 통계 계산을 위한 별도 쿼리들
        stats = calculate_filter_statistics(cursor, include_terms, exclude_terms)
        
        # 결과 포맷팅
        cases = []
        for row in filtered_results:
            cases.append({
                "company": row["company_name"] or "정보 없음",
                "brand": row["brand_name"] or "정보 없음", 
                "product_type": row["product_type"] or "정보 없음",
                "recall_reason": row["recall_reason"] or "정보 없음",
                "recall_detail": row["recall_reason_detail"] or "정보 없음",
                "fda_date": row["fda_publish_date"] or "정보 없음",
                "url": row["url"] or ""
            })
        
        return {
            "filtered_cases": cases,
            "total_found": len(cases),
            "statistics": stats,
            "filters": {
                "include_terms": include_terms or [],
                "exclude_terms": exclude_terms,
                "limit": limit
            },
            "query_type": "exclude_filter"
        }
        
    except Exception as e:
        return {"error": f"제외 필터링 오류: {e}"}
    
#-----------------------
# 메인 시스템 클래스
#-----------------------
    

class FunctionCallRecallSystem:
    """Function Calling 기반 하이브리드 리콜 시스템 - 전문 프롬프트 통합"""
    
    def __init__(self):
        self.tools = [
            count_recalls, rank_by_field, get_monthly_trend, 
            compare_periods, search_recall_cases, filter_exclude_conditions
        ]
        # OpenAI Function Calling 모델
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0
        ).bind_tools(self.tools)
        
        # 답변 생성용 LLM (프롬프트 템플릿 적용)
        self.answer_llm = ChatOpenAI(
            model="gpt-4o-mini", 
            temperature=0.3
        )
    
    def process_question(self, question: str, chat_history: List = None) -> Dict[str, Any]:
        """Function Calling으로 질문 처리 - 기존과 동일"""
        
        if chat_history is None:
            chat_history = []
        
        try:
            # 하이브리드 시스템 프롬프트
            system_prompt = """
                당신은 FDA 리콜 데이터 전문 분석가입니다. 
                사용자의 질문을 분석하여 적절한 함수들을 호출해서 정확한 답변을 제공하세요.

                🗓️ 현재 시점 정보:
                - 현재 연도: 2025년
                - 작년: 2024년  
                - 재작년: 2023년

                ⚠️ **날짜 함수 호출 시 절대 규칙**:
                - 사용자가 "작년"이라고 하면 → period1="작년" 또는 period2="작년"으로 그대로 전달
                - 사용자가 "올해"라고 하면 → period1="올해" 또는 period2="올해"로 그대로 전달  
                - 절대로 "작년"을 "2024"로 바꾸거나 "올해"를 "2025"로 바꾸지 말고 원본 그대로 전달

                🎯 함수 선택 가이드라인:

                1. **수치/통계 질문** (SQLite 기반):
                - "총 건수", "몇 건" → count_recalls()
                - "상위 N개", "순위", "가장 많은" → rank_by_field()  
                - "월별 트렌드", "증가/감소" → get_monthly_trend()
                - "작년과 올해", "2023년 vs 2024년" → compare_periods()

                2. **사례 검색** (ChromaDB 기반):
                - "사례 알려줘", "어떤 제품", "구체적인 내용" → search_recall_cases()

                3. **제외 조건 필터링**:
                - "A에서 B를 제외한", "A 빼고" → filter_exclude_conditions()

                🔧 핵심 매핑 규칙:

                **필드 매핑 (현재 JSON 구조)**:
                - 회사/기업 → company_name
                - 브랜드/상표 → brand_name  
                - 제품/식품 → product_type
                - 리콜사유/원인 → recall_reason
                - 상세사유 → recall_reason_detail

                **키워드 통합 검색**:
                - "계란", "우유", "견과류" 등 식품 카테고리 → keyword 파라미터 사용
                - keyword는 모든 필드(company_name, brand_name, product_type, recall_reason, recall_reason_detail)에서 자동 통합 검색

                **번역 매핑**:
                - "복합 가공식품" = "processed foods"
                - "살모넬라" = "Salmonella"
                - "리스테리아" = "Listeria"
                - "알레르겐" = "allergen"

                🎯 질문 유형별 함수 호출 예시:

                **건수 질문**:
                - "계란 관련 리콜 총 몇 건?" → count_recalls(keyword="계란")
                - "2024년 총 리콜 건수는?" → count_recalls(year="2024")
                - "몬델리즈 회사 리콜 몇 건?" → count_recalls(company="몬델리즈")

                **순위 질문**:
                - "주요 리콜 사유 5가지" → rank_by_field(field="recall_reason", limit=5)
                - "상위 회사 10곳" → rank_by_field(field="company", limit=10)
                - "과자류 브랜드 순위" → rank_by_field(field="brand", product_type="과자")

                **트렌드 질문**:
                - "최근 6개월 트렌드" → get_monthly_trend(months=6)
                - "알레르겐 월별 현황" → get_monthly_trend(keyword="알레르겐")
                - "몬델리즈 트렌드" → get_monthly_trend(company="몬델리즈")

                **비교 질문**:
                - "작년과 올해 리콜 비교" → compare_periods("작년", "올해")
                - "2023년 vs 2024년 알레르겐 비교" → compare_periods("2023", "2024", keyword="알레르겐", include_reasons=True)

                **사례 검색**:
                - "살모넬라 관련 사례 알려줘" → search_recall_cases("살모넬라")
                - "복합 가공식품 사례" → search_recall_cases("복합 가공식품")

                **제외 조건**:
                - "알레르겐 관련인데 우유는 제외" → filter_exclude_conditions(include_terms=["알레르겐"], exclude_terms=["우유"])
                - "세균 오염에서 살모넬라 빼고" → filter_exclude_conditions(include_terms=["세균"], exclude_terms=["살모넬라"])

                ⚡ 중요 참고사항:
                - 한국어 질문이지만 데이터는 영어로 저장되어 있음
                - 검색 시 한영 번역 자동 지원
                - 수치는 SQLite, 내용 검색은 ChromaDB 사용
                - 특정 식품 카테고리는 keyword 파라미터로 통합 검색
                - 날짜는 원본 표현 그대로 전달 ("작년" → "작년")
                """

            
            # 대화 메시지 구성
            messages = [
                {"role": "system", "content": system_prompt},
                *[{"role": msg.type, "content": msg.content} for msg in chat_history[-6:]],
                {"role": "user", "content": question}
            ]
            
            # Function Calling 실행
            response = self.llm.invoke(messages)
            
            # 함수 호출이 있는 경우 실행
            if hasattr(response, 'tool_calls') and response.tool_calls:
                print(f"🔧 Function Calls: {len(response.tool_calls)}개")
                
                tool_results = []
                for tool_call in response.tool_calls:
                    func_name = tool_call['name']
                    func_args = tool_call.get('args', {})
                    
                    print(f"  → {func_name}({func_args})")
                    
                    # 함수 실행
                    for tool in self.tools:
                        if tool.name == func_name:
                            result = tool.invoke(func_args)
                            tool_results.append({
                                "function": func_name,
                                "args": func_args,
                                "result": result
                            })
                            break
                
                # 전문 프롬프트 템플릿으로 최종 답변 생성
                final_answer = self._generate_final_answer(question, tool_results)
                
                return {
                    "answer": final_answer,
                    "function_calls": tool_results,
                    "processing_type": "function_calling"
                }
            else:
                # 일반 답변
                return {
                    "answer": response.content,
                    "function_calls": [],
                    "processing_type": "direct_answer"
                }
                
        except Exception as e:
            return {
                "answer": f"처리 중 오류가 발생했습니다: {e}",
                "function_calls": [],
                "processing_type": "error"
            }
    
    def _generate_final_answer(self, question: str, tool_results: List[Dict]) -> str:
        """전문 프롬프트 템플릿을 활용한 답변 생성"""
        
        if not tool_results:
            return "죄송합니다. 관련 정보를 찾을 수 없습니다."
        
        # 질문 유형별 프롬프트 선택 및 컨텍스트 구성
        answer_context = self._build_answer_context(tool_results)
        selected_prompt = self._select_prompt_template(question, tool_results)
        
        try:
            # 전문 프롬프트로 최종 답변 생성
            final_prompt = selected_prompt.format(
                question=question,
                **answer_context
            )
            
            response = self.answer_llm.invoke([
                {"role": "system", "content": "당신은 FDA 리콜 데이터 전문 분석가입니다."},
                {"role": "user", "content": final_prompt}
            ])
            
            return response.content
            
        except Exception as e:
            print(f"답변 생성 오류: {e}")
            # 폴백: 기존 방식 사용
            return self._generate_basic_answer(question, tool_results)
    
    def _select_prompt_template(self, question: str, tool_results: List[Dict]) -> str:
        """질문과 결과 유형에 따른 프롬프트 템플릿 선택"""
        
        # 사례 검색 결과가 있는 경우
        if any("search_recall_cases" == tr["function"] for tr in tool_results):
            return RecallPrompts.RECALL_ANSWER
        
        # 수치/통계 분석 결과
        elif any(tr["function"] in ["count_recalls", "rank_by_field", "get_monthly_trend"] 
                for tr in tool_results):
            return RecallPrompts.NUMERICAL_ANSWER
        
        # 논리 연산/비교 분석 결과  
        elif any(tr["function"] in ["compare_periods", "filter_exclude_conditions"]
                for tr in tool_results):
            return RecallPrompts.LOGICAL_ANSWER
        
        # 기본: 리콜 답변 템플릿
        else:
            return RecallPrompts.RECALL_ANSWER
    
    def _build_answer_context(self, tool_results: List[Dict]) -> Dict[str, str]:
        """프롬프트 템플릿용 컨텍스트 구성"""
        
        context = {}
        
        # 사례 검색 결과 처리
        search_results = [tr for tr in tool_results if tr["function"] == "search_recall_cases"]
        if search_results:
            cases = search_results[0]["result"].get("cases", [])
            context["recall_context"] = self._format_cases_for_prompt(cases)
        
        # 수치 분석 결과 처리
        numerical_results = [tr for tr in tool_results 
                           if tr["function"] in ["count_recalls", "rank_by_field", "get_monthly_trend"]]
        if numerical_results:
            context.update(self._format_numerical_for_prompt(numerical_results))
        
        # 논리 분석 결과 처리
        logical_results = [tr for tr in tool_results 
                 if tr["function"] in ["compare_periods", "filter_exclude_conditions"]]
        if logical_results:
            context.update(self._format_logical_for_prompt(logical_results))
        
        return context
    
    def _format_cases_for_prompt(self, cases: List[Dict]) -> str:
        """사례 데이터를 프롬프트용 텍스트로 변환"""
        
        if not cases:
            return "관련 사례를 찾을 수 없습니다."

        formatted_cases = []
        
        for i, case in enumerate(cases[:8], 1):  # 최대 8건
            case_text = f"""사례 {i}:
- 회사: {case.get('company', 'Unknown')}
- 브랜드: {case.get('brand', 'N/A')}
- 제품: {case.get('product_type', 'N/A')}
- 리콜 사유: {case.get('recall_reason', 'N/A')}
- 상세 사유: {case.get('recall_detail', 'N/A')}
- FDA 발표일: {case.get('fda_date', 'N/A')}
- 출처: {case.get('url', 'N/A')}"""
            formatted_cases.append(case_text.strip())
        
        return "\n\n".join(formatted_cases)
    
    def _format_numerical_for_prompt(self, numerical_results: List[Dict]) -> Dict[str, str]:
        """수치 분석 결과를 프롬프트용으로 변환"""
        
        context = {}
        
        for tool_result in numerical_results:
            func_name = tool_result["function"]
            result = tool_result["result"]
            
            if func_name == "count_recalls":
                context["analysis_type"] = "리콜 건수 통계"
                context["result"] = f"총 {result.get('count', 0):,}건"
                context["description"] = f"필터 조건: {result.get('filters', {})}"
                
            elif func_name == "rank_by_field":
                context["analysis_type"] = f"{result.get('field', '')}별 상위 순위"
                ranking = result.get("ranking", [])
                context["result"] = "\n".join([
                    f"{i+1}위: {item['name']} ({item['count']}건)" 
                    for i, item in enumerate(ranking[:10])
                ])
                context["description"] = f"총 {len(ranking)}개 항목 분석"
                
            elif func_name == "get_monthly_trend":
                context["analysis_type"] = "월별 리콜 트렌드"
                trend = result.get("trend", [])
                context["result"] = "\n".join([
                    f"{item['month']}: {item['count']}건" 
                    for item in trend[:12]
                ])
                context["description"] = f"최근 {len(trend)}개월 데이터"
        
        return context
    
    def _format_logical_for_prompt(self, logical_results: List[Dict]) -> Dict[str, str]:
        """논리 분석 결과를 프롬프트용으로 변환"""
        
        context = {}
        
        for tool_result in logical_results:
            func_name = tool_result["function"]
            result = tool_result["result"]
            
            if func_name == "compare_periods":
                context["operation"] = "기간별 비교 분석"
                
                # 🔧 상세한 정보 포함
                period1 = result.get("period1", {})
                period2 = result.get("period2", {})
                comparison = result.get("comparison", {})
                
                # 🔧 구체적인 데이터 구성
                detailed_result = f"""
    기간 1: {period1.get('period', '')} - {period1.get('data', {}).get('total', 0)}건
    기간 2: {period2.get('period', '')} - {period2.get('data', {}).get('total', 0)}건
    변화: {comparison.get('change', 0)}건
    변화율: {comparison.get('change_percent', 0)}%
    추세: {comparison.get('trend_description', '')}
                """.strip()
                
                context["result"] = detailed_result
                context["description"] = f"두 기간 간 리콜 건수 비교 분석 완료"
                
            elif func_name == "filter_exclude_conditions":
                # 기존 코드 유지
                context["operation"] = "제외 조건 필터링"
                cases = result.get("filtered_cases", [])
                stats = result.get("statistics", {})
                context["result"] = f"필터링된 사례 {len(cases)}건"
                context["description"] = f"포함: {result.get('filters', {}).get('include_terms', [])}, 제외: {result.get('filters', {}).get('exclude_terms', [])}"

        context["related_links"] = "FDA 공식 데이터베이스 기반 분석"
        
        return context
    
    def _generate_basic_answer(self, question: str, tool_results: List[Dict]) -> str:
        """폴백용 기본 답변 생성 (URL 링크 포함)"""
        
        if not tool_results:
            return "죄송합니다. 관련 정보를 찾을 수 없습니다."
        
        answer_parts = []
        answer_parts.append("## 📊 FDA 리콜 데이터 분석 결과\n")
        all_urls = []  # URL 수집용
        
        for tool_result in tool_results:
            func_name = tool_result["function"]
            result = tool_result["result"]
            
            if "error" in result:
                answer_parts.append(f"⚠️ {func_name} 오류: {result['error']}\n")
                continue
            
            # 함수별 결과 포맷팅
            if func_name == "count_recalls":
                count = result.get("count", 0)
                filters = result.get("filters", {})
                answer_parts.append(f"**총 리콜 건수**: {count:,}건")
                if any(filters.values()):
                    answer_parts.append(f"**필터 조건**: {filters}")
                    
            elif func_name == "rank_by_field":
                field = result.get("field", "")
                ranking = result.get("ranking", [])
                answer_parts.append(f"**{field} 순위**:")
                for i, item in enumerate(ranking[:10], 1):
                    answer_parts.append(f"{i}. {item['name']}: {item['count']}건")
                    
            elif func_name == "get_monthly_trend":
                trend = result.get("trend", [])
                answer_parts.append("**월별 트렌드**:")
                for item in trend[:6]:
                    answer_parts.append(f"- {item['month']}: {item['count']}건")
                    
            elif func_name == "compare_periods":
                period1 = result.get("period1", {})
                period2 = result.get("period2", {})
                
                # 🔧 여러 위치에서 change_percent 찾기
                change_percent = (
                    result.get("comparison", {}).get("change_percent", 0) or
                    result.get("change_percent", 0)
                )
                
                answer_parts.append("**기간별 비교**:")
                answer_parts.append(f"- {period1.get('period', '')}: {period1.get('data', {}).get('total', 0)}건")
                answer_parts.append(f"- {period2.get('period', '')}: {period2.get('data', {}).get('total', 0)}건")
                answer_parts.append(f"- 변화율: {change_percent:+.1f}%")
                
            elif func_name == "search_recall_cases":
                cases = result.get("cases", [])
                answer_parts.append(f"**관련 사례 {len(cases)}건**:")
                for i, case in enumerate(cases[:5], 1):
                    answer_parts.append(f"{i}. **{case.get('company', 'Unknown')}** - {case.get('product_type', 'N/A')}")
                    answer_parts.append(f"   사유: {case.get('recall_reason', 'N/A')}")
                    answer_parts.append(f"   상세: {case.get('recall_detail', 'N/A')}")
                    answer_parts.append(f"   발표일: {case.get('fda_date', 'N/A')}")
                    
                    # 개별 사례 URL 링크 추가
                    if case.get('url'):
                        answer_parts.append(f"   📋 [상세 정보 보기]({case.get('url')})")
                        all_urls.append(case.get('url'))
                    
            elif func_name == "filter_exclude_conditions":
                filtered_cases = result.get("filtered_cases", [])
                stats = result.get("statistics", {})
                answer_parts.append(f"**필터링 결과**: {len(filtered_cases)}건")
                answer_parts.append(f"**통계**: 전체 {stats.get('total_records', 0)}건 중 {stats.get('final_filtered', 0)}건 선별")
                
                # 필터링된 사례들에 URL 포함
                if filtered_cases:
                    answer_parts.append("**주요 사례**:")
                    for i, case in enumerate(filtered_cases[:3], 1):
                        answer_parts.append(f"{i}. **{case.get('company', 'Unknown')}** - {case.get('product_type', 'N/A')}")
                        answer_parts.append(f"   사유: {case.get('recall_reason', 'N/A')}")
                        
                        # URL 링크 추가
                        if case.get('url'):
                            answer_parts.append(f"   📋 [상세 정보 보기]({case.get('url')})")
                            all_urls.append(case.get('url'))
            
            answer_parts.append("")  # 섹션 구분
        
        # 출처 섹션 추가
        if all_urls:
            # 중복 제거
            unique_urls = list(dict.fromkeys(all_urls))
            answer_parts.append("---")
            answer_parts.append("**📚 관련 출처**:")
            for i, url in enumerate(unique_urls[:5], 1):  # 최대 5개
                answer_parts.append(f"{i}. [FDA 공식 리콜 공지 #{i}]({url})")
            answer_parts.append("")
        
        # 일반 FDA 출처 링크 항상 포함
        answer_parts.append("**🔗 추가 정보**:")
        answer_parts.append("- [FDA 리콜 및 안전 경고 전체 목록](https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/)")
        answer_parts.append("- [FDA 식품 안전 정보](https://www.fda.gov/food/food-safety-during-emergencies)")
        
        return "\n".join(answer_parts)
    
# ======================
# 외부 인터페이스 함수들
# ======================

def create_function_calling_system():
    """Function Calling 시스템 초기화"""
    try:
        return FunctionCallRecallSystem()
    except Exception as e:
        print(f"Function Calling 시스템 초기화 오류: {e}")
        return None

def ask_recall_question_fc(question: str, chat_history: List = None) -> Dict[str, Any]:
    """Function Calling 기반 질문 처리"""
    system = create_function_calling_system()
    if system:
        return system.process_question(question, chat_history)
    else:
        return {
            "answer": "시스템 초기화에 실패했습니다.",
            "function_calls": [],
            "processing_type": "error"
        }

def ask_recall_question(question: str, chat_history: List = None) -> Dict[str, Any]:
    """통합된 리콜 질문 처리 함수 (tab_recall.py 호환용)"""
    
    if chat_history is None:
        chat_history = []
    
    try:
        # Function Calling 시스템 사용
        result = ask_recall_question_fc(question, chat_history)
        
        # 기존 UI 호환 형식으로 반환
        return {
            "answer": result["answer"],
            "recall_documents": [],
            "chat_history": chat_history + [
                HumanMessage(content=question),
                AIMessage(content=result["answer"])
            ],
            "processing_type": result["processing_type"],
            "function_calls": result.get("function_calls", []),
            "has_realtime_data": True,
            "realtime_count": len(result.get("function_calls", []))
        }
        
    except Exception as e:
        return {
            "answer": f"처리 중 오류가 발생했습니다: {e}",
            "recall_documents": [],
            "chat_history": chat_history,
            "processing_type": "error",
            "function_calls": []
        }
    

# === Agent 연동용 툴/리소스 export ===

def export_recall_tools():
    """RecallAgent가 그대로 바인딩해서 쓸 수 있는 LangChain Tool 리스트 반환"""
    return [
        count_recalls,
        rank_by_field,
        get_monthly_trend,
        compare_periods,
        search_recall_cases,
        filter_exclude_conditions,
    ]

def get_sqlite_conn():
    """Agent 등 외부에서 동일 커넥션을 재사용할 수 있도록 반환"""
    conn, _, _ = _get_system_components()
    return conn

def tool_router(func_name: str, func_args: dict):
    """에이전트가 func_name으로 바로 호출할 수 있는 라우터 (선택)"""
    try:
        tools = {t.name: t for t in export_recall_tools()}
        if func_name in tools:
            return tools[func_name].invoke(func_args or {})
        return {"error": f"Unknown function: {func_name}"}
    except Exception as e:
        return {"error": f"tool_router error: {e}"}