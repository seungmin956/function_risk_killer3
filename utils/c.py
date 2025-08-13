## utils/c.py (v0)

import requests
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import re
from bs4 import BeautifulSoup
import openai
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

if not openai.api_key:
    raise ValueError("OPENAI_API_KEY가 .env 파일에 설정되어 있지 않습니다.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("risk_federal_changes.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# 현재 날짜와 시간을 파일명에 사용할 형식으로 가져오기
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"./risk_federal_changes_{current_datetime}.json"

# 처리할 Subchapter 목록 (A, B, L만 포함)
ALLOWED_SUBCHAPTERS = ['A', 'B', 'L']

# 번역 및 내용 길이 설정
TRANSLATE_CONTENT = True  # False로 설정하면 번역하지 않음
MAX_CONTENT_LENGTH = 10000  # 번역할 최대 내용 길이 (문자 수)
TRANSLATION_CHUNK_SIZE = 2500  # 번역 청크 크기

def translate_to_korean(text, max_retries=3):
    """OpenAI API를 사용하여 영어 텍스트를 한글로 번역"""
    if not text or len(text.strip()) == 0:
        return text
    
    # 텍스트가 너무 길면 청크로 나누어 번역
    max_chunk_size = TRANSLATION_CHUNK_SIZE  # 설정에서 가져옴
    
    if len(text) <= max_chunk_size:
        return _translate_chunk(text, max_retries)
    
    # 긴 텍스트를 청크로 나누어 번역
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + max_chunk_size
        
        # 문장 경계에서 자르기 위해 조정
        if end < len(text):
            # 마지막 마침표나 줄바꿈을 찾아서 자르기
            last_period = text.rfind('.', start, end)
            last_newline = text.rfind('\n', start, end)
            
            if last_period > start:
                end = last_period + 1
            elif last_newline > start:
                end = last_newline + 1
        
        chunk = text[start:end]
        chunks.append(chunk)
        start = end
    
    # 각 청크를 번역
    translated_chunks = []
    total_chunks = len(chunks)
    
    for i, chunk in enumerate(chunks):
        logger.info(f"번역 중... 청크 {i+1}/{total_chunks} (길이: {len(chunk)}자)")
        translated_chunk = _translate_chunk(chunk, max_retries)
        translated_chunks.append(translated_chunk)
        
        # API 호출 간 잠시 대기 (레이트 리밋 방지)
        if i < len(chunks) - 1:
            time.sleep(2)  # 2초 대기
    
    return ''.join(translated_chunks)

def _translate_chunk(text, max_retries=3):
    """단일 텍스트 청크를 번역"""
    for attempt in range(max_retries):
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "당신은 전문 번역가입니다. 미국 연방 규정(CFR)의 영어 텍스트를 정확하고 자연스러운 한국어로 번역해주세요. 법률 및 규제 용어는 정확하게 번역하되, 한국어로 읽기 쉽게 번역해주세요. 원본의 구조와 형식을 최대한 유지해주세요."
                    },
                    {
                        "role": "user",
                        "content": f"다음 텍스트를 한국어로 번역해주세요:\n\n{text}"
                    }
                ],
                max_tokens=4000,
                temperature=0.3
            )
            
            translated_text = response.choices[0].message.content.strip()
            return translated_text
            
        except Exception as e:
            logger.warning(f"번역 시도 {attempt + 1}/{max_retries} 실패: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))  # 점진적으로 대기 시간 증가
            else:
                logger.error(f"번역 실패, 원본 텍스트 반환: {str(e)}")
                return text  # 번역 실패 시 원본 반환

def get_date_one_month_ago():
    """정확히 한 달 전 날짜 반환"""
    today = datetime.now()
    one_month_ago = today - timedelta(days=90)  # 30일을 한 달로 간주
    return one_month_ago.strftime("%Y-%m-%d")

def is_valid_regulation_content(content):
    """추출된 내용이 실제 규정 내용인지 검증"""
    if not content or len(content) < 200:
        return False
    
    # 규정 내용을 나타내는 강한 지표들
    strong_indicators = [
        'part ', 'section ', 'subpart', '§', 'cfr',
        'shall', 'must', 'may not', 'prohibited',
        'required', 'defined as', 'means', 'includes'
    ]
    
    # 메타 정보를 나타내는 지표들 (이게 많으면 규정 내용이 아님)
    meta_indicators = [
        'ecfr에서 가져온', '권위 있는 정보', '비공식적',
        '최신 정보', '개정되었습니다', '역사적 버전',
        '작성 사이트', 'browser support', 'feedback'
    ]
    
    content_lower = content.lower()
    
    # 강한 지표 개수 세기
    strong_count = sum(1 for indicator in strong_indicators if indicator in content_lower)
    
    # 메타 지표 개수 세기
    meta_count = sum(1 for indicator in meta_indicators if indicator in content_lower)
    
    # 규정 내용으로 판단하는 조건
    return strong_count >= 3 and meta_count <= 2

def extract_regulation_from_full_page(soup, part_num):
    """전체 페이지에서 규정 내용만 추출하는 마지막 수단"""
    logger.info(f"Part {part_num}: 전체 페이지에서 규정 내용 추출 시도")
    
    # 모든 텍스트를 가져와서 규정 관련 섹션만 필터링
    all_text = soup.get_text(separator='\n', strip=True)
    lines = all_text.split('\n')
    
    regulation_lines = []
    in_regulation_section = False
    
    for line in lines:
        line = line.strip()
        if not line or len(line) < 10:
            continue
        
        line_lower = line.lower()
        
        # 규정 섹션 시작 감지
        if any(pattern in line_lower for pattern in [
            f'part {part_num}', 'section', 'subpart', '§'
        ]):
            in_regulation_section = True
        
        # 메타 정보 섹션 감지 (규정 섹션 종료)
        elif any(pattern in line_lower for pattern in [
            'ecfr에서 가져온', '권위 있는 정보', '최신 정보',
            'browser support', 'feedback', '작성 사이트'
        ]):
            in_regulation_section = False
        
        # 규정 섹션 내의 줄만 수집
        if in_regulation_section:
            # 여전히 메타 정보가 포함된 줄은 제외
            if not any(pattern in line_lower for pattern in [
                '개정되었습니다', '역사적 버전', '전환', 'switch',
                '2025년', 'historical', 'authoring'
            ]):
                regulation_lines.append(line)
    
    result = '\n'.join(regulation_lines)
    
    if len(result) > 200:
        logger.info(f"Part {part_num}: 전체 페이지에서 규정 내용 추출 성공 (길이: {len(result)})")
        return result
    else:
        logger.error(f"Part {part_num}: 전체 페이지에서도 유효한 규정 내용을 찾지 못함")
        return ""

def clean_text(text):
    """텍스트 정리: 불필요한 공백 제거 및 정리"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_recent_changes():
    """최근 변경된 Title 21 규정 목록 가져오기"""
    one_month_ago = get_date_one_month_ago()
    url = f'https://www.ecfr.gov/recent-changes?search%5Bhierarchy%5D%5Btitle%5D=21&search%5Blast_modified_after%5D={one_month_ago}'
    
    logger.info(f"최근 변경 목록 가져오는 중: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.ecfr.gov/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 디버깅을 위해 HTML 저장
        # with open('ecfr_debug.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        changes = []
        
        # 페이지에 있는 모든 날짜 헤더 찾기 (다양한 클래스나 형식을 처리)
        date_headers = soup.find_all(['h3', 'h4', 'div'], text=re.compile(r'\d+/\d+/\d{4}'))
        
        # 날짜 헤더가 없는 경우 다른 방식으로 시도
        if not date_headers:
            date_headers = soup.find_all(string=re.compile(r'\d+/\d+/\d{4}'))
        
        # 디버깅 정보
        if date_headers:
            logger.info(f"날짜 헤더 {len(date_headers)}개 발견")
            for i, header in enumerate(date_headers):
                logger.info(f"날짜 헤더 {i+1}: {header.get_text().strip() if hasattr(header, 'get_text') else header.strip()}")
        
        # 각 날짜 헤더에 대해 처리
        for date_header in date_headers:
            date_text = date_header.get_text().strip() if hasattr(date_header, 'get_text') else date_header.strip()
            
            # 날짜 헤더의 위치를 기준으로 코드 섹션 찾기
            # 날짜 헤더가 div 내부에 있는 경우
            if hasattr(date_header, 'parent'):
                parent_section = date_header.parent
                
                # 상위 요소로 올라가면서 Title 21 관련 섹션 찾기
                while parent_section and parent_section.name != 'body':
                    # Title 21 섹션 찾기
                    title21_elements = parent_section.find_all(string=lambda s: s and 'Title 21' in s)
                    
                    if title21_elements:
                        for title21_elem in title21_elements:
                            # Part 링크 찾기
                            title21_parent = title21_elem.parent
                            
                            # 먼저 하위 요소에서 링크 찾기
                            part_links = []
                            if hasattr(title21_parent, 'find_all'):
                                part_links = title21_parent.find_all('a', href=re.compile(r'/title-21/.*part-\d+'))
                            
                            # 링크가 없으면 상위 요소에서 찾기
                            if not part_links and hasattr(title21_parent, 'parent'):
                                parent_container = title21_parent.parent
                                if hasattr(parent_container, 'find_all'):
                                    part_links = parent_container.find_all('a', href=re.compile(r'/title-21/.*part-\d+'))
                            
                            # 발견된 링크 처리
                            for part_link in part_links:
                                href = part_link.get('href', '')
                                if href:
                                    part_url = f"https://www.ecfr.gov{href}" if href.startswith('/') else href
                                    
                                    # Part 번호 추출
                                    part_match = re.search(r'/part-(\d+)', part_url)
                                    if part_match:
                                        part_num = int(part_match.group(1))
                                        
                                        # Subchapter 추출
                                        subchapter = "Unknown"
                                        subchapter_match = re.search(r'/subchapter-([A-Z])', part_url)
                                        if subchapter_match:
                                            subchapter = subchapter_match.group(1)
                                        
                                        # 허용된 Subchapter만 처리 (A, B, L)
                                        if subchapter in ALLOWED_SUBCHAPTERS:
                                            # 변경 항목에 추가
                                            changes.append({
                                                "subchapter": subchapter,  # 처리 중에 필요하지만 최종 출력에서는 제외됨
                                                "part_number": part_num,   # 처리 중에 필요하지만 최종 출력에서는 제외됨
                                                "change_date": date_text,
                                                "url": part_url
                                            })
                                            logger.info(f"변경 항목 발견: Subchapter {subchapter}, Part {part_num}, 날짜: {date_text}")
                    
                    # 다음 상위 요소로 이동
                    parent_section = parent_section.parent
        
        # 아직도 변경 항목이 발견되지 않았다면 마지막 시도
        if not changes:
            logger.info("직접적인 Title 21 변경 항목을 찾지 못했습니다. 모든 링크 검색...")
            
            # 페이지 내 모든 Title 21 관련 링크 찾기
            all_part_links = soup.find_all('a', href=re.compile(r'/title-21/.*part-\d+'))
            
            if all_part_links:
                logger.info(f"총 {len(all_part_links)}개의 Title 21 링크 발견")
                
                # 날짜 정보를 가장 가까운 날짜 헤더에서 가져오기
                current_date = "Unknown"
                for part_link in all_part_links:
                    # 이 링크에 대한 날짜 찾기
                    date_elem = part_link.find_previous(string=re.compile(r'\d+/\d+/\d{4}'))
                    if date_elem:
                        current_date = date_elem.strip()
                    
                    href = part_link.get('href', '')
                    if href:
                        part_url = f"https://www.ecfr.gov{href}" if href.startswith('/') else href
                        
                        # Part 번호 추출
                        part_match = re.search(r'/part-(\d+)', part_url)
                        if part_match:
                            part_num = int(part_match.group(1))
                            
                            # Subchapter 추출
                            subchapter = "Unknown"
                            subchapter_match = re.search(r'/subchapter-([A-Z])', part_url)
                            if subchapter_match:
                                subchapter = subchapter_match.group(1)
                            
                            # 허용된 Subchapter만 처리 (A, B, L)
                            if subchapter in ALLOWED_SUBCHAPTERS:
                                # 변경 항목에 추가
                                changes.append({
                                    "subchapter": subchapter,  # 처리 중에 필요하지만 최종 출력에서는 제외됨
                                    "part_number": part_num,   # 처리 중에 필요하지만 최종 출력에서는 제외됨
                                    "change_date": current_date,
                                    "url": part_url
                                })
                                logger.info(f"변경 항목 발견: Subchapter {subchapter}, Part {part_num}, 날짜: {current_date}")
        
        # 중복 제거
        unique_changes = []
        seen_parts = set()
        for change in changes:
            key = (change["subchapter"], change["part_number"])
            if key not in seen_parts:
                seen_parts.add(key)
                unique_changes.append(change)
        
        logger.info(f"총 {len(unique_changes)}개의 최근 변경 항목 발견 (Subchapter A, B, L만 포함)")
        return unique_changes
    
    except Exception as e:
        logger.error(f"최근 변경 목록 가져오기 실패: {str(e)}")
        # 디버깅을 위해 예외 스택 트레이스 출력
        import traceback
        logger.error(traceback.format_exc())
        return []

def get_part_data(subchapter_letter, part_num, url=None):
    """eCFR 사이트에서 직접 데이터 가져오기 (웹 페이지에서 필요한 정보 추출)"""
    if not url:
        url = f"https://www.ecfr.gov/current/title-21/chapter-I/subchapter-{subchapter_letter}/part-{part_num}"
    
    logger.info(f"가져오는 중: {url}")
    
    # 비교 URL인 경우 현재 버전 URL로 변환
    if 'compare' in url:
        # 예: https://www.ecfr.gov/compare/2025-05-12/to/2025-05-11/title-21/chapter-I/subchapter-A/part-73
        # 변환: https://www.ecfr.gov/current/title-21/chapter-I/subchapter-A/part-73
        try:
            # 현재 URL에서 타이틀, 챕터, 서브챕터, 파트 정보 추출
            parts_match = re.search(r'/title-21/chapter-I/subchapter-([A-Z])/part-(\d+)', url)
            if parts_match:
                sub_chapter = parts_match.group(1)
                part_no = parts_match.group(2)
                # 현재 버전 URL 생성
                current_url = f"https://www.ecfr.gov/current/title-21/chapter-I/subchapter-{sub_chapter}/part-{part_no}"
                logger.info(f"비교 URL을 현재 버전 URL로 변환: {url} -> {current_url}")
                url = current_url
        except Exception as e:
            logger.warning(f"URL 변환 중 오류: {e}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8',
    }
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # with open(f'debug_page_{part_num}.html', 'w', encoding='utf-8') as f:
            #     f.write(response.text)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 제목 추출
            title = ""
            title_elem = soup.find('h1', class_='title')
            if title_elem:
                title = clean_text(title_elem.get_text())
            else:
                # 대체 방법으로 제목 찾기
                title_elems = soup.find_all(string=re.compile(r'PART\s+\d+'))
                for elem in title_elems:
                    if f"PART {part_num}" in elem:
                        title = clean_text(elem)
                        break
            
            if not title:
                title = f"PART {part_num}"
            
            # 내용 추출 - eCFR 사이트 구조에 맞춘 정밀 추출
            content = ""
            
            # eCFR 특화 컨텐츠 셀렉터들 (우선순위 순)
            ecfr_selectors = [
                'div.cfr-content div.section',           # 개별 섹션
                'div.cfr-content',                       # CFR 메인 컨텐츠
                'div[data-testid="cfr-content"]',        # 테스트 ID 기반
                'div.part-content',                      # Part 컨텐츠
                'div.document-body',                     # 문서 본문
                'div.regulation-text',                   # 규정 텍스트
                'section[role="main"] div.content',      # 메인 섹션 내 컨텐츠
                'article.regulation',                    # 규정 아티클
                'div#cfr-reference-content',             # CFR 참조 컨텐츠
                'div.legal-document'                     # 법적 문서
            ]
            
            # 제거할 불필요한 요소들의 CSS 셀렉터
            unwanted_selectors = [
                'nav', 'header', 'footer', 'script', 'style', 'aside',
                '.navigation', '.breadcrumb', '.sidebar', '.banner',
                '.feedback', '.alert', '.notice', '.warning', '.ad',
                '.site-feedback', '.content-feedback', '.website-feedback',
                '.browser-support', '.version-info', '.last-updated',
                '.historical-versions', '.authoring-site', '.switch-site',
                'div[class*="feedback"]', 'div[class*="browser"]',
                'div[class*="support"]', 'div[class*="version"]',
                'div[id*="feedback"]', 'div[id*="navigation"]'
            ]
            
            # 각 셀렉터로 컨텐츠 추출 시도
            for selector in ecfr_selectors:
                try:
                    content_elem = soup.select_one(selector)
                    if content_elem:
                        logger.info(f"컨텐츠 요소 발견: {selector}")
                        
                        # 불필요한 요소들 제거
                        for unwanted_selector in unwanted_selectors:
                            for unwanted in content_elem.select(unwanted_selector):
                                unwanted.decompose()
                        
                        # 특정 텍스트 패턴을 포함하는 요소 제거
                        unwanted_text_patterns = [
                            'eCFR에서 가져온', '권위 있는 정보', '비공식적입니다',
                            '최신 정보를 보여줍니다', '마지막으로', '개정되었습니다',
                            '역사적 버전', '작성 사이트', '수정 언어', '전환',
                            'browser support', 'feedback', 'switch to',
                            'historical version', 'authoring site'
                        ]
                        
                        for pattern in unwanted_text_patterns:
                            for elem in content_elem.find_all(string=lambda text: 
                                text and pattern.lower() in text.lower()):
                                if elem.parent:
                                    elem.parent.decompose()
                        
                        # 텍스트 추출
                        raw_content = content_elem.get_text(separator='\n', strip=True)
                        
                        # 줄 단위로 필터링
                        lines = raw_content.split('\n')
                        filtered_lines = []
                        
                        for line in lines:
                            line = line.strip()
                            # 불필요한 줄 제거
                            if line and len(line) > 10:  # 너무 짧은 줄 제외
                                # 메타 정보가 포함된 줄 제외
                                skip_line = False
                                for pattern in unwanted_text_patterns:
                                    if pattern.lower() in line.lower():
                                        skip_line = True
                                        break
                                
                                # 날짜 패턴이 있는 줄도 제외 (메타 정보일 가능성)
                                date_patterns = [r'\d{4}년 \d{1,2}월 \d{1,2}일', r'\d{1,2}/\d{1,2}/\d{4}', r'2025년']
                                for date_pattern in date_patterns:
                                    if re.search(date_pattern, line):
                                        skip_line = True
                                        break
                                
                                if not skip_line:
                                    filtered_lines.append(line)
                        
                        content = '\n'.join(filtered_lines)
                        
                        # 실제 규정 내용인지 검증
                        if is_valid_regulation_content(content):
                            logger.info(f"유효한 규정 내용 추출 성공: {selector} (길이: {len(content)})")
                            break
                        else:
                            logger.warning(f"추출된 내용이 규정 내용이 아님: {selector}")
                            content = ""  # 다음 셀렉터 시도
                            
                except Exception as e:
                    logger.warning(f"셀렉터 {selector} 처리 중 오류: {e}")
                    continue
            
            # 모든 셀렉터가 실패하면 마지막 수단으로 전체 페이지에서 규정 섹션 찾기
            if not content:
                logger.warning("모든 특정 셀렉터 실패. 전체 페이지에서 규정 내용 검색...")
                content = extract_regulation_from_full_page(soup, part_num)
            
            # 추출된 내용이 실제 규정 내용인지 검증
            if content:
                # 규정 내용을 나타내는 키워드들
                regulation_keywords = [
                    'part', 'section', 'subpart', 'shall', 'must', 'may not',
                    'regulation', 'cfr', 'code of federal regulations',
                    'defined', 'means', 'includes', 'requirements', 'standards',
                    'prohibited', 'permitted', 'approved', 'exempt'
                ]
                
                content_lower = content.lower()
                keyword_count = sum(1 for keyword in regulation_keywords if keyword in content_lower)
                
                # 규정 관련 키워드가 너무 적으면 경고
                if keyword_count < 3:
                    logger.warning(f"Part {part_num}: 추출된 내용에 규정 관련 키워드가 부족함 (키워드 수: {keyword_count})")
                    logger.warning(f"내용 미리보기: {content[:200]}...")
                
                # 불필요한 반복 텍스트 제거
                lines = content.split('\n')
                unique_lines = []
                seen_lines = set()
                
                for line in lines:
                    line = line.strip()
                    if line and line not in seen_lines and len(line) > 5:
                        seen_lines.add(line)
                        unique_lines.append(line)
                
                content = '\n'.join(unique_lines)
            
            # 내용이 여전히 비어있거나 너무 짧으면 오류 처리
            if not content or len(content) < 100:
                logger.error(f"Part {part_num}: 유효한 내용을 추출하지 못함")
                return None
            
            # 내용이 너무 길면 자르기 (번역 비용 및 시간 절약)
            max_content_length = MAX_CONTENT_LENGTH  # 설정에서 가져옴
            if len(content) > max_content_length:
                # 문장 경계에서 자르기
                truncated_content = content[:max_content_length]
                last_period = truncated_content.rfind('.')
                last_newline = truncated_content.rfind('\n')
                
                # 마지막 완전한 문장이나 단락에서 자르기
                if last_period > max_content_length * 0.8:  # 80% 지점 이후에 마침표가 있으면
                    content = content[:last_period + 1] + "\n\n...(내용이 너무 길어 일부 생략됨. 전체 내용은 원본 URL을 참조하세요)..."
                elif last_newline > max_content_length * 0.8:  # 80% 지점 이후에 줄바꿈이 있으면
                    content = content[:last_newline] + "\n\n...(내용이 너무 길어 일부 생략됨. 전체 내용은 원본 URL을 참조하세요)..."
                else:
                    content = content[:max_content_length] + "\n\n...(내용이 너무 길어 일부 생략됨. 전체 내용은 원본 URL을 참조하세요)..."
                
                logger.info(f"Part {part_num} 내용이 {max_content_length}자로 제한됨 (원본: {len(soup.find('body').get_text())}자)")
            
            return {
                "title": title,
                "subchapter": subchapter_letter,
                "part_number": part_num,
                "url": url,  # 원래 URL을 유지하여 기록 보존
                "content": content,
            }
            
        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"최대 재시도 횟수 초과: {url}, 오류: {str(e)}")
                return None
            
            wait_time = 5 * retry_count
            logger.warning(f"요청 오류, {wait_time}초 대기 후 재시도 ({retry_count}/{max_retries}): {str(e)}")
            time.sleep(wait_time)
        
        except Exception as e:
            logger.error(f"데이터 처리 중 오류 발생: {url}, 오류: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    return None

def process_part(part_info):
    """최근 변경된 Part 데이터 처리 및 번역"""
    subchapter = part_info["subchapter"]
    part_num = part_info["part_number"]
    url = part_info["url"]
    change_date = part_info["change_date"]
    
    logger.info(f"처리 중: Subchapter {subchapter}, Part {part_num}, 변경일: {change_date}")
    
    # 웹 페이지에서 데이터 가져오기
    data = get_part_data(subchapter, part_num, url)
    
    if data is not None:
        # 변경 날짜 정보 추가
        data["change_date"] = change_date
        
        # 번역 설정에 따라 번역 수행
        if TRANSLATE_CONTENT:
            # 제목과 내용을 한글로 번역
            logger.info(f"Part {part_num} 제목 번역 중...")
            data["title_korean"] = translate_to_korean(data["title"])
            
            logger.info(f"Part {part_num} 내용 번역 중... (길이: {len(data['content'])} 문자)")
            data["content_korean"] = translate_to_korean(data["content"])
            data["summary_korean"] = summarize_korean_text(data["content_korean"])
            
            logger.info(f"Subchapter {subchapter}, Part {part_num} 처리 및 번역 완료")
        else:
            logger.info(f"Subchapter {subchapter}, Part {part_num} 처리 완료 (번역 생략)")
            data["title_korean"] = ""  # 빈 값으로 설정
            data["content_korean"] = ""  # 빈 값으로 설정
    else:
        logger.warning(f"Subchapter {subchapter}, Part {part_num} 데이터를 가져올 수 없음")
    
    return data

def summarize_korean_text(text, max_retries=3):
    prompt = f"""
    다음은 미국 연방 규정 eCFR의 한글 번역 내용입니다. 
    이 텍스트를 바탕으로 가장 핵심적인 규제 내용을 **1,000자 정도로** 요약해주세요. 
    단, 마크다운 문법(**굵게**)은 절대 사용하지 마세요.
    주요 규제 항목, 대상, 조건 등을 한국어로 정리해 주세요. 식료품 규정과 상관없는 불필요한 배경 설명은 생략해주세요.

    규정 전문:
    {text}
    """
    for attempt in range(max_retries):
        try:
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "당신은 한국어 요약 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=700  # 1,000자 내외로 제한
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
            else:
                return "요약 실패"


def main():
    start_time = time.time()
    
    logger.info(f"처리 시작 - 출력 파일: {output_filename}")
    
    # 최근 변경 목록 가져오기
    recent_changes = get_recent_changes()
    
    if not recent_changes:
        logger.error("최근 변경 목록을 가져오지 못했습니다.")
        return
    
    # 총 태스크 수
    total_tasks = len(recent_changes)
    logger.info(f"총 처리 대상: {total_tasks}개 파트")
    
    # 진행 상황 추적 변수
    completed_tasks = 0
    all_data = []
    
    # 번역 작업은 시간이 오래 걸리므로 배치 크기를 줄임
    batch_size = 2  # 번역 때문에 더 작은 배치로 처리
    batches = [recent_changes[i:i + batch_size] for i in range(0, len(recent_changes), batch_size)]
    
    for batch_index, batch in enumerate(batches):
        logger.info(f"배치 {batch_index + 1}/{len(batches)} 처리 중...")
        
        # 번역 작업은 순차 처리 (API 레이트 리밋 고려)
        for part_info in batch:
            try:
                part_data = process_part(part_info)
                # 성공적으로 처리된 경우에만 결과에 추가
                if part_data is not None:
                    # 요청된 필드만 포함하여 최종 데이터 생성 (한글 번역 포함)
                    simplified_data = {
                        "title": part_data["title"],
                        "title_korean": part_data["title_korean"],
                        "change_date": part_data["change_date"],
                        "url": part_data["url"],
                        "content": part_data["content"],
                        "content_korean": part_data["content_korean"],
                        "summary_korean": part_data["summary_korean"]
                    }
                    all_data.append(simplified_data)
                
                completed_tasks += 1
                progress_percent = (completed_tasks / total_tasks) * 100
                elapsed_time = time.time() - start_time
                eta = (elapsed_time / completed_tasks) * (total_tasks - completed_tasks) if completed_tasks > 0 else 0
                logger.info(f"진행 상황: {completed_tasks}/{total_tasks} ({progress_percent:.1f}%) 완료, ETA: {eta:.1f}초")
                
            except Exception as e:
                logger.error(f"Subchapter {part_info['subchapter']}, Part {part_info['part_number']} 결과 처리 중 오류: {str(e)}")
        
        # 배치 간 대기 (API 제한 고려하여 더 긴 대기)
        if batch_index < len(batches) - 1:
            logger.info(f"다음 배치 전 30초 대기... (API 레이트 리밋 고려)")
            time.sleep(30)
    
    # 결과가 없으면 종료
    if not all_data:
        logger.error("데이터를 가져오지 못했습니다. 로그 파일을 확인하세요.")
        return
    
    # 날짜순으로 정렬
    all_data.sort(key=lambda x: x["change_date"], reverse=True)
    
    # 진행 상황 출력
    logger.info(f"총 {len(all_data)}/{total_tasks} 파트 데이터 추출 및 번역 성공")
    
    # JSON 파일로 저장
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    elapsed_time = time.time() - start_time
    logger.info(f"처리 완료! 총 {len(all_data)} 파트의 변경된 규정이 추출되고 번역되었습니다.")
    logger.info(f"파일 저장 완료: {output_filename}")
    logger.info(f"총 소요 시간: {elapsed_time:.2f}초")

if __name__ == "__main__":
    main()