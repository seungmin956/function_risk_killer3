# components/tab_news.py

import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import os
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()

def fetch_articles_with_keyword(keyword=None, max_pages=5, max_articles=3):
    """특정 키워드가 포함된 최신 기사들을 크롤링하여 제목, 요약, 날짜, 링크, 이미지 정보를 담아 리스트로 반환하는 함수"""
    base_url = "https://www.thinkfood.co.kr/news/articleList.html?sc_section_code=S1N2&view_type=sm"
    results = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}&page={page}"
        res = requests.get(url)
        if res.status_code != 200:
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        articles = soup.select(".list-block")

        for article in articles:
            if len(results) >= max_articles:
                return results  # 기사 3개 모이면 바로 반환

            title_tag = article.select_one(".list-titles strong")
            link_tag = article.select_one(".list-titles a")
            summary_tag = article.select_one(".line-height-3-2x")
            date_tag = article.select_one(".list-dated")

            if not title_tag or not link_tag:
                continue

            title = title_tag.get_text(strip=True)
            # keyword가 None이 아닐 경우에만 필터링 적용
            if keyword is not None and keyword not in title:
                continue

            link = "https://www.thinkfood.co.kr" + link_tag["href"]
            summary = (summary_tag.get_text(strip=True)[:200] + "...") if summary_tag else ""
            info = date_tag.get_text(strip=True) if date_tag else ""

            # 기사 본문에서 이미지 가져오기
            img_url = None
            try:
                res_detail = requests.get(link)
                soup_detail = BeautifulSoup(res_detail.text, "html.parser")
                img_tag = soup_detail.select_one("figure img")
                if img_tag and "src" in img_tag.attrs:
                    src = img_tag["src"]
                    img_url = src if src.startswith("http") else "https://cdn.thinkfood.co.kr" + src
            except:
                pass # 오류 무시하고 이미지 없음으로 처리

            results.append({
                "title": title,
                "summary": summary,
                "info": info,
                "link": link,
                "img_url": img_url
            })

    return results

def fetch_full_article_content(url):
    """기사 URL에서 전체 본문 내용을 가져오는 함수"""
    try:
        res = requests.get(url)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # 기사 본문 추출
        content = ""
        
        # 방법 1: div.user-snip 시도
        content_div = soup.select_one("div.user-snip")
        if content_div:
            # 광고나 관련 기사 링크 제거
            for unwanted in content_div.select('.ad, .related, .link-area, .photo-info'):
                unwanted.decompose()
            
            # 본문 p 태그들만 추출
            paragraphs = content_div.select('p')
            if paragraphs:
                content = ' '.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20])
            else:
                # p 태그가 없으면 전체 텍스트 추출
                content = content_div.get_text(strip=True)
        
        # 방법 2: article 태그 시도
        if not content:
            article_tag = soup.select_one('article')
            if article_tag:
                paragraphs = article_tag.select('p')
                content = ' '.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20])
        
        # 방법 3: .article-content 같은 클래스 시도
        if not content:
            for selector in ['.article-content', '.news-content', '.content']:
                content_area = soup.select_one(selector)
                if content_area:
                    paragraphs = content_area.select('p')
                    content = ' '.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20])
                    if content:
                        break
        
        if content:
            # 불필요한 공백과 줄바꿈 정리
            content = re.sub(r'\s+', ' ', content).strip()
            # 너무 짧은 내용은 제외
            if len(content) > 100:
                return content
        return None
    except Exception as e:
        print(f"기사 내용 추출 실패: {e}")
        return None
    
@st.cache_data(show_spinner="요약 캐시 확인 중...")
def get_cached_summary(article_urls, openai_api_key):
    """기사 링크 목록을 기반으로 GPT 요약 캐싱"""
    contents = []
    for url in article_urls:
        content = fetch_full_article_content(url)
        if content:
            contents.append(content)

    if contents:
        combined = "\n\n".join(contents)
        return summarize_with_openai(combined, openai_api_key)
    else:
        return "요약할 기사 본문을 불러오지 못했습니다."

def summarize_with_openai(content, openai_api_key):
    """OpenAI API를 사용하여 미국 식품 시장 기사 요약"""
    try:
        from openai import OpenAI

        client = OpenAI(api_key=openai_api_key)

        prompt = f"""
        다음은 최근 미국 식품 시장 관련 뉴스 기사들의 본문입니다.

        다음 조건을 반드시 지켜 요약문을 작성하세요:

        1. 현재는 2025년입니다. **2025년 이후의 내용만 포함**하고, 2024년 이전의 수치, 사례, 트렌드는 절대 포함하지 마세요.
        2. **기사에 명시된 내용만 사용**하고, 임의의 추론이나 허구의 수치는 사용하지 마세요.
        3. 문체는 객관적이고 부드럽게, 중립적 톤으로 서술하세요. (~할 수 있다, ~로 보인다 등)
        4. 각각의 항목은 단문 나열이 아니라 **부드러운 연결어와 문장 흐름**으로 구성합니다.
        5. 독자에게 ‘설명해주는 느낌’으로, **전문성이 있으면서도 과하지 않은 따뜻한 어조**를 유지합니다.

        ---

        요약은 아래 4개의 항목으로 작성하며, 각 항목은 2~3문장 내외로 정리합니다:

        **시장 환경 변화**  
        **주요 트렌드와 사례**  
        **산업 과제 및 리스크**  
        **시사점 및 전망**

        전체는 6~9문장 내외로 구성하며, **굵은 제목으로 문단을 구분**하고,  
        보고서처럼 자연스럽고 간결한 시장 분석을 작성합니다.

        기사 본문:
        {content}
        """

        system_msg = (
            "당신은 미국 식품 산업 전문 분석가입니다. "
            "기사 내용을 기반으로 시장 변화, 트렌드, 리스크, 시사점을 연결해 통찰력 있게 요약합니다. "
            "사례를 구체적으로 인용하고, 전략 제안은 현실적이어야 합니다."
        )

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt}
            ],
            max_tokens=700,
            temperature=0.3
        )

        return response.choices[0].message.content.strip()

    except Exception as e:
        return f"요약 생성 중 오류가 발생했습니다: {str(e)}"


def show_news():
    st.info("""
    최신 뉴스 기사를 통해 세계 식품 시장의 흐름을 파악할 수 있습니다. \n 
    AI가 분석한 미국의 주요 이슈 관련 인사이트를 함께 확인할 수 있습니다.
    """)
    openai_api_key = os.getenv('OPENAI_API_KEY')

    articles = fetch_articles_with_keyword(keyword=None, max_pages=5, max_articles=3)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📰 세계 뉴스 기사**")
        for article in articles:
            summary = article["summary"]
            if article["img_url"]:
                cols = st.columns([1, 3])
                with cols[0]:
                    st.image(article["img_url"], use_container_width=True)
                with cols[1]:
                    st.markdown(f"**[{article['title']}]({article['link']})**", unsafe_allow_html=True)
                    st.markdown(summary)
                    st.caption(article["info"])
            else:
                st.markdown(f"**[{article['title']}]({article['link']})**", unsafe_allow_html=True)
                st.markdown(summary)
                st.caption(article["info"])
            st.markdown("---")
        st.caption("출처: [식품음료신문](https://www.thinkfood.co.kr/)")

    with col2:
        st.markdown("**📊 미국 식품 산업 동향 요약 분석 (by OpenAI)**")
        with st.spinner("기사 내용을 바탕으로 식품 산업 동향 분석 중..."):

            # 요약용 기사 수집
            articles_for_summary = fetch_articles_with_keyword(keyword="미국", max_pages=10, max_articles=10)
            article_urls = [a["link"] for a in articles_for_summary]

            # 캐시된 요약 불러오기
            summary_result = get_cached_summary(article_urls, openai_api_key)

        st.success(summary_result)

        with st.expander("🔍 요약에 사용된 기사 출처 보기"):
            for article in articles_for_summary:
                st.markdown(f"- [{article['title']}]({article['link']})", unsafe_allow_html=True)