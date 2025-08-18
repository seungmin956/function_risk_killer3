# components/tab_regulation.py

import streamlit as st
import glob
import json
import time
from utils.chat_regulation import ask_question
from utils.chat_common_functions import (
    save_chat_history, get_session_keys, initialize_session_state,
    clear_session_state, handle_project_change, display_chat_history,
    update_chat_history, handle_example_question, handle_user_input,
    reset_processing_state, quick_stream_response  # quick_stream_response로 변경
)
import utils.c as c
from functools import lru_cache
import os
from datetime import datetime
import asyncio

# 캐시된 규제 데이터 로딩
@st.cache_data(ttl=300)  # 5분 TTL
def load_recent_regulation_data():
    """최신 크롤링 결과 파일 로드 - 캐시 적용"""
    try:
        pattern = "./risk_federal_changes_*.json"
        json_files = glob.glob(pattern)
        
        if not json_files:
            return None
        
        # 파일 수정 시간 기준으로 정렬
        latest_file = max(json_files, key=os.path.getmtime)
        
        with open(latest_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        # 데이터 전처리
        for item in data:
            # HTML 변환
            if 'summary_korean' in item:
                item['summary_html'] = item['summary_korean'].replace('\n', '<br>')
                
        return data
        
    except Exception as e:
        st.error(f"규제 데이터 로드 실패: {e}")
        return None

@st.cache_data(ttl=300)
def get_filtered_regulations(regulation_data, page_size=5, page_num=0):
    """규제 데이터 필터링 및 페이지네이션"""
    if not regulation_data:
        return []
    
    start_idx = page_num * page_size
    end_idx = start_idx + page_size
    return regulation_data[start_idx:end_idx]

def display_recent_regulations(regulation_data, max_items=5):
    """최근 규제 변경 내용을 하나의 익스팬더 안에 리스트처럼 표시"""
    if not regulation_data:
        st.info("📋 표시할 규제 변경 내용이 없습니다.")
        return

    items_to_show = get_filtered_regulations(regulation_data, max_items, 0)

    with st.expander("📋 최근 규제 변경", expanded=False):
        for i, item in enumerate(items_to_show):
            title = item.get('title_korean', '제목 없음')
            date = item.get('change_date', 'N/A')
            url = item.get('url', None)
            summary_html = item.get('summary_html', item.get('summary_korean', '').replace('\n', '<br>'))

            st.markdown(f"### {title}")
            st.markdown(f"📅 변경일: **{date}**    🔗 [원문 보기]({url})")

            if summary_html:
                st.markdown(f"""
                <div style="margin-top:8px; padding:10px; background-color:#F0F2F5; border-radius:6px;">
                    <b>내용 요약:</b><br>
                    {summary_html}
                </div>
                """, unsafe_allow_html=True)

            if i < len(items_to_show) - 1: # 마지막 항목이 아니라면 --- 구분선 삽입
                st.markdown("---")

# 예시 질문 캐싱
@lru_cache(maxsize=1)
def get_regulation_questions():
    """규제 예시 질문 목록 - 캐시 적용"""
    return [
        "미국으로 식품을 처음 수출할 때, 준비해야 할 서류 목록은?", 
        "글루텐 프리(Gluten-Free) 표기를 위한 조건은?", 
        "미국 통관 거부 시 대처법 알려줘",
        "심장 건강에 도움' 같은 문구 사용 조건은?"
    ]

# 모니터링 상태 관리
def init_monitoring_state():
    """모니터링 관련 세션 상태 초기화"""
    if "monitoring_in_progress" not in st.session_state:
        st.session_state.monitoring_in_progress = False
    if "last_monitoring_time" not in st.session_state:
        st.session_state.last_monitoring_time = None
    if "regulation_processing_start_time" not in st.session_state:
        st.session_state.regulation_processing_start_time = None

def render_sidebar_controls(project_name, chat_mode, session_keys):
    """사이드바 컨트롤 패널 렌더링"""
    project_changed = handle_project_change(project_name, chat_mode, session_keys)
    if project_changed:
        st.rerun()
    elif project_name:
        st.success(f"✅ '{project_name}' 진행 중")
    
    has_project_name = bool(project_name and project_name.strip())
    has_chat_history = bool(st.session_state[session_keys["chat_history"]])
    is_processing = st.session_state[session_keys["is_processing"]]
    
    # 저장 버튼
    save_disabled = not (has_project_name and has_chat_history) or is_processing
    if st.button("💾 대화 저장", disabled=save_disabled, use_container_width=True):
        if has_project_name and has_chat_history:
            with st.spinner("저장 중..."):
                success = save_chat_history(
                    project_name.strip(),
                    st.session_state[session_keys["chat_history"]],
                    st.session_state[session_keys["langchain_history"]],
                    chat_mode
                )
                if success:
                    st.success("✅ 저장 완료!")
                else:
                    st.error("❌ 저장 실패")
    
    # 초기화 버튼
    clear_disabled = not (has_project_name and has_chat_history) or is_processing
    if st.button("🗑️ 대화 초기화", disabled=clear_disabled, use_container_width=True):
        clear_session_state(session_keys)
        st.success("초기화 완료")
        st.rerun()
    
    return has_project_name, has_chat_history, is_processing

def render_example_questions(session_keys, is_processing):
    """예시 질문 섹션 렌더링"""
    with st.expander("💡 예시 질문", expanded=True):
        regulation_questions = get_regulation_questions()
        
        cols = st.columns(2)
        for i, question in enumerate(regulation_questions[:4]):
            col_idx = i % 2
            with cols[col_idx]:
                label = question # 질문 전체 문구 그대로 사용
                
                if st.button(
                    label,
                    key=f"regulation_example_{i}", 
                    use_container_width=True, 
                    disabled=is_processing,
                    help=question
                ):
                    handle_example_question(question, session_keys)
                    st.rerun()

def render_monitoring_section(is_processing):
    """모니터링 섹션 렌더링"""
    st.markdown("### 📡 규제 변경 모니터링")
    monitoring_disabled = st.session_state.monitoring_in_progress or is_processing
    
    if st.button("📡 모니터링 시작", key="regulation_monitoring", 
                use_container_width=True, disabled=monitoring_disabled):
        st.session_state.monitoring_in_progress = True
        
        with st.spinner("FDA 최신 규제 정보 수집 중..."):
            try:
                st.cache_data.clear()
                c.main()
                
                regulation_data = load_recent_regulation_data()
                if regulation_data:
                    st.session_state.recent_regulation_data = regulation_data
                    st.session_state.last_monitoring_time = datetime.now().strftime("%H:%M:%S")
                    st.session_state.regulation_display_ready = True
                    st.success(f"📡 완료! {len(regulation_data)}건 수집")
                else:
                    st.warning("수집된 데이터가 없습니다.")
                    
            except Exception as e:
                st.error(f"❌ 모니터링 오류: {str(e)[:50]}...")
                
            finally:
                st.session_state.monitoring_in_progress = False
    
    if st.session_state.last_monitoring_time:
        st.caption(f"🕐 마지막 업데이트: {st.session_state.last_monitoring_time}")

def render_chat_area(session_keys, is_processing):
    """메인 채팅 영역 렌더링 - 빠른 모드 전용"""
    
    # 최근 규제 변경 내용 표시
    if st.session_state.get("regulation_display_ready"):
        display_recent_regulations(st.session_state.recent_regulation_data)
    
    render_example_questions(session_keys, is_processing)
    
    # 대화 기록 표시
    chat_container = st.container()
    with chat_container:
        display_chat_history(session_keys)
    
    # 질문 처리 - 항상 빠른 모드로 스트리밍
    if st.session_state[session_keys["selected_question"]]:
        if not st.session_state.regulation_processing_start_time:
            st.session_state.regulation_processing_start_time = datetime.now()
        
        with st.chat_message("assistant"):
            # 스트리밍 출력을 위한 빈 공간 생성
            response_placeholder = st.empty()
            
            with st.spinner("🏛️ 규제 데이터 분석 중..."):
                try:
                    # 스피너와 함께 초기 메시지 표시
                    response_placeholder.markdown("💭 규제 문서를 분석하고 있습니다...")
                    
                    current_question = st.session_state[session_keys["selected_question"]]
                    
                    # 규제 질문 처리
                    result = ask_question(
                        current_question,
                        st.session_state[session_keys["langchain_history"]]
                    )
                    
                    # answer 추출
                    answer = result.get("answer", "답변을 생성할 수 없습니다.")
                    
                    # 항상 빠른 모드로 스트리밍 (청크 단위)
                    if answer:
                        # 빠른 스트리밍 애니메이션 실행
                        quick_stream_response(
                            answer, 
                            response_placeholder, 
                            chunk_size=20,  # 한번에 20단어씩 표시
                            delay=0.5  # 청크 간 0.5초 딜레이
                        )
                    else:
                        response_placeholder.markdown("죄송합니다. 답변을 생성할 수 없습니다.")
                    
                    # 처리 시간 표시
                    if st.session_state.regulation_processing_start_time:
                        processing_time = (datetime.now() - st.session_state.regulation_processing_start_time).total_seconds()
                        st.caption(f"⏱️ 처리 시간: {processing_time:.1f}초")
                    
                    # 히스토리 업데이트
                    update_chat_history(
                        current_question, 
                        answer, 
                        session_keys, 
                        result.get("chat_history", [])
                    )
                    
                    reset_processing_state(session_keys)
                    st.session_state.regulation_processing_start_time = None
                    
                    # 완료 메시지
                    time.sleep(0.3)
                    st.info("🏛️ 규제 AI 답변 완료")
                    
                except Exception as e:
                    response_placeholder.markdown(f"❌ 답변 생성 중 오류: {str(e)[:100]}...")
                    reset_processing_state(session_keys)
                    st.session_state.regulation_processing_start_time = None
                
                st.rerun()

def show_regulation_chat():
    """규제 전용 챗봇 - 빠른 모드 전용 버전"""
    st.info("""
    🤖 **AI 챗봇을 활용한 FDA 규제 관련 정보 분석 시스템**
    - FDA 관련 규제 정보와 출처 URL 제공
    - 공식 데이터 기반의 신뢰도 높은 응답
    - 저장한 대화는 '기획안 요약 도우미' 탭에서 자동 요약 가능
    """)
    
    chat_mode = "규제"
    session_keys = get_session_keys(chat_mode)
    
    # 세션 상태 초기화
    initialize_session_state(session_keys)
    init_monitoring_state()
    
    # 규제 전용 세션 상태 - 조건부 초기화
    if "recent_regulation_data" not in st.session_state:
        st.session_state.recent_regulation_data = load_recent_regulation_data()

    # 레이아웃 - 설정 컬럼 제거하고 2개 컬럼만 사용
    col_left, col_center = st.columns([1, 4])
   
    with col_left:
        # 프로젝트 이름 입력
        project_name = st.text_input(
            "프로젝트 이름", 
            placeholder="규제 프로젝트명", 
            key="regulation_project_input"
        )
        
        # 사이드바 컨트롤 렌더링
        has_project_name, has_chat_history, is_processing = render_sidebar_controls(
            project_name, chat_mode, session_keys
        )
        
        # 모니터링 섹션
        render_monitoring_section(is_processing)

    with col_center:
        # 메인 채팅 영역
        render_chat_area(session_keys, is_processing)
        
        # 사용자 입력
        if not is_processing:
            user_input = st.chat_input(
                "규제 관련 질문을 입력하세요...", 
                key="regulation_chat_input"
            )
            if user_input and user_input.strip():
                if len(user_input.strip()) < 3:
                    st.warning("⚠️ 질문이 너무 짧습니다.")
                else:
                    handle_user_input(user_input.strip(), session_keys)
                    st.rerun()
        else:
            st.info("🔄 처리 중입니다. 잠시만 기다려주세요...")

# 추가 최적화 함수들
@st.cache_data(ttl=3600)  # 1시간 캐시
def get_regulation_statistics():
    """규제 데이터 통계 정보"""
    data = load_recent_regulation_data()
    if not data:
        return {}
    
    return {
        "total_count": len(data),
        "latest_date": max(item.get('change_date', '') for item in data),
        "categories": len(set(item.get('category', 'unknown') for item in data))
    }

def preload_regulation_data():
    """앱 시작 시 규제 데이터 미리 로드"""
    if "regulation_preloaded" not in st.session_state:
        st.session_state.recent_regulation_data = load_recent_regulation_data()
        st.session_state.regulation_preloaded = True