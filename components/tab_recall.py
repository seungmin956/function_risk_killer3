# components/tab_recall.py

import streamlit as st
import time
from utils.agent_recall import RecallAgent
from utils.chat_common_functions import (
    save_chat_history, get_session_keys, initialize_session_state,
    clear_session_state, handle_project_change, display_chat_history,
    update_chat_history, handle_example_question, handle_user_input,
    reset_processing_state, quick_stream_response  # quick_stream_response로 변경
)
from utils.function_calling_system import get_recall_vectorstore
from db_utils import get_improved_visualization_data, check_recent_data_update
from functools import lru_cache
from datetime import datetime

recall_vectorstore = get_recall_vectorstore()
agent = RecallAgent(add_hint=True)

# 리콜 관련 예시 질문
@lru_cache(maxsize=1)
def get_recall_questions():
    return [
        "지난 달에 새로 발표된 식품 리콜이 있나요?",
        "살모넬라균으로 리콜된 제품 목록을 보여줘.",
        "리콜이 가장 빈번하게 발생하는 식품 3개를 알려줘",
        "작년 대비 올해 리콜 트렌드에 변화가 있나요?"
    ]

def init_recall_session_state(session_keys):
    """리콜 특화 세션 상태 초기화"""
    initialize_session_state(session_keys)
    
    if "recall_processing_start_time" not in st.session_state:
        st.session_state.recall_processing_start_time = None
    if "viz_data" not in st.session_state:
        st.session_state.viz_data = None
    if "show_charts" not in st.session_state:
        st.session_state.show_charts = False
    if st.session_state.viz_data is None:
        update_visualization_data()

def render_improved_dashboard():
    """개선된 대시보드 - 정확한 키 사용"""
    if not st.session_state.show_charts or not st.session_state.viz_data:
        return
    
    viz_container = st.container()
    
    with viz_container:
        st.markdown("""<h1 style="font-size: 20px;">📊 리콜 데이터 분석 대시보드</h1>""", unsafe_allow_html=True)
        
        stats = st.session_state.viz_data.get('stats', {})
        if stats:
            col1, col2, col3, col4 = st.columns(4)
            
            # 카드 1: 총 데이터
            with col1:
                total_recalls = stats.get('total_recalls', 0)
                st.markdown(f"""
                <div style="
                    background-color:#f8f9fa; 
                    padding:20px; 
                    border-radius:12px; 
                    border:2px solid #dee2e6;
                    height:140px;
                    display:flex;
                    flex-direction:column;
                    justify-content:center;
                    text-align:center;
                ">
                    <p style='font-size:14px;color:#6c757d;margin:0;font-weight:500;'>총 리콜 데이터</p>
                    <p style='font-size:28px;font-weight:bold;color:#212529;margin:8px 0;'>{total_recalls:,}건</p>
                </div>
                """, unsafe_allow_html=True)
            
            # 카드 2: 최근 추가
            with col2:
                recent_added = stats.get('recent_added', 0)
                recent_period = stats.get('recent_period', '이번 주')
                has_new = stats.get('has_new_data', False)
                
                if has_new and recent_added > 0:
                    bg_color, border_color, text_color = "#d4edda", "#c3e6cb", "#155724"
                elif recent_added == 0:
                    bg_color, border_color, text_color = "#f8d7da", "#f5c6cb", "#721c24"
                else:
                    bg_color, border_color, text_color = "#fff3cd", "#ffeaa7", "#856404"
                
                st.markdown(f"""
                <div style="
                    background-color:{bg_color}; 
                    padding:20px; 
                    border-radius:12px; 
                    border:2px solid {border_color};
                    height:140px;
                    display:flex;
                    flex-direction:column;
                    justify-content:center;
                    text-align:center;
                ">
                    <p style='font-size:14px;color:{text_color};margin:0;font-weight:500;'>최근 추가</p>
                    <p style='font-size:28px;font-weight:bold;color:{text_color};margin:8px 0;'>{recent_added}건</p>
                </div>
                """, unsafe_allow_html=True)
            
            # 카드 3: 기존 데이터
            with col3:
                baseline_data = stats.get('baseline_data', 0)
                st.markdown(f"""
                <div style="
                    background-color:#f8f9fa; 
                    padding:20px; 
                    border-radius:12px; 
                    border:2px solid #dee2e6;
                    height:140px;
                    display:flex;
                    flex-direction:column;
                    justify-content:center;
                    text-align:center;
                ">
                    <p style='font-size:14px;color:#212529;margin:0;font-weight:500;'>기존 데이터</p>
                    <p style='font-size:28px;font-weight:bold;color:#212529;margin:8px 0;'>{baseline_data:,}건</p>
                </div>
                """, unsafe_allow_html=True)
            
            # 카드 4: 마지막 업데이트
            with col4:
                last_update = stats.get('last_update', '정보 없음')
                update_method = stats.get('update_method', '수동')
                days_ago = stats.get('days_since_update', 999)
                
                if days_ago <= 3:
                    status_emoji = "🟢"
                elif days_ago <= 7:
                    status_emoji = "🟡"
                else:
                    status_emoji = "🔴"
                
                if last_update != '정보 없음' and last_update != '오류' and len(last_update) > 10:
                    display_date = last_update[:10]
                    display_time = last_update[11:16] if len(last_update) > 16 else ""
                else:
                    display_date = last_update[:12] if len(last_update) > 12 else last_update
                    display_time = ""
                
                st.markdown(f"""
                <div style="
                    background-color:#f8f9fa; 
                    padding:20px; 
                    border-radius:12px; 
                    border:2px solid #dee2e6;
                    height:140px;
                    display:flex;
                    flex-direction:column;
                    justify-content:center;
                    text-align:center;
                ">
                    <p style='font-size:14px;color:#212529;margin:0;font-weight:500;'>{status_emoji} 마지막 업데이트</p>
                    <p style='font-size:20px;font-weight:bold;color:#212529;margin:4px 0;'>{display_date}</p>
                    <p style='font-size:12px;color:#212529;margin:0;'>{display_time} ({update_method})</p>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div style="margin-top:15px; padding:12px; background-color:#f8f9fa; border-radius:8px; border-left:4px solid #007bff;">
            <p style="margin:0; font-size:14px;">
                <strong>업데이트 방식:</strong> 
                매주 월요일 정기적으로 실시간 데이터가 업데이트됩니다.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)

def update_visualization_data():
    """시각화 데이터 업데이트"""
    if recall_vectorstore is None:
        return
    
    try:
        viz_data = get_improved_visualization_data()
        
        if viz_data and viz_data.get('has_data'):
            st.session_state.viz_data = viz_data
            st.session_state.show_charts = True
        else:
            st.session_state.show_charts = False
            
    except Exception as e:
        st.error(f"시각화 데이터 업데이트 오류: {e}")
        st.session_state.show_charts = False

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
        recall_questions = get_recall_questions()
        
        cols = st.columns(2)
        for i, question in enumerate(recall_questions[:4]):
            col_idx = i % 2
            with cols[col_idx]:
                label = question # 질문 전체 문구 그대로 사용
                
                if st.button(
                    label,
                    key=f"recall_example_{i}", 
                    use_container_width=True, 
                    disabled=is_processing,
                    help=question
                ):
                    handle_example_question(question, session_keys)
                    st.rerun()

def render_chat_area(session_keys, is_processing):
    """메인 채팅 영역 렌더링 - 빠른 모드 전용"""
    
    render_improved_dashboard()
    render_example_questions(session_keys, is_processing)
    
    # 대화 기록 표시
    chat_container = st.container()
    with chat_container:
        display_chat_history(session_keys)
    
    # 질문 처리 - 항상 빠른 모드로 스트리밍
    if st.session_state[session_keys["selected_question"]]:
        if not st.session_state.recall_processing_start_time:
            st.session_state.recall_processing_start_time = datetime.now()
        
        with st.chat_message("assistant"):
            # 스트리밍 출력을 위한 빈 공간 생성
            response_placeholder = st.empty()
            
            with st.spinner("🔍 실시간 데이터 수집 및 분석 중..."):
                try:
                    # 스피너와 함께 초기 메시지 표시
                    response_placeholder.markdown("💭 리콜 데이터를 분석하고 있습니다...")
                    
                    current_question = st.session_state[session_keys["selected_question"]]
                    
                    # agent_recall 실행
                    result = agent.run(
                        query=current_question,
                        history=st.session_state[session_keys["langchain_history"]]
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
                    
                    # 처리 타입 표시
                    processing_type = result.get("processing_type", "unknown")
                    if processing_type == "agent":
                        st.info("🧠 Agent 컨트롤러로 처리됨")
                    elif processing_type == "function_calling":
                        st.info("⚡ Function Calling으로 처리됨")
                        function_calls = result.get('function_calls', [])
                        if function_calls:
                            with st.expander("🔧 실행된 함수들 보기"):
                                for i, call in enumerate(function_calls, 1):
                                    func_name = call.get('function', '알 수 없음')
                                    args = call.get('args', {})
                                    st.code(f"{i}. {func_name}({args})")
                    elif processing_type == "direct_answer":
                        st.info("💬 직접 답변")
                    else:
                        st.info("📄 처리 완료")
                    
                    # 처리 시간 표시
                    if st.session_state.recall_processing_start_time:
                        processing_time = (datetime.now() - st.session_state.recall_processing_start_time).total_seconds()
                        st.caption(f"⏱️ 처리 시간: {processing_time:.1f}초")
                    
                    # 실시간 데이터 정보 표시
                    if result.get("has_realtime_data"):
                        st.info(f"⚡ 실시간 데이터 {result.get('realtime_count', 0)}건 포함됨")
                    
                    # 시각화 데이터 업데이트
                    update_visualization_data()
                    
                    # 히스토리 업데이트
                    update_chat_history(
                        current_question, 
                        answer, 
                        session_keys, 
                        result.get("chat_history", [])
                    )
                    
                    reset_processing_state(session_keys)
                    st.session_state.recall_processing_start_time = None
                    
                    # 완료 메시지
                    time.sleep(0.3)
                    st.info("🔍 리콜 AI 답변 완료")
                    
                except Exception as e:
                    response_placeholder.markdown(f"❌ 답변 생성 중 오류: {str(e)[:100]}...")
                    reset_processing_state(session_keys)
                    st.session_state.recall_processing_start_time = None
                
                st.rerun()

def show_recall_chat():
    """리콜 전용 챗봇 - 빠른 모드 전용 버전"""
    st.info("""
    🔎 **자동 실시간 리콜 분석 시스템** 
    - 질문 시, 최신 리콜 데이터를 실시간으로 자동 수집
    - 기존 DB와 통합하여 리콜 이슈를 분석 제공
    - 저장한 대화는 '기획안 요약 도우미' 탭에서 자동 요약 가능
    """)
    
    chat_mode = "리콜사례"
    session_keys = get_session_keys(chat_mode)
    
    # 세션 상태 초기화
    init_recall_session_state(session_keys)

    # 레이아웃 - 설정 컬럼 제거하고 2개 컬럼만 사용
    col_left, col_center = st.columns([1, 4])
   
    with col_left:
        # 프로젝트 이름 입력
        project_name = st.text_input("프로젝트 이름", placeholder="리콜 프로젝트명", key="recall_project_input")
        
        # 사이드바 컨트롤 렌더링
        has_project_name, has_chat_history, is_processing = render_sidebar_controls(project_name, chat_mode, session_keys)

    with col_center:
        # 메인 채팅 영역
        render_chat_area(session_keys, is_processing)
        
        # 사용자 입력
        if not is_processing:
            user_input = st.chat_input(
                "리콜 관련 질문을 입력하세요...", 
                key="recall_chat_input"
            )
            if user_input and user_input.strip():
                if len(user_input.strip()) < 3:
                    st.warning("⚠️ 질문이 너무 짧습니다.")
                else:
                    handle_user_input(user_input.strip(), session_keys)
                    st.rerun()
        else:
            st.info("🔄 실시간 데이터 수집 및 분석 중입니다...")

# 추가 최적화 함수들
def preload_recall_data():
    """앱 시작 시 리콜 데이터 미리 로드"""
    if "recall_preloaded" not in st.session_state:
        st.session_state.viz_data = get_improved_visualization_data()
        st.session_state.recall_preloaded = True

def check_new_realtime_data():
    """실시간 신규 데이터 확인"""
    try:
        return check_recent_data_update()
    except Exception as e:
        print(f"실시간 데이터 확인 오류: {e}")
        return False