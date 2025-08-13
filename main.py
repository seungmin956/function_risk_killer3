#main.py
import sys

# SQLite 버전 문제 해결 (ChromaDB용) - 더 강력한 방법
try:
    # pysqlite3를 sqlite3로 완전히 대체
    import pysqlite3
    sys.modules['sqlite3'] = pysqlite3
    
    # 추가 패치: __pysqlite__ 모듈도 대체
    import pysqlite3.dbapi2 as sqlite3
    sys.modules['pysqlite2'] = pysqlite3
    sys.modules['pysqlite2.dbapi2'] = pysqlite3.dbapi2
    
except ImportError:
    pass

import streamlit as st
# 페이지 기본 설정
st.set_page_config(page_title="Risk Killer", page_icon="🔪", layout="wide")

# CSS 스타일
st.markdown("""
<style>
@keyframes glitterSweep {
  0% {background-position: -200% 0;}
  100% {background-position: 200% 0;}
}

/* 기본 텍스트 크기 설정 */
html, body, [class*="css"] {
  font-size: 22px !important;
}

/* 제목 태그 (h1 ~ h4) 크기/굵기 설정 */
h1, h2, h3, h4 {
  font-size: 26px !important;
  font-weight: bold !important;
}

/* 입력/버튼/라디오 글자 크기 설정 */
.stTextInput > div > input,
.stChatInput > div > textarea,
.stRadio > div {
  font-size: 17px !important;
}
            
/* st.alert 계열의 스타일을 커스터마이징 */
.stAlert > div {
    background-color: #E5E5E5;  /* 배경색 */
    color: #1F1F1F;  /* 텍스트 색상 */
}            

.main-header {
  text-align: center;
  padding: 2rem 0;
  border-radius: 10px;
  margin-bottom: 2rem;
  background: linear-gradient(60deg,
    transparent 0%,
    rgba(255,255,255,0.3) 20%,
    transparent 40%),
    #764ba2;
  background-size: 200% 100%;
  animation: glitterSweep 8s linear infinite;
  color: #FFFFFF;
}

.main-title {
  font-size: 3.5rem;
  font-weight: 800;
  margin-bottom: 0.5rem;
}
</style>
""", unsafe_allow_html=True)

# 헤더 표시
st.markdown("""
<div class="main-header">
    <div class="main-title">Risk Killer</div>
</div>
""", unsafe_allow_html=True)

# 탭 상태 초기화
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = 'market'

# 탭 정의
tabs = {'market': '📢 시장 동향', 'news': '🌏 식료품 뉴스', 'chatbot': '🤖 AI Q&A 챗봇', 'risk': '🔎 리스크 검토', 'summary': '📝 기획안 요약 도우미'}

# 탭 버튼 생성
cols = st.columns(len(tabs))
for i, (tab_key, tab_name) in enumerate(tabs.items()):
    with cols[i]:
        if st.button(tab_name, key=f"tab_{tab_key}", use_container_width=True):
            st.session_state.active_tab = tab_key
            st.rerun()

# CSS로 버튼 스타일링
st.markdown(f"""
<style>
button[kind="secondary"] {{
    background: linear-gradient(135deg, #ffffff 0%, #f1f3f4 100%) !important;
    border: 2px solid #e0e0e0 !important;
    border-radius: 12px !important;
    padding: 14px 24px !important;
    font-weight: 700 !important;
    font-size: 17px !important;
    color: #333333 !important;
    transition: all 0.3s ease !important;
    box-shadow: 0 3px 8px rgba(0,0,0,0.15) !important;
    text-transform: none !important;
    letter-spacing: 0.5px !important;
}}

/* 호버 효과 */
button[kind="secondary"]:hover {{
    background: linear-gradient(135deg, #f8f4ff 0%, #ede7f6 100%) !important;
    border-color: #9C27B0 !important;
    transform: translateY(-3px) !important;
    box-shadow: 0 6px 20px rgba(156, 39, 176, 0.3) !important;
    color: #6A1B9A !important;
}}

/* 클릭/활성 상태 - 연보라색 */
button[kind="secondary"]:active,
button[kind="secondary"]:focus {{
    background: linear-gradient(135deg, #9C27B0 0%, #7B1FA2 100%) !important;
    color: white !important;
    border-color: #9C27B0 !important;
    box-shadow: 0 6px 20px rgba(156, 39, 176, 0.5) !important;
    transform: translateY(-2px) !important;
}}
</style>
""", unsafe_allow_html=True)

# 탭 내용 표시
if st.session_state.active_tab == 'market':
    try:
        from components.tab_tableau import create_market_dashboard
        create_market_dashboard()
    except ImportError:
        st.error("시장 동향 모듈을 불러올 수 없습니다.")
    except Exception as e:
        st.error(f"시장 동향 로딩 중 오류 발생: {str(e)}")

elif st.session_state.active_tab == 'news':
    try:
        from components.tab_news import show_news
        show_news()
    except ImportError:
        st.error("뉴스 모듈을 불러올 수 없습니다.")
    except Exception as e:
        st.error(f"뉴스 로딩 중 오류 발생: {str(e)}")

elif st.session_state.active_tab == 'chatbot':
    # AI Q&A 챗봇 탭 전용 버튼 스타일
    # st.markdown("""
    # <style>
    # .stButton > button[kind="primary"] {
    #     background-color: #A8E6CF !important;
    #     border-color: #A8E6CF !important;
    #     color: #2C3E50 !important;
    # }
    # .stButton > button[kind="primary"]:hover {
    #     background-color: #7FCDCD !important;
    #     border-color: #7FCDCD !important;
    #     color: white !important;
    # }
    # </style>
    # """, unsafe_allow_html=True)
    
    try:
        from components.tab_regulation import show_regulation_chat
        show_regulation_chat()
    except ImportError:
        st.error("규제 챗봇 모듈을 불러올 수 없습니다.")
    except Exception as e:
        st.error(f"규제 챗봇 로딩 중 오류 발생: {str(e)}")

elif st.session_state.active_tab == 'risk':
    # 리스크 검토 탭 전용 버튼 스타일
    # st.markdown("""
    # <style>
    # .stButton > button[kind="primary"] {
    #     background-color: #FFD93D !important;
    #     border-color: #FFD93D !important;
    #     color: #2C3E50 !important;
    # }
    # .stButton > button[kind="primary"]:hover {
    #     background-color: #FFC312 !important;
    #     border-color: #FFC312 !important;
    # }
    # </style>
    # """, unsafe_allow_html=True)
    
    try:
        from components.tab_recall import show_recall_chat
        show_recall_chat()
    except ImportError:
        st.error("리콜 모듈을 불러올 수 없습니다.")
    except Exception as e:
        st.error(f"리콜 로딩 중 오류 발생: {str(e)}")

elif st.session_state.active_tab == 'summary':
    # 기획안 요약 도우미 탭 전용 버튼 스타일
    # st.markdown("""
    # <style>
    # .stButton > button[kind="primary"] {
    #     background-color: #5DADE2 !important;
    #     border-color: #5DADE2 !important;
    #     color: white !important;
    # }
    # .stButton > button[kind="primary"]:hover {
    #     background-color: #357ABD !important;
    #     border-color: #357ABD !important;
    # }
    # </style>
    # """, unsafe_allow_html=True)
    
    try:
        from components.tab_export import show_export_helper
        show_export_helper()
    except ImportError:
        st.error("내보내기 도우미 모듈을 불러올 수 없습니다.")
    except Exception as e:
        st.error(f"내보내기 도우미 로딩 중 오류 발생: {str(e)}")
