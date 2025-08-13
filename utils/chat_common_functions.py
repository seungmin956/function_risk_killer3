# utils/chat_common_functions.py
"""
챗봇 공통 기능 모음 - 최적화 버전
- 세션 상태 관리
- 대화 기록 저장/로드
- LangChain 히스토리 변환
- 기타 공통 유틸리티
"""
import streamlit as st
import json
import os
import glob
from datetime import datetime
from typing import List, Dict, Any, Optional
from langchain_core.messages import AIMessage, HumanMessage
import threading
from functools import lru_cache

# 대화 기록 파일 경로
CHAT_HISTORY_FILE = "chat_histories.json"

# 파일 락 객체 (동시 접근 방지)
_file_lock = threading.Lock()

# 캐시된 히스토리 데이터
@st.cache_data(ttl=60)  # 60초 TTL로 캐싱
def _load_all_histories() -> Dict:
    """모든 대화 기록을 캐시와 함께 로드"""
    try:
        if not os.path.exists(CHAT_HISTORY_FILE):
            return {}
            
        with open(CHAT_HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"파일 로드 실패: {e}")
        return {}

def save_chat_history(project_name: str, chat_history: List, langchain_history: List, chat_mode: str) -> bool:
    """프로젝트 대화 기록을 JSON 파일에 저장 - 최적화 버전"""
    try:
        # 파일 락 사용으로 동시 접근 방지
        with _file_lock:
            # 기존 데이터 로드 (캐시 무효화)
            st.cache_data.clear()  # 캐시 클리어
            all_histories = _load_all_histories()
            
            # 프로젝트 데이터 업데이트 - 모드별로 분리 저장
            project_key = f"{project_name}_{chat_mode}"
            
            # LangChain 히스토리 직렬화 최적화
            serialized_langchain = []
            if langchain_history:
                for msg in langchain_history:
                    msg_type = "HumanMessage" if isinstance(msg, HumanMessage) else "AIMessage"
                    serialized_langchain.append({
                        "type": msg_type, 
                        "content": msg.content
                    })
            
            all_histories[project_key] = {
                "last_updated": datetime.now().isoformat(),
                "chat_mode": chat_mode,
                "chat_history": chat_history,
                "langchain_history": serialized_langchain
            }
            
            # 파일 저장 (원자적 쓰기)
            temp_file = f"{CHAT_HISTORY_FILE}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(all_histories, f, ensure_ascii=False, indent=2)
            
            # 원자적 파일 교체
            os.replace(temp_file, CHAT_HISTORY_FILE)
            
            return True
    except Exception as e:
        st.error(f"저장 실패: {e}")
        return False

def load_chat_history(project_name: str, chat_mode: str) -> Optional[Dict]:
    """프로젝트 대화 기록 로드 - 캐시 활용"""
    try:
        all_histories = _load_all_histories()
        project_key = f"{project_name}_{chat_mode}"
        return all_histories.get(project_key)
    except Exception as e:
        st.error(f"불러오기 실패: {e}")
        return None

@lru_cache(maxsize=128)  # LRU 캐시로 메시지 객체 재생성 방지
def _create_message_object(msg_type: str, content: str):
    """메시지 객체 생성 - 캐시 적용"""
    if msg_type == "HumanMessage":
        return HumanMessage(content=content)
    elif msg_type == "AIMessage":
        return AIMessage(content=content)
    return None

def restore_langchain_history(langchain_data: List[Dict]) -> List:
    """JSON에서 불러온 데이터를 LangChain 메시지 객체로 변환 - 최적화"""
    if not langchain_data:
        return []
    
    restored = []
    try:
        for msg_data in langchain_data:
            msg_obj = _create_message_object(msg_data["type"], msg_data["content"])
            if msg_obj:
                restored.append(msg_obj)
    except Exception as e:
        print(f"LangChain 히스토리 복원 실패: {e}")
    
    return restored

# 세션 키 생성도 캐시 적용
@lru_cache(maxsize=32)
def get_session_keys(chat_mode: str) -> Dict[str, str]:
    """챗봇 모드별 세션 상태 키 생성 - 캐시 적용"""
    return {
        "chat_history": f"chat_history_{chat_mode}",
        "langchain_history": f"langchain_history_{chat_mode}",
        "project_name": f"current_project_name_{chat_mode}",
        "is_processing": f"is_processing_{chat_mode}",
        "selected_question": f"selected_question_{chat_mode}"
    }

def initialize_session_state(session_keys: Dict[str, str]) -> None:
    """세션 상태 초기화 - 조건 체크 최적화"""
    # 딕셔너리 컴프리헨션으로 한 번에 처리
    defaults = {
        session_keys["selected_question"]: "",
        session_keys["is_processing"]: False,
        session_keys["chat_history"]: [],
        session_keys["langchain_history"]: [],
        session_keys["project_name"]: ""
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

def clear_session_state(session_keys: Dict[str, str]) -> None:
    """세션 상태 초기화 (대화 기록 삭제) - 배치 처리"""
    # 한 번에 여러 상태 업데이트
    updates = {
        session_keys["chat_history"]: [],
        session_keys["langchain_history"]: [],
        session_keys["is_processing"]: False,
        session_keys["selected_question"]: ""
    }
    
    for key, value in updates.items():
        st.session_state[key] = value

def handle_project_change(project_name: str, chat_mode: str, session_keys: Dict[str, str]) -> bool:
    """프로젝트 변경 처리 - 조건 체크 최적화"""
    current_project = st.session_state.get(session_keys["project_name"], "")
    
    # 프로젝트 변경이 없으면 빠르게 반환
    if not project_name or project_name == current_project:
        return False
    
    # 프로젝트 변경 처리
    st.session_state[session_keys["project_name"]] = project_name
    
    # 기존 대화 기록 불러오기
    project_data = load_chat_history(project_name, chat_mode)
    
    if project_data:
        # 대화 기록 복원
        st.session_state[session_keys["chat_history"]] = project_data.get("chat_history", [])
        
        # LangChain 히스토리 복원
        langchain_data = project_data.get("langchain_history", [])
        if langchain_data:
            st.session_state[session_keys["langchain_history"]] = restore_langchain_history(langchain_data)
        else:
            st.session_state[session_keys["langchain_history"]] = []
        
        st.success(f"'{project_name}' ({chat_mode}) 프로젝트의 이전 대화를 불러왔습니다.")
    else:
        # 새 프로젝트인 경우 기록 초기화
        st.session_state[session_keys["chat_history"]] = []
        st.session_state[session_keys["langchain_history"]] = []
        st.success(f"'{project_name}' ({chat_mode}) 새 프로젝트를 시작합니다.")
    
    return True

def display_chat_history(session_keys: Dict[str, str]) -> None:
    """대화 기록 출력 - 메모리 효율적 렌더링"""
    chat_history = st.session_state.get(session_keys["chat_history"], [])
    
    # 빈 히스토리는 빠르게 반환
    if not chat_history:
        return
    
    # 메시지 출력
    for msg in chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

def update_chat_history(question: str, answer: str, session_keys: Dict[str, str], chat_history: List) -> None:
    """대화 기록 업데이트 - 배치 처리"""
    # 새 메시지들을 한 번에 추가
    new_messages = [
        {"role": "user", "content": question},
        {"role": "assistant", "content": answer}
    ]
    
    # 현재 히스토리 가져오기
    current_history = st.session_state.get(session_keys["chat_history"], [])
    current_history.extend(new_messages)
    
    # 세션 상태 업데이트
    st.session_state[session_keys["chat_history"]] = current_history
    st.session_state[session_keys["langchain_history"]] = chat_history

def handle_example_question(question: str, session_keys: Dict[str, str]) -> None:
    """예시 질문 처리 - 배치 업데이트"""
    # 한 번에 여러 상태 업데이트
    st.session_state.update({
        session_keys["selected_question"]: question,
        session_keys["is_processing"]: True
    })

def handle_user_input(user_input: str, session_keys: Dict[str, str]) -> None:
    """사용자 입력 처리 - 배치 업데이트"""
    # 한 번에 여러 상태 업데이트
    st.session_state.update({
        session_keys["selected_question"]: user_input,
        session_keys["is_processing"]: True
    })

def reset_processing_state(session_keys: Dict[str, str]) -> None:
    """처리 상태 리셋 - 배치 업데이트"""
    # 한 번에 여러 상태 업데이트
    st.session_state.update({
        session_keys["selected_question"]: "",
        session_keys["is_processing"]: False
    })

# 추가 최적화 함수들
def get_project_list() -> List[str]:
    """프로젝트 목록 조회 - 캐시 적용"""
    try:
        all_histories = _load_all_histories()
        projects = set()
        for project_key in all_histories.keys():
            # 프로젝트명과 모드 분리
            if '_' in project_key:
                project_name = '_'.join(project_key.split('_')[:-1])
                projects.add(project_name)
        return sorted(list(projects))
    except Exception:
        return []

def cleanup_old_histories(days_to_keep: int = 30) -> None:
    """오래된 대화 기록 정리 (선택사항)"""
    try:
        all_histories = _load_all_histories()
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
        
        cleaned_histories = {}
        for project_key, data in all_histories.items():
            try:
                last_updated = datetime.fromisoformat(data["last_updated"]).timestamp()
                if last_updated > cutoff_date:
                    cleaned_histories[project_key] = data
            except Exception:
                # 날짜 파싱 실패 시 보존
                cleaned_histories[project_key] = data
        
        # 정리된 데이터 저장
        if len(cleaned_histories) < len(all_histories):
            with open(CHAT_HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(cleaned_histories, f, ensure_ascii=False, indent=2)
            st.cache_data.clear()  # 캐시 클리어
            
    except Exception as e:
        print(f"히스토리 정리 실패: {e}")