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
import time

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

def stream_response_typing(sentences: List[str], placeholder, delay_between_sentences=0.8, char_delay=0.03):
    """ChatGPT 스타일 스트리밍 타이핑 애니메이션"""
    if not sentences:
        return
    
    displayed_text = ""
    
    for sentence in sentences:
        # 문장 단위로 타이핑
        sentence_text = ""
        for char in sentence:
            sentence_text += char
            current_display = displayed_text + sentence_text + "▊"
            placeholder.markdown(current_display)
            time.sleep(char_delay)
        
        # 완성된 문장을 전체 텍스트에 추가
        displayed_text += sentence + " "
        
        # 문장 간 딜레이
        if sentence != sentences[-1]:  # 마지막 문장이 아니면
            placeholder.markdown(displayed_text + "▊")
            time.sleep(delay_between_sentences)
    
    # 최종 텍스트 출력 (커서 제거)
    placeholder.markdown(displayed_text.strip())

def quick_stream_response(text: str, placeholder, chunk_size=15, delay=0.5):
    """빠른 청크 단위 스트리밍 (긴 답변용)"""
    words = text.split()
    chunks = []
    
    # 단어를 청크로 나누기
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)
    
    displayed_text = ""
    for chunk in chunks:
        displayed_text += chunk + " "
        placeholder.markdown(displayed_text + "▊")
        time.sleep(delay)
    
    # 최종 출력
    placeholder.markdown(displayed_text.strip())

def handle_streaming_response(result: Dict, placeholder, use_quick_mode=False):
    """스트리밍 응답 처리 - 사용자 설정 완전 반영 버전"""
    import streamlit as st
    import time
    
    try:
        # 사용자 설정 가져오기 (기본값 포함)
        settings = st.session_state.get("animation_settings", {})
        char_delay = settings.get("char_delay", 0.03)
        sentence_delay = settings.get("sentence_delay", 0.8)
        enabled = settings.get("enabled", True)
        quick_threshold = settings.get("quick_mode_threshold", 2000)
        debug_mode = st.session_state.get("debug_mode", False)
        
        # 디버그 정보 출력
        if debug_mode:
            st.caption(f"🔧 디버그: 애니메이션={'ON' if enabled else 'OFF'}, 속도={char_delay}s, 문장딜레이={sentence_delay}s")
        
        # 애니메이션이 비활성화된 경우 즉시 출력
        if not enabled:
            placeholder.markdown(result["answer"])
            if debug_mode:
                st.success("⚡ 즉시 출력 모드로 표시 완료")
            return
        
        # 답변 길이 체크
        answer_text = result.get("answer", "")
        answer_length = len(answer_text)
        
        # 빠른 모드 조건 체크
        force_quick_mode = use_quick_mode or answer_length > quick_threshold
        
        if debug_mode:
            st.caption(f"📏 답변 길이: {answer_length}자, 빠른모드: {'ON' if force_quick_mode else 'OFF'}")
        
        # 스트리밍 방식 선택 및 실행
        if "streaming_sentences" in result and result["streaming_sentences"] and not force_quick_mode:
            # 방식 1: 문장 단위 정밀 스트리밍 (일반 모드)
            _stream_response_typing_enhanced(
                result["streaming_sentences"], 
                placeholder,
                char_delay=char_delay,
                sentence_delay=sentence_delay,
                debug_mode=debug_mode
            )
        else:
            # 방식 2: 빠른 청크 스트리밍 (긴 답변 또는 fallback)
            chunk_size = _calculate_optimal_chunk_size(answer_length)
            chunk_delay = max(0.1, char_delay * 20)  # 청크 딜레이는 문자 딜레이의 20배
            
            _quick_stream_response_enhanced(
                answer_text, 
                placeholder, 
                chunk_size=chunk_size,
                delay=chunk_delay,
                debug_mode=debug_mode
            )
            
    except Exception as e:
        # 오류 시 즉시 출력
        placeholder.markdown(result.get("answer", "답변을 표시할 수 없습니다."))
        if debug_mode:
            st.error(f"🚨 스트리밍 애니메이션 오류: {e}")
        else:
            print(f"스트리밍 애니메이션 오류: {e}")

def _stream_response_typing_enhanced(sentences: List[str], placeholder, char_delay=0.03, sentence_delay=0.8, debug_mode=False):
    """향상된 문장 단위 타이핑 애니메이션"""
    if not sentences:
        return
    
    displayed_text = ""
    total_sentences = len(sentences)
    
    if debug_mode:
        st.caption(f"🎬 문장별 타이핑 시작: {total_sentences}개 문장")
    
    for sentence_idx, sentence in enumerate(sentences):
        if not sentence.strip():  # 빈 문장 스킵
            continue
            
        # 문장별 타이핑
        sentence_text = ""
        sentence = sentence.strip()
        
        # 문장 시작 시 약간의 딜레이 (첫 문장 제외)
        if sentence_idx > 0:
            time.sleep(sentence_delay)
        
        # 문자별 타이핑
        for char_idx, char in enumerate(sentence):
            sentence_text += char
            
            # 현재 표시 텍스트 생성 (커서 포함)
            current_display = displayed_text + sentence_text + "▊"
            placeholder.markdown(current_display)
            
            # 문자 간 딜레이
            time.sleep(char_delay)
        
        # 완성된 문장을 전체 텍스트에 추가
        displayed_text += sentence
        
        # 문장 끝에 공백이나 줄바꿈이 없으면 추가
        if sentence_idx < total_sentences - 1:
            if not displayed_text.endswith((' ', '\n')):
                displayed_text += " "
        
        # 진행상황 표시 (디버그 모드)
        if debug_mode and sentence_idx % 3 == 0:  # 3문장마다 표시
            progress = (sentence_idx + 1) / total_sentences
            st.caption(f"📝 진행률: {progress:.1%} ({sentence_idx + 1}/{total_sentences})")
    
    # 최종 텍스트 출력 (커서 제거)
    placeholder.markdown(displayed_text.strip())
    
    if debug_mode:
        st.success(f"✅ 문장별 타이핑 완료: {len(displayed_text)}자 출력")

def _quick_stream_response_enhanced(text: str, placeholder, chunk_size=15, delay=0.3, debug_mode=False):
    """향상된 빠른 청크 단위 스트리밍"""
    words = text.split()
    chunks = []
    
    # 단어를 청크로 나누기
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)
    
    if debug_mode:
        st.caption(f"🚀 빠른 모드 시작: {len(chunks)}개 청크, 청크당 {chunk_size}단어")
    
    displayed_text = ""
    total_chunks = len(chunks)
    
    for chunk_idx, chunk in enumerate(chunks):
        displayed_text += chunk + " "
        
        # 커서와 함께 표시
        current_display = displayed_text + "▊"
        placeholder.markdown(current_display)
        
        # 청크 간 딜레이
        time.sleep(delay)
        
        # 진행상황 표시 (디버그 모드, 20% 간격)
        if debug_mode and chunk_idx % max(1, total_chunks // 5) == 0:
            progress = (chunk_idx + 1) / total_chunks
            st.caption(f"🏃 빠른모드 진행률: {progress:.1%}")
    
    # 최종 출력
    placeholder.markdown(displayed_text.strip())
    
    if debug_mode:
        st.success(f"⚡ 빠른 모드 완료: {len(chunks)}개 청크 출력")

def _calculate_optimal_chunk_size(text_length: int) -> int:
    """텍스트 길이에 따른 최적 청크 크기 계산"""
    if text_length < 500:
        return 8      # 짧은 텍스트: 작은 청크
    elif text_length < 1500:
        return 15     # 중간 텍스트: 보통 청크
    elif text_length < 3000:
        return 25     # 긴 텍스트: 큰 청크
    else:
        return 35     # 매우 긴 텍스트: 매우 큰 청크

def stream_response_typing(sentences: List[str], placeholder, delay_between_sentences=0.8, char_delay=0.03):
    """기본 문장 단위 타이핑 애니메이션 (하위 호환성 유지)"""
    return _stream_response_typing_enhanced(
        sentences, 
        placeholder, 
        char_delay=char_delay, 
        sentence_delay=delay_between_sentences,
        debug_mode=False
    )

def quick_stream_response(text: str, placeholder, chunk_size=15, delay=0.5):
    """기본 빠른 청크 스트리밍 (하위 호환성 유지)"""
    return _quick_stream_response_enhanced(
        text, 
        placeholder, 
        chunk_size=chunk_size, 
        delay=delay,
        debug_mode=False
    )

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