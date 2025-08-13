# utils/chat_regulation.py

import json
import os
from functools import wraps
from dotenv import load_dotenv
from typing import TypedDict, List, Dict, Any 
from langchain_openai import OpenAIEmbeddings, ChatOpenAI 
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate 
from langchain_core.messages import AIMessage, HumanMessage
from langchain_community.chat_message_histories import ChatMessageHistory
from langgraph.graph import StateGraph, START, END
from langchain_teddynote import logging   # LangSmith 추적 활성화

load_dotenv()                   # 환경변수 로드
logging.langsmith("LLMPROJECT") # LangSmith 추적 설정

# 계층적 구조를 위한 카테고리 그룹핑
CATEGORY_HIERARCHY = {
    "guidance": {
        "allergen": ["알러지", "allergen", "알레르기", "알러겐", "과민반응"],
        "additives": ["첨가물", "additive", "식품첨가물", "방부제", "감미료", "향료", "착색료"],
        "labeling": ["라벨링", "labeling", "라벨", "표시", "영양성분", "원재료", "성분표시"],
        "main": ["가이드라인", "guidance", "cpg", "가이드", "일반", "식품관련", "food"]
    },
    "regulation": {
        "ecfr": ["ecfr", "연방규정집", "전자연방규정", "cfr"],
        "usc": ["21usc", "법률", "조항", "규정", "regulation", "법령"]
    }
}

# 한국어-영어 번역 함수
def translate_korean_to_english(korean_text: str) -> str:
    """한국어 텍스트를 영어로 번역"""
    try:
        llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)
        prompt = f"Translate the following Korean text to English. Only return the translation without any explanation:\n\n{korean_text}"
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content.strip()
    except Exception as e:
        print(f"번역 중 오류 발생: {e}")
        return korean_text

# ChromaDB 컬렉션 초기화
def initialize_chromadb_collection():
    """기존 ChromaDB chroma_regulations 컬렉션에 연결"""
    try:
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        
        # 기존 ChromaDB 컬렉션에 연결
        vectorstore = Chroma(
            collection_name="chroma_regulations",  # 사용자가 지정한 컬렉션명
            embedding_function=embeddings,
            persist_directory="./data/chroma_db"
        )
        
        # 컬렉션이 존재하고 데이터가 있는지 확인
        collection = vectorstore._collection
        document_count = collection.count()
        
        if document_count > 0:
            print(f"ChromaDB 컬렉션 'chroma_regulations' 연결 완료 ({document_count}개 문서)")
            return vectorstore
        else:
            raise ValueError("ChromaDB 컬렉션이 비어있습니다. 데이터를 먼저 로드해주세요.")
            
    except Exception as e:
        print(f"ChromaDB 컬렉션 초기화 중 오류: {e}")
        raise

# 전역 변수로 벡터스토어 초기화
vectorstore = initialize_chromadb_collection()

# 상태 정의
class GraphState(TypedDict):
    question: str
    question_en: str
    document_type: str
    categories: List[str]
    chat_history: List[HumanMessage | AIMessage]
    context: str
    urls: List[str]
    answer: str
    need_synthesis: bool
    guidance_references: List[str]  # guidance에서 regulation 참조를 위한 필드

# 노드 정의
def router_node(state: GraphState) -> GraphState:
    """초기 라우팅: guidance vs regulation 결정 + 번역"""
    question = state["question"].lower()
    
    # 한국어 질문을 영어로 번역
    try:
        question_en = translate_korean_to_english(state["question"])
        print(f"번역된 질문: {question_en}")
    except Exception as e:
        print(f"번역 실패: {e}")
        question_en = state["question"]
    
    # regulation 키워드 체크
    regulation_keywords = ["법률","규제", "21usc", "규정", "regulation", "법령", "조항", "cfr", "code of federal"]
    guidance_keywords = ["가이드", "guidance", "cpg", "지침", "guideline"]
    
    combined_text = question + " " + question_en.lower()
    
    regulation_score = sum(1 for keyword in regulation_keywords if keyword in combined_text)
    guidance_score = sum(1 for keyword in guidance_keywords if keyword in combined_text)
    
    # 기본적으로 guidance 우선
    document_type = "regulation" if regulation_score > guidance_score else "guidance"
    
    return {
        **state,
        "question_en": question_en,
        "document_type": document_type,
        "guidance_references": []
    }

def category_node(state: GraphState) -> GraphState:
    """카테고리별 세부 분류 - 복합 질문 처리"""
    question = state["question"].lower()
    question_en = state["question_en"].lower()
    doc_type = state["document_type"]
    
    # 키워드 점수 계산
    category_scores = {}
    category_keywords = CATEGORY_HIERARCHY[doc_type]
    
    # 영어 키워드 매핑 확장
    english_keywords = {
        "allergen": ["allergen", "allergy", "allergenic", "hypersensitivity", "allergic reaction"],
        "additives": ["additive", "preservatives", "sweetener", "flavoring", "coloring", "food additive"],
        "labeling": ["labeling", "label", "nutrition", "ingredient", "declaration", "nutritional facts"],
        "main": ["guidance", "general", "main", "comprehensive", "cpg", "food related"],
        "ecfr": ["electronic code", "federal regulations", "cfr", "code of federal regulations"],
        "usc": ["united states code", "federal law", "statute", "21 usc", "federal statute"]
    }
    
    # 각 카테고리별 점수 계산
    for category, korean_keywords in category_keywords.items():
        score = 0
        
        # 한국어 키워드 매칭
        for keyword in korean_keywords:
            if keyword.lower() in question:
                score += 2
        
        # 영어 키워드 매칭
        for keyword in english_keywords.get(category, []):
            if keyword in question_en:
                score += 1.5
        
        category_scores[category] = score
    
    # 복합 질문 처리
    selected_categories = []
    
    # 특별 패턴 감지
    import re
    combined_text = question + " " + question_en.lower()
    
    complex_patterns = [
        (r'알러지.*규제|allergen.*regulation', 'allergen', 'guidance'),
        (r'첨가물.*규제|additive.*regulation', 'additives', 'guidance'), 
        (r'라벨링.*규제|labeling.*regulation', 'labeling', 'guidance'),
    ]
    
    pattern_matched = False
    for pattern, target_category, target_doc_type in complex_patterns:
        if re.search(pattern, combined_text, re.IGNORECASE):
            selected_categories = [target_category]
            state["document_type"] = target_doc_type
            pattern_matched = True
            print(f"복합 질문 감지: '{target_category}' 카테고리, '{target_doc_type}' 문서타입으로 변경")
            break
    
    if not pattern_matched:
        # 일반 로직: 가장 높은 점수를 가진 카테고리들 선택
        if category_scores:
            max_score = max(category_scores.values())
            if max_score > 0:
                threshold = max_score * 0.7
                selected_categories = [cat for cat, score in category_scores.items() 
                                     if score >= threshold]
    
    # 기본값 설정
    if not selected_categories:
        selected_categories = ["main"] if state["document_type"] == "guidance" else ["usc", "ecfr"]
    
    # 여러 카테고리가 선택되면 종합이 필요
    need_synthesis = len(selected_categories) > 1
    
    print(f"선택된 카테고리: {selected_categories}, 문서타입: {state['document_type']}, 점수: {category_scores}")
    
    return {
        **state,
        "categories": selected_categories,
        "need_synthesis": need_synthesis
    }

def document_retrieval_node(state: GraphState) -> GraphState:
    all_documents = []; guidance_references = []; search_query = state["question_en"]
    for category in state["categories"]:
        try:
            filter_dict = {"$and": [{"document_type": {"$eq": state["document_type"]}}, {"category": {"$eq": category.lower()}}]}
            docs = vectorstore.as_retriever(search_kwargs={"k": 3, "filter": filter_dict}).invoke(search_query)
            if docs: all_documents.extend(docs)
        except Exception: continue
    if not all_documents: all_documents = vectorstore.as_retriever(search_kwargs={"k": 5}).invoke(search_query)
        
    unique_docs = list({doc.page_content[:100]: doc for doc in all_documents}.values())
    selected_docs = unique_docs[:5]
    
    unique_urls = sorted(list(set([doc.metadata.get("url", "") for doc in selected_docs if doc.metadata.get("url")])))
    url_to_number_map = {url: i + 1 for i, url in enumerate(unique_urls)}

    context_parts = []
    for doc in selected_docs:
        source_url = doc.metadata.get("url")
        if source_url and source_url in url_to_number_map:
            cite_num = url_to_number_map[source_url]
            context_part = f"[출처 {cite_num}]: {doc.page_content}"
            context_parts.append(context_part)
        
    context = "\n\n---\n\n".join(context_parts)
    return { **state, "context": context, "urls": unique_urls, "guidance_references": [] }


def synthesis_node(state: GraphState) -> GraphState:
    """guidance → regulation 단방향 참조를 통한 답변 품질 향상"""
    additional_context = ""
    additional_urls = []
    
    # guidance 문서에서 regulation 참조가 있는 경우에만 실행
    if state["document_type"] == "guidance" and state["guidance_references"]:
        try:
            print(f"regulation 참조 검색 시작: {state['guidance_references']}")
            
            # 참조된 regulation 섹션들을 검색
            for reference in state["guidance_references"]:
                reference = reference.strip()
                if not reference:
                    continue
                
                # CFR 참조인지 USC 참조인지 판단
                ref_lower = reference.lower()
                if "cfr" in ref_lower or "21 cfr" in ref_lower:
                    target_category = "ecfr"
                elif "usc" in ref_lower or "21 u.s.c" in ref_lower:
                    target_category = "usc"
                else:
                    # 기본적으로 둘 다 검색
                    target_category = None
                
                # regulation 문서에서 해당 참조 검색
                try:
                    if target_category:
                        # 특정 카테고리로 검색
                        reg_filter = {
                            "$and": [
                                {"document_type": {"$eq": "regulation"}},
                                {"category": {"$eq": target_category}}
                            ]
                        }
                    else:
                        # regulation 문서 전체에서 검색
                        reg_filter = {"document_type": {"$eq": "regulation"}}
                    
                    reg_retriever = vectorstore.as_retriever(
                        search_kwargs={"k": 2, "filter": reg_filter}
                    )
                    
                    # 참조 번호를 검색 쿼리로 사용
                    reg_docs = reg_retriever.invoke(reference)
                    
                    if reg_docs:
                        ref_context = f"\n\n[{reference} 관련 규정]\n"
                        ref_context += "\n".join([doc.page_content[:500] + "..." for doc in reg_docs])
                        additional_context += ref_context
                        
                        ref_urls = [doc.metadata.get("url", "") for doc in reg_docs if doc.metadata.get("url")]
                        additional_urls.extend(ref_urls)
                        
                        print(f"참조 '{reference}'에서 {len(reg_docs)}개 regulation 문서 발견")
                    
                except Exception as e:
                    print(f"참조 '{reference}' 검색 중 오류: {e}")
                    continue
            
            # 일반적인 관련 regulation 검색 (참조가 구체적이지 않은 경우)
            if not additional_context:
                try:
                    search_query = state["question_en"]
                    reg_filter = {"document_type": {"$eq": "regulation"}}
                    reg_retriever = vectorstore.as_retriever(
                        search_kwargs={"k": 2, "filter": reg_filter}
                    )
                    reg_docs = reg_retriever.invoke(search_query)
                    
                    if reg_docs:
                        additional_context = "\n\n[관련 규정 참조]\n"
                        additional_context += "\n".join([doc.page_content[:500] + "..." for doc in reg_docs])
                        additional_urls = [doc.metadata.get("url", "") for doc in reg_docs if doc.metadata.get("url")]
                        print(f"일반 regulation 검색에서 {len(reg_docs)}개 문서 발견")
                
                except Exception as e:
                    print(f"일반 regulation 검색 중 오류: {e}")
        
        except Exception as e:
            print(f"guidance → regulation 참조 검색 중 전체 오류: {e}")
    
    # 종합이 필요한 경우 (여러 카테고리)
    elif state["need_synthesis"]:
        try:
            search_query = state["question_en"]
            cross_filter = {"document_type": {"$eq": state["document_type"]}}
            cross_retriever = vectorstore.as_retriever(
                search_kwargs={"k": 2, "filter": cross_filter}
            )
            cross_docs = cross_retriever.invoke(search_query)
            
            if cross_docs:
                additional_context = "\n\n[추가 관련 정보]\n"
                additional_context += "\n".join([doc.page_content[:500] + "..." for doc in cross_docs])
                additional_urls = [doc.metadata.get("url", "") for doc in cross_docs if doc.metadata.get("url")]
        
        except Exception as e:
            print(f"종합 검색 중 오류: {e}")
    
    # 추가 컨텍스트와 URL 병합
    if additional_context:
        updated_context = state["context"] + additional_context
        updated_urls = state["urls"] + additional_urls
        
        return {
            **state,
            "context": updated_context,
            "urls": updated_urls
        }
    
    return state

def extract_domain_name(url: str) -> str:
    """URL에서 읽기 쉬운 도메인명을 추출합니다."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '')
        
        if 'fda.gov' in domain:
            return 'FDA 공식 사이트'
        elif 'ecfr.gov' in domain:
            return 'eCFR 전자연방규정집'
        elif 'cornell.edu' in domain:
            return 'Cornell Law School'
        else:
            return domain.capitalize()
    except:
        return "관련 웹사이트"

def generate_answer(state: GraphState) -> GraphState:
    """Perplexity 스타일 주석을 생성하고, Python으로 최종 출처 목록을 포맷하는 답변 생성기"""
    
    source_list_str = "\n".join([f"[{i+1}] {url}" for i, url in enumerate(state["urls"])])
    
    # ▼▼▼▼▼ 1. 프롬프트 수정: AI에게 출처 목록 생성 지시를 삭제 ▼▼▼▼▼
    prompt = PromptTemplate.from_template(
        """당신은 미국 FDA 규제를 전문적으로 해석하는 규제 자문 전문가입니다.
아래 사용자의 질문에 대해 주어진 컨텍스트를 바탕으로 한국어로 정밀하고 신뢰성 있는 해석을 제공하세요.

❗️핵심 규칙:
- 반드시 규제 문서 내용을 기반으로 판단하고, 문서 내용을 최대한 많이 싣어주세요.
- **각 항목을 설명할 때, 그 근거가 되는 규정의 핵심 내용을 상세하게 풀어 설명해주세요.** (최소 2-3문장 이상)
- 중요한 정보나 규정을 언급할 때마다 해당하는 출처 번호를 [1], [2] 형태로 문장 끝에 삽입하세요.
- 출처가 포함된 조항은 인용 표시(예: 21 CFR 182.1)로 명시하세요.
- 중요 내용은 번호 목록 형식으로 명확히 정리하세요.
- 마지막에는 위의 항목들을 요약하여 정리한 종합적 분석 문단을 추가하세요.

📝 사용자 질문:
{question}
📖 문서 컨텍스트 (각 내용 앞의 [출처 N]을 보고 주석을 달아야 함):
{context}
📎 사용 가능한 출처 목록 (참고용):
{source_info}
🔽 위의 정보를 바탕으로 상세하고 전문적인 답변을 작성해주세요:"""
    )
    
    try:
        llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0.1)
        chain = prompt | llm | StrOutputParser()
        
        # AI는 본문과 인라인 주석까지만 생성
        answer_text = chain.invoke({
            "question": state["question"],
            "context": state["context"],
            "source_info": source_list_str
        })
        
        # Python 코드가 인라인 주석을 하이퍼링크로 변환
        final_answer_with_links = answer_text
        for i, url in enumerate(state["urls"]):
            final_answer_with_links = final_answer_with_links.replace(f"[{i+1}]", f" [[{i+1}]]({url})")

        # ▼▼▼▼▼ 2. Python 코드 수정: 이상적인 형태로 출처 목록을 직접 생성하여 추가 ▼▼▼▼▼
        if state["urls"]:
            # extract_domain_name 함수를 사용하여 이상적인 포맷의 출처 목록을 생성
            url_text = "\n\n📎 출처:\n"
            for i, url in enumerate(state["urls"]):
                domain = extract_domain_name(url) # 이 함수는 generate_answer 함수 밖에 정의되어 있어야 합니다.
                url_text += f"[{i+1}] [{domain}]({url})\n"
            
            # 최종적으로 AI 답변과 Python이 만든 출처 목록을 결합
            full_answer = f"{final_answer_with_links}{url_text}"
        else:
            full_answer = final_answer_with_links
        
        return { **state, "answer": full_answer }

    except Exception as e:
        return { **state, "answer": f"답변 생성 중 오류가 발생했습니다: {e}" }

def update_chat_history(state: GraphState) -> GraphState:
    """채팅 히스토리 업데이트"""
    try:
        current_history = state.get("chat_history", [])
        
        # 새 메시지 추가
        updated_history = current_history.copy()
        updated_history.append(HumanMessage(content=state["question"]))
        updated_history.append(AIMessage(content=state["answer"]))
        
        # 히스토리 길이 제한 (최대 10개 메시지)
        if len(updated_history) > 10:
            updated_history = updated_history[-10:]
        
        return {
            **state,
            "chat_history": updated_history
        }
    
    except Exception as e:
        print(f"채팅 히스토리 업데이트 중 오류: {e}")
        return state

# 그래프 구성
workflow = StateGraph(GraphState)

# 노드 추가
workflow.add_node("router", router_node)
workflow.add_node("category", category_node) 
workflow.add_node("retrieval", document_retrieval_node)
workflow.add_node("synthesis", synthesis_node)
workflow.add_node("generate", generate_answer)
workflow.add_node("update_history", update_chat_history)

# 엣지 추가
workflow.add_edge(START, "router")
workflow.add_edge("router", "category")
workflow.add_edge("category", "retrieval")
workflow.add_edge("retrieval", "synthesis")
workflow.add_edge("synthesis", "generate")
workflow.add_edge("generate", "update_history")
workflow.add_edge("update_history", END)

# 그래프 컴파일
graph = workflow.compile()

# 메인 실행 함수
def ask_question(question: str, chat_history: List = None) -> Dict[str, Any]:
    """질문 처리 메인 함수"""
    if chat_history is None:
        chat_history = []
    
    try:
        result = graph.invoke({
            "question": question,
            "question_en": "",
            "chat_history": chat_history,
            "document_type": "",
            "categories": [],
            "context": "",
            "urls": [],
            "answer": "",
            "need_synthesis": False,
            "guidance_references": []
        })
        
        return {
            "answer": result["answer"],
            "document_type": result["document_type"],
            "categories": result["categories"],
            "urls": result["urls"],
            "chat_history": result["chat_history"],
            "guidance_references": result["guidance_references"]
        }
    
    except Exception as e:
        return {
            "answer": f"처리 중 오류가 발생했습니다: {e}",
            "document_type": "",
            "categories": [],
            "urls": [],
            "chat_history": chat_history,
            "guidance_references": []
        }