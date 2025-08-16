# utils/agent_recall.py
from typing import List, Dict, Any, Optional
import re
from datetime import datetime
from langchain_core.messages import HumanMessage, AIMessage
from utils.function_calling_system import FunctionCallRecallSystem

class RecallAgent:
    """
    컨트롤러:
    - 질문을 간단히 분류해 상황별 '[힌트]'를 동적으로 생성
    - Function Calling 시스템에 힌트를 포함해 전달
    - tab_recall.py가 기대하는 구조로 결과 반환
    """

    def __init__(self, add_hint: bool = True):
        self.fc = FunctionCallRecallSystem()
        self.add_hint = add_hint

    # -------------------- Hint logic --------------------
    def _make_hint(self, q: str) -> str:
        q_raw = q or ""
        q_l = q_raw.lower()

        # 숫자 N (상위 N, 최근 N개월 등)
        n_match = re.search(r'(?:상위\s*(\d+)|top\s*(\d+)|최근\s*(\d+)\s*개월|(\d+)\s*개)', q_raw, re.IGNORECASE)
        n_val = next((int(g) for g in (n_match.groups() if n_match else []) if g and g.isdigit()), None)

        # 연도/비교 신호
        years = re.findall(r'(20\d{2})', q_raw)
        has_last = any(k in q_raw for k in ["작년", "지난해", "전년"])
        has_this = any(k in q_raw for k in ["올해", "금년"])
        has_both_relative = has_last and has_this
        explicit_compare = any(k in q_raw for k in ["비교", "대비", "vs", "전년 대비", "전년대비"])
        two_years = len(set(years)) >= 2

        # 제외 신호
        exclude_triggers = ["제외", "빼고", "빼줘", "빼서", "제외해", "제외한", "without", "except"]
        is_exclude = any(k in q_raw for k in exclude_triggers)

        # 간단한 제외 대상 추출 (옵션)
        exclude_terms = []
        m = re.search(r'([\w가-힣\s,/]+?)\s*(?:는|은)?\s*(?:제외|빼고|without|except)', q_raw)
        if m:
            cand = m.group(1).strip()
            for t in re.split(r'[,\s/]+', cand):
                t = t.strip()
                if t and t not in exclude_terms and len(t) <= 20:
                    exclude_terms.append(t)

        # 트렌드/랭킹/카운트/사례 신호
        is_trend = (any(k in q_raw for k in ["월별", "월간", "추이", "트렌드", "흐름", "동향", "패턴"])
                    or ("최근" in q_raw and "개월" in q_raw))
        is_rank = any(k in q_raw for k in ["상위", "순위", "랭킹", "가장 많은", "top", "최다", "베스트"])
        is_count = any(k in q_raw for k in ["몇 건", "건수", "총 몇", "총건수", "how many", "count"])
        is_cases = any(k in q_raw for k in ["사례", "목록", "리스트", "보여줘", "어떤 제품", "무엇이었", "case", "examples", "제품들"])
        is_riskish = any(k in q_raw for k in ["위험", "치명", "중대", "serious", "class i", "injury", "death"])

        # ===== 분기 순서 =====
        # 1) 제외
        if is_exclude:
            if exclude_terms:
                return (f"[힌트] 가능하면 filter_exclude_conditions(include_terms=[...], "
                        f"exclude_terms={exclude_terms}, limit=10) 함수를 사용해 제외 조건을 적용하고, 표로 핵심 사례를 보여줘.")
            return "[힌트] 가능하면 filter_exclude_conditions(include_terms=[...], exclude_terms=[...], limit=10) 함수를 사용해 제외 조건을 적용하고, 표로 핵심 사례를 보여줘."

        # 2) **사례(우선)** — '사례'나 '위험' 맥락이면 검색으로 유도
        if is_cases or is_riskish:
            # 상대연도 → 구체 연도 토큰으로 힌트에 넣어 검색 품질 개선
            year_hint = None
            if has_this:
                year_hint = str(datetime.now().year)
            elif has_last:
                year_hint = str(datetime.now().year - 1)
            elif "재작년" in q_raw:
                year_hint = str(datetime.now().year - 2)
            elif years:
                year_hint = years[0]
            if year_hint:
                return (f'[힌트] 가능하면 search_recall_cases(query="{year_hint} 위험 리콜 사례" 또는 유사어) 를 사용해 '
                        "관련 사례를 표(날짜/브랜드/제품/사유/출처)로 정리해. 결과가 부족하면 count_recalls로 보조 집계해.")
            return "[힌트] 가능하면 search_recall_cases를 사용해 관련 사례를 찾고, 표(날짜/브랜드/제품/사유/출처)로 정리해."

        # 3) 비교 — **실제로 비교 신호가 있을 때만**
        if (has_both_relative or two_years or explicit_compare) and (explicit_compare or has_both_relative or two_years):
            if has_both_relative:
                return '[힌트] 가능하면 compare_periods("작년","올해", include_reasons=True) 함수를 사용해 변화율(±%)과 상위 원인을 함께 비교해.'
            if two_years:
                return f'[힌트] 가능하면 compare_periods("{years[0]}","{years[1]}", include_reasons=True) 함수를 사용해 변화율(±%)을 명시해.'
            return '[힌트] 가능하면 compare_periods("작년","올해", include_reasons=True) 함수를 사용해 비교해.'

        # 4) 월별 추이
        if is_trend:
            months = min(max(n_val or 12, 3), 24)
            return f"[힌트] 가능하면 get_monthly_trend(months={months}) 함수를 사용해 월별 추이를 표 또는 목록으로 요약해."

        # 5) 순위
        if is_rank:
            if any(k in q_raw for k in ["원인", "사유", "reason"]): field = "recall_reason_detail"
            elif any(k in q_raw for k in ["회사", "기업", "company"]): field = "company"
            elif any(k in q_raw for k in ["브랜드", "상표", "brand"]): field = "brand"
            elif any(k in q_raw for k in ["제품", "식품", "product"]): field = "product_type"
            else: field = "recall_reason"
            limit = min(max(n_val or 5, 3), 20)
            return f'[힌트] 가능하면 rank_by_field(field="{field}", limit={limit}) 함수를 사용해 상위 {limit}개를 표로 보여줘.'

        # 6) 건수
        if is_count:
            return "[힌트] 가능하면 count_recalls 함수를 사용해 건수를 정확히 집계해. 상대 기간 표현(작년/올해/재작년)은 원문 그대로 인자로 사용해."

        # 7) 기본
        return ("[힌트] 가능한 경우 count_recalls, rank_by_field, get_monthly_trend, compare_periods, "
                "search_recall_cases, filter_exclude_conditions 중 1~2개 함수를 적절히 선택해 근거 기반으로 답변해. "
                "상대 기간 표현(작년/올해/재작년)은 원문 그대로 인자로 전달해.")


    def _compose_query(self, query: str) -> str:
        if not self.add_hint:
            return query
        hint = self._make_hint(query)
        return f"{query}\n\n{hint}"

    # -------------------- Run --------------------
    def run(self, query: str, history: Optional[List] = None) -> Dict[str, Any]:
        history = history or []
        guided_query = self._compose_query(query)

        try:
            fc_result = self.fc.process_question(guided_query, history)
            answer = fc_result.get("answer") or "답변을 생성할 수 없습니다."
            tool_calls = fc_result.get("function_calls", [])

            return {
                "answer": answer,
                "processing_type": "agent",
                "function_calls": tool_calls,
                "has_realtime_data": bool(tool_calls),
                "realtime_count": len(tool_calls),
                "chat_history": history + [
                    HumanMessage(content=query),
                    AIMessage(content=answer),
                ],
            }
        except Exception as e:
            err = f"에이전트 처리 중 오류: {e}"
            return {
                "answer": err,
                "processing_type": "error",
                "function_calls": [],
                "has_realtime_data": False,
                "realtime_count": 0,
                "chat_history": history + [
                    HumanMessage(content=query),
                    AIMessage(content=err),
                ],
            }