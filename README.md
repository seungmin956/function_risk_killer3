<div align="center">
<a href="https://functionriskkiller-5wmwxsr87emk8z5zh5nee3.streamlit.app/">
<img src="riskkiller.png" alt="Risk Killer Demo" style="border-radius: 10px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);" width="85%">
</a>
<br><br>
</div>
<h2> K-푸드 미국 수출을 위한 FDA 규제 대응 AI 플랫폼</h2>
<p><strong>팀 프로젝트</strong> | <strong>🏆2025 K-디지털 트레이닝 해커톤 예선 합격 및 본선 진행중</strong><br></p>
<p>중소 식품 기업들이 전문 인력 비용 부담을 줄이고 효율적으로 미국 시장에 진입할 수 있는 </p>
<p>맞춤형 LLM 챗봇을 구현하기 위해 진행한 프로젝트입니다.</p>

-- [실시간 앱 실행 (Streamlit)](https://functionriskkiller-5wmwxsr87emk8z5zh5nee3.streamlit.app/)
-- [상세보고서 (PDF)](Risk_Killer.pdf)

---

## 주요 기능
<table>
<tr>
<td width="33%" align="center">
<h4>📢 시장 동향</h4>
<p>Tableau 연동<br>미국 식품 시장 소비 패턴<br>주별 소비 규모 시각화</p>
</td>
<td width="33%" align="center">
<h4>🤖 규제 분석 AI 챗봇</h4>
<p>제품 정보 입력 시<br>관련 FDA 규제 자동 분석<br>핵심 조항 + 해설 + 출처 제공</p>
</td>
<td width="33%" align="center">
<h4>🔍 리콜 사례 검색</h4>
<p>과거 리콜 사례 기반<br>위험 요소 사전 경고<br>기업명 + 제품명 + 사유 분석</p>
</td>
</tr>
</table>

---

## 개발 과정에서 해결한 주요 문제들
<table>
<thead>
<tr>
<th width="15%">문제 영역</th>
<th width="35%">발생한 문제</th>
<th width="35%">해결 방법</th>
<th width="15%">핵심 기술</th>
</tr>
</thead>
<tbody>
<tr>
<td align="center"><strong> 데이터 수집</strong></td>
<td>
• FDA 사이트 크롤링 시 지속적 차단<br>
• Selenium, BeautifulSoup 모두 실패<br>
• User-Agent 변경으로도 해결 불가
</td>
<td>
• <strong>Microsoft Playwright 도입</strong><br>
• 웹 테스트 도구 특성으로 탐지 회피<br>
• 실제 브라우저 환경 시뮬레이션<br>
• JavaScript 동적 콘텐츠 안정적 수집
</td>
<td align="center">Playwright<br>동적 크롤링</td>
</tr>
<tr>
<td align="center"><strong> 성능 평가</strong></td>
<td>
• 일반 챗봇 평가 도구 부적합<br>
• FDA 규제 전문성 측정 불가<br>
• 도메인 특화 지표 부재
</td>
<td>
• <strong>커스텀 평가 데이터셋 구축</strong><br>
• FSVP, Nutritional Labeling 전문 질문<br>
• 어려운 질문 리스트 반복 테스트<br>
• 점진적 성능 개선
</td>
<td align="center">Custom<br>Evaluation</td>
</tr>
<tr>
<td align="center"><strong> 벡터 DB 한계</strong></td>
<td>
• ChromaDB의 수치/논리 질문 취약<br>
• 텍스트 유사성에만 특화<br>
• 정확한 수치 답변 성능 저하
</td>
<td>
• <strong>하이브리드 검색 시스템</strong><br>
• ChromaDB + SQLite 결합<br>
• 수치형 → SQLite, 텍스트 → ChromaDB<br>
• 쿼리 타입별 자동 라우팅
</td>
<td align="center">ChromaDB<br>+ SQLite</td>
</tr>
<tr>
<td align="center"><strong> 프롬프트 한계</strong></td>
<td>
• 단일 프롬프트 성능 천장<br>
• 복잡한 규제 해석 한계<br>
• 개선 효과 정체
</td>
<td>
• <strong>Multi-Agent 패러다임</strong><br>
• 규제분석 → 리콜검색 → 전략도출<br>
• 각 Agent별 전문 영역 분업<br>
• 협업을 통한 정확도 향상
</td>
<td align="center">LangGraph<br>Multi-Agent</td>
</tr>
<tr>
<td align="center"><strong> Fine-tuning</strong></td>
<td>
• FDA 문서 Fine-tuning 시도<br>
• 과적합으로 성능 악화<br>
• 일반화 능력 저하
</td>
<td>
• <strong>RAG 최적화에 집중</strong><br>
• 청킹: 512토큰 → 256토큰<br>
• 임베딩 모델 비교 선택<br>
• 20% 오버래핑 기법 적용
</td>
<td align="center">RAG<br>Optimization</td>
</tr>
<tr>
<td align="center"><strong> 디버깅</strong></td>
<td>
• 15,000개 문서 중 참조 추적 불가<br>
• 답변 근거 확인 어려움<br>
• 성능 병목 지점 파악 불가
</td>
<td>
• <strong>LangSmith 모니터링</strong><br>
• 실제 검색 문서 리스트 추적<br>
• 단계별 처리 시간 분석<br>
• 비용 및 토큰 사용량 모니터링
</td>
<td align="center">LangSmith<br>Monitoring</td>
</tr>
<tr>
<td align="center"><strong> 모델 선택</strong></td>
<td>
• 지속 업데이트되는 LLM들<br>
• 최적 모델 선택 기준 모호<br>
• 성능 vs 비용 트레이드오프
</td>
<td>
• <strong>체계적 모델 비교</strong><br>
• GPT-4o-mini, Claude-3.5, Gemini-Pro, Grok<br>
• 가성비와 성능 균형점 고려<br>
• GPT-4o-mini 최종 선택
</td>
<td align="center">Model<br>Comparison</td>
</tr>
<tr>
<td align="center"><strong> 인프라 제약</strong></td>
<td>
• 개인 프로젝트 GPU 사용 어려움<br>
• 로컬 모델 실행 불가<br>
• API 의존성 및 비용 부담
</td>
<td>
• <strong>단계적 접근 전략</strong><br>
• 현재: OpenAI API 프로토타입<br>
• 향후: OLLAMA + Llama 3.1<br>
• 로컬 배포로 차별화 계획
</td>
<td align="center">API → Local<br>Migration</td>
</tr>
</tbody>
</table>
