# components/tableau.py

import streamlit as st
import pandas as pd
import streamlit.components.v1 as components

## tableau
def create_market_dashboard():
    """미국 시장 진출 대시보드 UI 생성"""
    # 설명 문구 추가
    st.info("""
    미국 식품 시장 동향을 시각화된 자료로 확인할 수 있습니다.\n
    모든 시각화 자료는 다운로드 기능을 제공합니다.""")
    
    # 2행: 두 개의 태블로 시각화 (미국 주별 식품 지출 시각화와 연도별 미국 식품 지출 추이)
    viz_col1, viz_col2= st.columns(2)
    
    with viz_col1:
        # 첫 번째 태블로 시각화: 미국 주별 식품 지출 시각화
        st.markdown("<h5 style='text-align: left;'># 미국 주별 식품 지출 시각화</h5>", unsafe_allow_html=True)
        
        components.html(
            """
            <!-- ▶︎▶︎ Tableau Public embed (state_food_exp2_17479635670940 / State) ◀︎◀︎ -->
            <div  id="viz_food_state"   class="tableauPlaceholder" style="position:relative; width:100%;">
            <noscript>
                <a href="#">
                <img style="border:none"
                    src="https://public.tableau.com/static/images/st/state_food_exp2_17479635670940/State/1.png"
                    alt="State">
                </a>
            </noscript>

            <object class="tableauViz" style="display:none;">
                <!-- Tableau Public 고정 파라미터 -->
                <param name="host_url" value="https%3A%2F%2Fpublic.tableau.com%2F" />
                <param name="embed_code_version" value="3" />
                <param name="site_root" value="" />

                <!-- ▼▼▼ **딱 한 줄! 대시보드 경로** ▼▼▼ -->
                <param name="name" value="state_food_exp2_17479635670940/State" />

                <!-- 기타 옵션 -->
                <param name="tabs"     value="no"   />
                <param name="toolbar"  value="bottom" />
                <param name="language" value="ko-KR" />
            </object>
            </div>

            <!-- Tableau Public 전용 로더 -->
            <script src="https://public.tableau.com/javascripts/api/viz_v1.js"></script>

            <!-- 반응형(가로폭 100 % : 높이 4 : 3) 설정 -->
            <script>
            const div   = document.getElementById("viz_food_state");
            const vizOB = div.getElementsByTagName("object")[0];

            function resizeViz(){
                vizOB.style.width  = "100%";
                vizOB.style.height = ( div.offsetWidth * 0.75 ) + "px"; // 0.75 = 4:3
            }
            resizeViz();
            window.addEventListener("resize", resizeViz);
            </script>
            """,
            height=420,        # Streamlit 쪽에서 확보할 최소 높이
            scrolling=False
        )
        
        #맨 아래 설명 적기
        st.caption("출처: [Statista Food](https://www.statista.com/outlook/cmo/food/united-states)")
    
    
    with viz_col2:
            st.markdown("<h5 style='text-align:left;'># 연도/리콜원인별 발생 건수 히트맵</h5>",
                        unsafe_allow_html=True)

            components.html(
                """
                <div class='tableauPlaceholder' id='viz_recall_trend' style='position:relative;width:100%;'>
                <noscript>
                    <a href='#'>
                    <img src='https://public.tableau.com/static/images/fo/food_recall_year_01/1_1/1_rss.png'
                        style='border:none' alt='연도별 리콜건수 변화 히트맵'>
                    </a>
                </noscript>

                <object class='tableauViz' style='display:none;'>
                    <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F'/>
                    <param name='embed_code_version' value='3'/>
                    <param name='site_root' value=''/>
                    <param name='name' value='food_recall_year_01/1_1'/>
                    <param name='tabs' value='no'/>
                    <param name='toolbar' value='yes'/>
                    <param name='static_image' value='https://public.tableau.com/static/images/fo/food_recall_year_01/1_1/1.png'/>
                    <param name='animate_transition' value='yes'/>
                    <param name='display_static_image' value='yes'/>
                    <param name='display_spinner' value='yes'/>
                    <param name='display_overlay' value='yes'/>
                    <param name='display_count' value='yes'/>
                    <param name='language' value='ko-KR'/>
                    <param name='filter' value='publish=yes'/>
                </object>
                </div>

                <script type='text/javascript'>
                    var divElement = document.getElementById('viz_recall_trend');
                    var vizElement = divElement.getElementsByTagName('object')[0];
                    
                    if (divElement.offsetWidth > 800) {
                        vizElement.style.width = '100%';
                        vizElement.style.height = (divElement.offsetWidth * 0.75) + 'px';
                    } else if (divElement.offsetWidth > 500) {
                        vizElement.style.width = '100%';
                        vizElement.style.height = (divElement.offsetWidth * 0.75) + 'px';
                    } else {
                        vizElement.style.width = '100%';
                        vizElement.style.height = '727px';
                    }
                    
                    var scriptElement = document.createElement('script');
                    scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
                    vizElement.parentNode.insertBefore(scriptElement, vizElement);
                </script>
                """,
                height=420
            )
            
            st.caption("출처: [FDA Recall Database](https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts)")

    # 2행
    viz_col3, viz_col4= st.columns(2)

    with viz_col3:
        st.markdown("<h5 style='text-align:left;'># 연도/카테고리별 미국 식품 지출 추이</h5>",
                    unsafe_allow_html=True)

        components.html(
            """
            <div class='tableauPlaceholder' id='vizPublic' style='position:relative;width:100%;'>
            <noscript><a href='#'>
                <img src='https://public.tableau.com/static/images/ma/main01/1_1/1.png'
                    style='border:none' alt='대시보드 1'></a></noscript>

            <object class='tableauViz' style='display:none;'>
                <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F'/>
                <param name='embed_code_version' value='3'/>
                <param name='site_root' value=''/>
                <param name='name' value='main01/1_1'/>
                <param name='tabs' value='no'/>
                <param name='toolbar' value='yes'/>
                <param name='language' value='ko-KR'/>
            </object>
            </div>

            <script src='https://public.tableau.com/javascripts/api/viz_v1.js'></script>
            <script>
            // 가로폭에 따라 높이 자동 조정 (비율 = 4:3 예시)
            const divEl = document.getElementById('vizPublic');
            const vizEl = divEl.getElementsByTagName('object')[0];
            vizEl.style.width  = '100%';
            vizEl.style.height = (divEl.offsetWidth * 0.75) + 'px';
            </script>
            """,
            height=540                      # Streamlit 공간 확보용 – 실제 높이는 JS가 재조정
        )
        
        st.caption("출처: [USDA](https://www.ers.usda.gov/data-products/us-food-imports)")
    
    
    with viz_col4:
        st.markdown("<h5 style='text-align: left;'># 리콜 등급(Class)별 발생 건수</h5>", unsafe_allow_html=True)

        components.html(
            """
            <div class='tableauPlaceholder' id='viz_recall_class' style='position:relative; width:100%;'>
            <noscript>
                <a href='#'>
                <img alt='리콜 등급(Class)별 발생 건수' src='https://public.tableau.com/static/images/fo/food_recall_class_01/1_1/1_rss.png' style='border: none' />
                </a>
            </noscript>
            <object class='tableauViz'  style='display:none;'>
                <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' />
                <param name='embed_code_version' value='3' />
                <param name='site_root' value='' />
                <param name='name' value='food_recall_class_01/1_1' />
                <param name='tabs' value='no' />
                <param name='toolbar' value='yes' />
                <param name='static_image' value='https://public.tableau.com/static/images/fo/food_recall_class_01/1_1/1.png' />
                <param name='animate_transition' value='yes' />
                <param name='display_static_image' value='yes' />
                <param name='display_spinner' value='yes' />
                <param name='display_overlay' value='yes' />
                <param name='display_count' value='yes' />
                <param name='language' value='ko-KR' />
                <param name='filter' value='publish=yes' />
            </object>
            </div>
            <script type='text/javascript'>
                var divElement = document.getElementById('viz_recall_class');
                var vizElement = divElement.getElementsByTagName('object')[0];
                
                if (divElement.offsetWidth > 800) {
                    vizElement.style.width='100%';
                    vizElement.style.height=(divElement.offsetWidth*0.75)+'px';
                } else if (divElement.offsetWidth > 500) {
                    vizElement.style.width='100%';
                    vizElement.style.height=(divElement.offsetWidth*0.75)+'px';
                } else {
                    vizElement.style.width='100%';
                    vizElement.style.height='727px';
                }
                
                var scriptElement = document.createElement('script');
                scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
                vizElement.parentNode.insertBefore(scriptElement, vizElement);
            </script>
            """,
            height=540 # 세로 크기를 넉넉하게 확보
        )
        
        st.caption("출처: [FDA Recall Database](https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts)")