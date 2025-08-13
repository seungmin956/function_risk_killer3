# selenium_downloader.py

import os
import time
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys

def setup_selenium_driver():
    """Selenium Chrome 드라이버 설정"""
    options = Options()
    options.add_argument("--headless")  # 백그라운드 실행
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")  # 알림 차단
    options.add_argument("--disable-popup-blocking")  # 팝업 차단 해제
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    download_dir = os.path.abspath("./charts")
    os.makedirs(download_dir, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,  # 팝업 차단
        "profile.default_content_setting_values.notifications": 2  # 알림 차단
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver

def close_all_popups_aggressively(driver):
    """강력한 팝업 닫기"""
    try:
        print("🔄 팝업 닫기 시도 중...")
        
        # 1. ESC 키 여러 번 시도
        for i in range(3):
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(0.5)
        
        # 2. 다양한 팝업 닫기 버튼들
        close_selectors = [
            # 한국어 버튼들
            "button:contains('닫기')",
            "button:contains('확인')", 
            "button:contains('취소')",
            "button[title='닫기']",
            
            # 영어 버튼들
            "button:contains('Close')",
            "button:contains('OK')",
            "button:contains('Cancel')",
            "button[title='Close']",
            "button[aria-label='Close']",
            
            # 일반적인 셀렉터들
            ".close-button",
            ".modal-close", 
            ".popup-close",
            ".btn-close",
            "[data-dismiss='modal']",
            ".fa-times",
            ".icon-close",
            
            # X 버튼들
            "button[aria-label='×']",
            "span:contains('×')",
            ".close",
            
            # Tableau 특화 셀렉터들
            ".tab-modal-close",
            ".tableau-close",
            "[data-tb-test-id='close-button']"
        ]
        
        for selector in close_selectors:
            try:
                # CSS selector 방식
                if ":contains(" in selector:
                    # contains는 CSS에서 지원하지 않으므로 XPath로 변환
                    text = selector.split(":contains('")[1].split("')")[0]
                    xpath = f"//button[contains(text(), '{text}')]"
                    elements = driver.find_elements(By.XPATH, xpath)
                else:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        try:
                            element.click()
                            print(f"✅ 팝업 닫기 성공: {selector}")
                            time.sleep(1)
                        except:
                            # JavaScript로 강제 클릭
                            driver.execute_script("arguments[0].click();", element)
                            time.sleep(1)
                            
            except Exception as e:
                continue
        
        # 3. 모든 모달/팝업 요소 숨기기 (JavaScript 강제 실행)
        hide_script = """
        // 모든 모달 요소들 숨기기
        var modals = document.querySelectorAll('.modal, .popup, .overlay, .dialog, [role="dialog"]');
        modals.forEach(function(modal) {
            modal.style.display = 'none';
            modal.style.visibility = 'hidden';
        });
        
        // Tableau 특화 팝업들 숨기기
        var tableauPopups = document.querySelectorAll('.tab-modal, .tableau-modal, .announcement');
        tableauPopups.forEach(function(popup) {
            popup.style.display = 'none';
            popup.style.visibility = 'hidden';
        });
        
        // z-index가 높은 요소들 숨기기 (팝업일 가능성)
        var allElements = document.querySelectorAll('*');
        allElements.forEach(function(el) {
            var zIndex = window.getComputedStyle(el).zIndex;
            if (zIndex > 1000) {
                el.style.display = 'none';
            }
        });
        """
        
        driver.execute_script(hide_script)
        print("✅ JavaScript로 팝업 강제 숨김 처리")
        time.sleep(2)
        
    except Exception as e:
        print(f"⚠️ 팝업 닫기 중 오류: {e}")

def wait_for_chart_load(driver, timeout=20):
    """차트 로딩 완료까지 대기"""
    try:
        wait = WebDriverWait(driver, timeout)
        
        # Tableau 차트 로딩 완료 신호들
        loading_complete_indicators = [
            # 로딩 스피너가 사라질 때까지 대기
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".loading, .spinner, .tab-loading")) == 0,
            
            # 차트 요소가 나타날 때까지 대기
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".tab-widget, .tableauViz")) > 0
        ]
        
        for indicator in loading_complete_indicators:
            try:
                wait.until(indicator)
            except:
                continue
                
        print("✅ 차트 로딩 완료")
        return True
        
    except Exception as e:
        print(f"⚠️ 차트 로딩 대기 중 오류: {e}")
        return False

def download_single_tableau_chart(driver, url, chart_name, timeout=30):
    """단일 Tableau 차트 다운로드 - 강화된 팝업 처리"""
    try:
        print(f"🔄 {chart_name} 접속 중...")
        driver.get(url)
        
        # 페이지 기본 로드 대기
        time.sleep(3)
        
        # 첫 번째 팝업 닫기 시도
        close_all_popups_aggressively(driver)
        
        # 차트 로딩 대기
        wait_for_chart_load(driver)
        
        # 두 번째 팝업 닫기 시도 (차트 로드 후 나타날 수 있음)
        close_all_popups_aggressively(driver)
        
        # 추가 대기 시간
        time.sleep(2)
        
        # Tableau 차트 영역 찾기
        chart_selectors = [
            ".tab-widget",
            ".tableauViz",
            "[data-testid='viz-container']", 
            ".viz-content",
            "#tableau",
            ".tab-content",
            ".visualization-content",
            ".tab-dashboard"
        ]
        
        chart_element = None
        for selector in chart_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    # 가장 큰 요소 선택 (메인 차트일 가능성이 높음)
                    chart_element = max(elements, key=lambda e: e.size['width'] * e.size['height'])
                    # 최소 크기 확인 (너무 작으면 차트가 아닐 수 있음)
                    if chart_element.size['width'] > 200 and chart_element.size['height'] > 200:
                        print(f"✅ {chart_name} 차트 영역 발견: {selector} ({chart_element.size['width']}x{chart_element.size['height']})")
                        break
                    else:
                        chart_element = None
            except:
                continue
        
        if chart_element:
            try:
                # 차트 영역이 화면에 완전히 보이도록 스크롤
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", chart_element)
                time.sleep(1)
                
                # 마지막 팝업 닫기 시도
                close_all_popups_aggressively(driver)
                time.sleep(1)
                
                # 차트 영역만 스크린샷
                screenshot_path = f"./charts/{chart_name}.png"
                chart_element.screenshot(screenshot_path)
                
                if os.path.exists(screenshot_path):
                    size = os.path.getsize(screenshot_path)
                    print(f"✅ {chart_name} 차트 영역 저장 완료 ({size} bytes)")
                    return True
                else:
                    print(f"❌ {chart_name} 저장 실패")
                    return False
                    
            except Exception as e:
                print(f"❌ {chart_name} 차트 영역 캡처 실패: {e}")
                # 대안: 전체 페이지 스크린샷
                try:
                    screenshot_path = f"./charts/{chart_name}.png"
                    driver.save_screenshot(screenshot_path)
                    
                    if os.path.exists(screenshot_path):
                        size = os.path.getsize(screenshot_path)
                        print(f"⚠️ {chart_name} 전체 페이지로 저장 ({size} bytes)")
                        return True
                    return False
                except:
                    return False
        else:
            print(f"⚠️ {chart_name} 차트 영역을 찾을 수 없음")
            # 최후의 수단: 전체 페이지 스크린샷
            try:
                screenshot_path = f"./charts/{chart_name}.png"
                driver.save_screenshot(screenshot_path)
                
                if os.path.exists(screenshot_path):
                    size = os.path.getsize(screenshot_path)
                    print(f"⚠️ {chart_name} 전체 페이지로 저장 ({size} bytes)")
                    return True
                return False
            except:
                return False

    except Exception as e:
        print(f"❌ {chart_name} 다운로드 실패: {e}")
        return False

def download_all_tableau_charts():
    """모든 Tableau 차트 다운로드"""
    charts = {
        "state_food": "https://public.tableau.com/views/state_food_exp2_17479635670940/State",
        "food_trend": "https://public.tableau.com/views/main01/1_1",
        "recall_heatmap": "https://public.tableau.com/views/food_recall_year_01/1_1",
        "recall_class": "https://public.tableau.com/views/food_recall_class_01/1_1"
    }

    print("🚀 Tableau 차트 다운로드 시작...")
    driver = None
    success_count = 0

    try:
        driver = setup_selenium_driver()

        for chart_name, url in charts.items():
            result = download_single_tableau_chart(driver, url, chart_name)
            if result:
                success_count += 1
            time.sleep(3)  # 서버 부하 방지

        print(f"\n🎉 다운로드 완료: {success_count}/{len(charts)}개 성공")

    except Exception as e:
        print(f"❌ 전체 프로세스 실패: {e}")

    finally:
        if driver:
            driver.quit()
            print("🔚 브라우저 종료")

    return success_count

# 나머지 함수들은 동일
def show_downloaded_images():
    """다운로드된 이미지 표시"""
    import streamlit as st

    chart_files = {
        "state_food.png": "🗺️ 미국 주별 식품 지출",
        "food_trend.png": "📈 연도별 식품 지출 추이",
        "recall_heatmap.png": "🔥 리콜 원인별 히트맵",
        "recall_class.png": "📊 리콜 등급별 발생 건수"
    }

    st.markdown("### 📊 다운로드된 차트들")
    cols = st.columns(2)

    for i, (filename, title) in enumerate(chart_files.items()):
        file_path = f"./charts/{filename}"
        with cols[i % 2]:
            if os.path.exists(file_path):
                st.image(file_path, caption=title, use_column_width=True)
                size = os.path.getsize(file_path)
                st.caption(f"✅ {filename} ({size} bytes)")
            else:
                st.error(f"❌ {filename} 없음")

if __name__ == "__main__":
    download_all_tableau_charts()