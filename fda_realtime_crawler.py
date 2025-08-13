# ./fda_realtime_crawler.py
import re
import asyncio
import json
import sqlite3
import os
from datetime import datetime,timedelta
from playwright.async_api import async_playwright
from urllib.parse import urljoin
from db_utils import save_to_sqlite, save_to_chromadb

def get_latest_date_from_db():
    """SQLite DB에서 가장 최신 날짜 조회"""
    db_path = "./data/fda_recalls.db"
    if not os.path.exists(db_path):
        print("📋 DB 파일이 존재하지 않음")
        return None
        
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 🆕 내림차순 정렬해서 가장 최신 데이터 1개 조회
        cursor.execute("""
            SELECT fda_publish_date 
            FROM recalls 
            WHERE fda_publish_date IS NOT NULL 
            ORDER BY fda_publish_date DESC 
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            latest_date = result[0]
            print(f"📊 DB에서 조회된 최신 날짜: {latest_date}")
            
            # 이미 YYYY-MM-DD 형식이면 그대로 반환
            if len(latest_date) == 10 and latest_date.count('-') == 2:
                return latest_date
            
            # 다른 형식이면 변환 시도
            try:
                parsed_date = datetime.strptime(latest_date, "%Y-%m-%d")
                return parsed_date.strftime("%Y-%m-%d")
            except:
                print(f"⚠️ 날짜 형식 변환 실패: {latest_date}")
                return latest_date  # 원본 그대로 반환
        else:
            print("📋 DB에 데이터가 없음")
            return None
        
    except Exception as e:
        print(f"DB 최신 날짜 조회 오류: {e}")
        return None
    

async def crawl_incremental_links():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        page = await browser.new_page()

        # 🆕 User-Agent 추가 (브라우저 위장)
        await page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        try:
            print("🌐 FDA 사이트 접속 (필터링 없이)...")
            await page.goto("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/")
            await page.wait_for_load_state('networkidle')
            print("✅ 페이지 로딩 완료")
            
        except Exception as e:
            print(f"💥 페이지 로딩 실패: {e}")
            return []

        base_url = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
        all_brand_urls = []
        current_page_count = 1

        latest_db_date = get_latest_date_from_db()
        print(f"📊 DB 최신 날짜: {latest_db_date}")
        
        max_pages = 10  # 안전장치
        while current_page_count <= max_pages:
            print(f"현재 {current_page_count}페이지 처리 중...")
            
            # 🆕 다양한 테이블 셀렉터 시도
            table_selectors = [
                "table tbody tr",
                ".view-content .views-row", 
                "table tr",
                ".views-table tbody tr"
            ]
            
            rows_found = False
            for table_selector in table_selectors:
                try:
                    await page.wait_for_selector(table_selector, timeout=15000)
                    rows = await page.locator(table_selector).all()
                    if len(rows) > 0:
                        print(f"✅ 테이블 발견: {len(rows)}개 행 ({table_selector})")
                        rows_found = True
                        break
                except:
                    continue
            
            if not rows_found:
                print("⚠️ 테이블을 찾을 수 없음 - 종료")
                break
            
            # 🆕 조건부 데이터 수집
            try:
                # 날짜, 링크, Product Type 동시 수집
                date_elements = await page.locator("td:nth-child(1)").all()  # 날짜
                link_elements = await page.locator("td:nth-child(2) a").all()  # 링크  
                product_type_elements = await page.locator("td:nth-child(4)").all()  # Product Type
                
                print(f"📊 발견된 요소: 날짜 {len(date_elements)}개, 링크 {len(link_elements)}개, 제품타입 {len(product_type_elements)}개")
                
            except Exception as e:
                print(f"⚠️ 요소 수집 실패: {e}")
                break
            
            page_has_new_data = False
            should_break = False
            
            # 🎯 조건부 수집 로직
            for i in range(min(len(date_elements), len(link_elements), len(product_type_elements))):
                try:
                    # 날짜 추출
                    date_text = await date_elements[i].text_content()
                    date_text = date_text.strip()
                    
                    # 날짜 변환
                    date_only = None
                    if 'T' in date_text:
                        date_only = date_text.split('T')[0]
                    elif '/' in date_text:
                        try:
                            parsed_date = datetime.strptime(date_text, "%m/%d/%Y")
                            date_only = parsed_date.strftime("%Y-%m-%d")
                        except:
                            date_only = date_text
                    elif '-' in date_text and len(date_text) == 10:
                        date_only = date_text
                    else:
                        continue  # 날짜 형식을 파싱할 수 없으면 스킵
                    
                    print(f"  📅 #{i}: 날짜 '{date_only}'")
                    
                    # 🚨 DB 최신 날짜와 비교 (더 오래된 데이터면 중단)
                    if latest_db_date and date_only:
                        try:
                            current_date_obj = datetime.strptime(date_only, "%Y-%m-%d")
                            latest_date_obj = datetime.strptime(latest_db_date, "%Y-%m-%d")
                            
                            if current_date_obj <= latest_date_obj:
                                print(f"📊 기존 DB 날짜 도달: {date_only} (DB 최신: {latest_db_date}) - 중단")
                                should_break = True
                                break
                        except Exception as e:
                            print(f"  ⚠️ 날짜 비교 오류: {e}")
                    
                    # Product Type 확인
                    product_type_text = await product_type_elements[i].text_content()
                    product_type_text = product_type_text.strip().lower()
                    
                    print(f"  🏷️ #{i}: 제품타입 '{product_type_text[:50]}...'")
                    
                    # 🎯 Food & Beverages 정확한 조건 확인
                    # 콤마로 분리해서 첫 번째가 "Food & Beverages"인지 확인
                    product_parts = product_type_text.split(',')
                    first_part = product_parts[0].strip()
                    
                    is_food_beverage = first_part.lower() == "food & beverages"
                    
                    print(f"  🔍 #{i}: 첫번째 부분 '{first_part}' → {'✅' if is_food_beverage else '❌'}")
                    
                    if is_food_beverage:
                        # URL 수집
                        url = await link_elements[i].get_attribute("href")
                        brand_name = await link_elements[i].text_content()
                        full_url = urljoin(base_url, url)
                        
                        all_brand_urls.append({
                            "name": brand_name.strip(), 
                            "url": full_url,
                            "date": date_only,
                            "product_type": product_type_text
                        })
                        page_has_new_data = True
                        print(f"  ✅ 수집: {date_only} - {brand_name.strip()}")
                    else:
                        print(f"  ⏭️ 스킵: Food & Beverages 아님")
                        
                except Exception as e:
                    print(f"  ⚠️ 항목 {i} 처리 오류: {e}")
                    continue
            
            # 🆕 페이지별 조건 확인 후 다음 페이지 결정
            if should_break:
                print(f"🔚 기존 DB 날짜 도달로 크롤링 종료 (페이지 {current_page_count})")
                break
            
            if not page_has_new_data:
                print(f"💡 페이지 {current_page_count}에서 새로운 Food & Beverages 데이터 없음")
                print(f"🔚 더 이상 진행할 필요 없음 - 크롤링 종료")
                break  # 🆕 새로운 데이터가 없으면 바로 종료
            
            # 다음 페이지 이동
            try:
                next_selectors = [
                    "a[rel='next']",
                    ".pager-next a",
                    ".pagination .next a",
                    "a:has-text('Next')",
                    "a:has-text('›')"
                ]
                
                next_found = False
                for next_selector in next_selectors:
                    try:
                        next_button = page.locator(next_selector)
                        if await next_button.count() > 0:
                            await next_button.click()
                            await page.wait_for_load_state('networkidle')
                            current_page_count += 1
                            next_found = True
                            print(f"  ➡️ 다음 페이지로 이동 ({current_page_count})")
                            break
                    except:
                        continue
                
                if not next_found:
                    print("🔚 다음 페이지 없음 - 종료")
                    break
                    
            except Exception as e:
                print(f"🔚 페이지 이동 실패 - 종료: {e}")
                break
        
        # 결과 정리
        unique_urls = []
        seen_urls = set()
        
        for item in all_brand_urls:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                unique_urls.append(item)
        
        print(f"\n📊 최종 결과:")
        print(f"총 처리 페이지: {current_page_count}개")
        print(f"Food & Beverages 데이터: {len(all_brand_urls)}개")
        print(f"중복 제거 후: {len(unique_urls)}개")
        
        await browser.close()
        return unique_urls

def check_existing_urls(new_urls):
    """기존 DB에서 URL 중복 체크"""
    db_path = "./data/fda_recalls.db"
    if not os.path.exists(db_path):
        return new_urls  # DB 없으면 모든 URL 처리
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    existing_urls = set()
    for url_info in new_urls:
        cursor.execute("SELECT url FROM recalls WHERE url = ?", (url_info["url"],))
        if cursor.fetchone():
            existing_urls.add(url_info["url"])
    
    conn.close()
    
    # 새로운 URL만 필터링
    filtered_urls = [url_info for url_info in new_urls if url_info["url"] not in existing_urls]
    print(f"🔍 중복 체크: {len(new_urls)}개 → {len(filtered_urls)}개 (새로운 데이터)")
    
    return filtered_urls

#url별 세부내용 추출 함수
async def crawl_brand_detail(url):
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-dev-shm-usage']  # 이 옵션들이 필요
            )
            page= await browser.new_page()

            await page.goto(url) #url로 이동
            await page.wait_for_load_state("networkidle") #페이지 로딩 대기

        #회사 리콜 발표일 추출
            company_announcement_element= page.locator("dt:has-text('Company Announcement Date')+dd")
            company_announcement_date_raw = await company_announcement_element.text_content()

            try:
                date_obj=datetime.strptime(company_announcement_date_raw.strip(), "%B %d, %Y")
                company_announcement_date= date_obj.strftime("%Y-%m-%d")
            except ValueError:
                company_announcement_date=company_announcement_date_raw.strip()

            # print(f"회사 리콜 발표일: {company_announcement_date}")

        #FDA 리콜 발표일 추출
            FDA_publish_element= page.locator("dt:has-text('FDA Publish Date') +dd")
            FDA_publish_date_raw= await FDA_publish_element.text_content()

            try: 
                date_obj2= datetime.strptime(FDA_publish_date_raw.strip(), "%B %d, %Y")
                FDA_publish_date= date_obj2.strftime("%Y-%m-%d")
            except ValueError:
                FDA_publish_date= FDA_publish_date_raw.strip()

            # print(f"FDA 리콜 발표일: {FDA_publish_date}")

        #회사명 추출
            company_element=page.locator("dt:has-text('Company Name') + dd")
            company_name= await company_element.text_content()

            # print(f"회사명:{company_name}")

        #브랜드명 추출
            try:
                brand_element=page.locator("dt:has-text('Brand Name') + dd .field--item")
                brand_count= await brand_element.text_content()

                if brand_count>0:
                    brand_names=[]
                    for i in range(brand_count):
                        brand_text= await brand_element.nth(i).text_content()
                        if brand_text and brand_text.strip():
                            brand_names.append(brand_text.strip())
                    brand_name= "/".join(brand_names)
                else:
                    brand_name=""
            except:
                brand_name=""

            # print(f"브랜드명:{brand_name}")

        #리콜원인 추출
            try:
                recall_reason_element = page.locator("dt:has-text('Product Type') + dd")
                total_recall_reason = await recall_reason_element.text_content()
                
                if total_recall_reason and total_recall_reason.strip():
                    # 두 번째 줄이나 마지막 단어 추출
                    lines = total_recall_reason.strip().split('\n')
                    if len(lines) >= 2:
                        recall_reason = lines[1].strip()  # 두 번째 줄
                    else:
                        recall_reason = total_recall_reason.split()[-1]  # 마지막 단어
                else:
                    recall_reason = ""
            except Exception as e:
                print(f"⚠️ 리콜원인 추출 실패: {e}")
                recall_reason = ""

            # print(f"리콜원인:{recall_reason}")

        #자세한리콜 사유 추출
            try:
                recall_reason_detail_element = page.locator("dt:has-text('Reason for Announcement')+ dd .field--item")
                recall_reason_detail = await recall_reason_detail_element.text_content(timeout=5000)
                recall_reason_detail = recall_reason_detail.strip() if recall_reason_detail else ""
            except:
                recall_reason_detail = ""

            # print(f"자세한리콜사유:{recall_reason_detail}")

        #식품종류 추출
            product_element= page.locator("dt:has-text('Product Description')+dd .field--item")
            product= await product_element.text_content()
            
            # print(f"식품종류:{product}")

        #내용 추출
            content_elements = await page.locator("h2:has-text('Company Announcement') ~ p:not(.inset_column p)").all()
            content_parts = []

            for element in content_elements:
                text = await element.text_content()
                if text and text.strip():
                    content_parts.append(text.strip())

            content = "\n\n".join(content_parts)
            # print(f"내용: {content}")


            await browser.close()

        #dict 형태로 구성
            recall_data= {
                "document_type": "recall",
                "url": url,
                "company_announcement_date": company_announcement_date,
                "fda_publish_date": FDA_publish_date,
                "company_name": company_name.strip() if company_name else "",
                "brand_name": brand_name.strip() if brand_name else "",
                "recall_reason": recall_reason.strip() if recall_reason else "",
                "recall_reason_detail": recall_reason_detail.strip() if recall_reason_detail else "",
                "product_type": product.strip() if product else "",
                "content": content,
            }

            print(f"✅ 크롤링 완료: {brand_name}")
            print(f"회사: {company_name}")
            print(f"리콜 발표일: {company_announcement_date}")
            print("-" * 50)

            return recall_data
        except Exception as e:
            print(f" {url} 크롤링 실패: {e}")
            return None


#JSON 파일 저장 함수 
def save_to_json(data_list, filename="fda_recalls.json"):
    """크롤링한 데이터를 JSON 파일로 저장"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)
    print(f"📁 {len(data_list)}개 데이터가 {filename}에 저장되었습니다.")


#메인함수
async def main():
    print("🚀 증분 크롤링 시작...")
    
    # 1. 증분 링크 수집
    brand_urls = await crawl_incremental_links()
    
    # 2. 중복 체크
    filtered_urls = check_existing_urls(brand_urls)
    
    if not filtered_urls:
        print("📋 새로운 데이터가 없습니다.")
        return
    
    print(f"📊 처리할 새로운 데이터: {len(filtered_urls)}개")
    
    # 3. 세부 정보 크롤링
    all_results = []
    for i, brand_info in enumerate(filtered_urls, 1):
        print(f"\n{i}/{len(filtered_urls)} 처리중...")
        try:
            result = await crawl_brand_detail(brand_info["url"])
            if result:
                all_results.append(result)
        except Exception as e:
            print(f"오류 발생: {brand_info['name']} - {e}")
    
    if not all_results:
        print("❌ 크롤링된 데이터가 없습니다.")
        return
    
    # 4. 파일명 생성 (타임스탬프 포함)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # 5. JSON 파일 저장 (기존 함수 활용)
    json_filename = f"./data/realtime_recalls_{timestamp}.json"
    save_to_json(all_results, json_filename)
    
    # 6. DB 저장
    print(f"💾 {len(all_results)}개 데이터를 DB에 저장 중...")
    save_to_sqlite(all_results)
    save_to_chromadb(all_results)
    
    print(f"🎉 증분 크롤링 완료!")
    print(f"   📄 JSON: {json_filename}")
    print(f"   🗄️ SQLite: ./data/fda_recalls.db") 
    print(f"   🔍 ChromaDB: ./data/chroma_db_recall")
    print(f"   📊 새로운 데이터: {len(all_results)}개")

# 저장된 brand_urls.json을 불러와서 상세 크롤링만 하는 함수
async def main_from_saved_urls(json_file):
    """저장된 brand_urls.json을 사용해서 상세 크롤링만 실행"""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            brand_urls = json.load(f)
        print(f"📂 저장된 브랜드 URL {len(brand_urls)}개를 불러왔습니다.")
    except FileNotFoundError:
        print("❌ brand_urls.json 파일을 찾을 수 없습니다. 먼저 main()을 실행하세요.")
        return
    
    today=datetime.now().strftime("%m%d")

    all_results = []
    failed_urls = [] 
    for i, brand_info in enumerate(brand_urls, 1):
        print(f"\n {i}/{len(brand_urls)} 처리중...")
        try:
            result = await crawl_brand_detail(brand_info["url"])
            if result:  # 성공한 경우만
                all_results.append(result)
            else:  # None 반환 시 실패로 간주
                failed_urls.append(brand_info)
        except Exception as e:
            print(f"오류 발생 : {brand_info['name']} - {e}")
            failed_urls.append(brand_info)

    save_to_json(all_results, f"fda_recalls_{today}.json")
    save_to_json(failed_urls, f"failed_urls2_{today}.json")

    print(f"🎉 총 {len(all_results)}개 데이터 크롤링 완료!")
    print(f"❌ 실패: {len(failed_urls)}개")

if __name__ == "__main__":
    asyncio.run(main())