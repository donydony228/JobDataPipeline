# src/scrapers/indeed_selenium_scraper.py
# Indeed 職缺爬蟲 - Selenium 版本 (更穩定,不易被封鎖)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import random
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlencode
import logging
import re


class IndeedSeleniumScraper:
    """
    Indeed 職缺爬蟲 - Selenium 版本
    
    優點:
    - 模擬真實瀏覽器,繞過反爬機制
    - 可處理 JavaScript 動態載入內容
    - 更穩定,不易被封鎖
    
    缺點:
    - 速度較慢
    - 需要 Chrome/ChromeDriver
    
    使用範例:
        scraper = IndeedSeleniumScraper(config={
            'search_keywords': ['data engineer'],
            'locations': ['San Francisco, CA'],
            'target_jobs': 50,
            'headless': True  # 無頭模式
        })
        jobs = scraper.scrape_jobs()
    """
    
    BASE_URL = "https://www.indeed.com"
    SEARCH_URL = f"{BASE_URL}/jobs"
    
    def __init__(self, config: Dict):
        """
        初始化 Selenium 爬蟲
        
        Args:
            config: 爬蟲配置
                - search_keywords: List[str] - 搜尋關鍵字
                - locations: List[str] - 搜尋地點
                - target_jobs: int - 目標職缺數量
                - max_pages: int - 最大爬取頁數
                - delay_range: tuple - 延遲範圍 (秒)
                - headless: bool - 是否使用無頭模式
                - scrape_details: bool - 是否爬取詳細頁
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 爬取統計
        self.scraped_jobs = []
        self.success_count = 0
        self.failed_count = 0
        self.total_attempts = 0
        
        # 初始化 Selenium WebDriver
        self.driver = None
        self._setup_driver()
    
    def _setup_driver(self):
        """設定 Selenium WebDriver"""
        try:
            chrome_options = Options()
            
            # 無頭模式 (背景執行,不顯示瀏覽器視窗)
            if self.config.get('headless', True):
                chrome_options.add_argument('--headless')
            
            # 反爬偵測設定
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # 性能優化
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            # User-Agent (模擬真實瀏覽器)
            chrome_options.add_argument(
                'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )
            
            # 視窗大小
            chrome_options.add_argument('--window-size=1920,1080')
            
            # 自動安裝並設定 ChromeDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # 隱藏 WebDriver 標記
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })
            
            # 設定隱式等待
            self.driver.implicitly_wait(10)
            
            self.logger.info("✅ Selenium WebDriver 初始化成功")
            
        except Exception as e:
            self.logger.error(f"❌ WebDriver 初始化失敗: {str(e)}")
            raise
    
    def _apply_rate_limiting(self):
        """應用延遲限制 (模擬人類行為)"""
        delay_range = self.config.get('delay_range', (3, 6))
        delay = random.uniform(delay_range[0], delay_range[1])
        
        # 隨機滾動頁面 (更像真人)
        if random.random() > 0.5:
            scroll_amount = random.randint(300, 800)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(0.5, 1.5))
        
        time.sleep(delay)
    
    def _build_search_url(self, keyword: str, location: str, start: int = 0) -> str:
        """
        建構搜尋 URL (支援進階篩選)
        
        Args:
            keyword: 搜尋關鍵字
            location: 地點
            start: 起始位置 (分頁)
        
        Returns:
            完整搜尋 URL
        """
        # 基礎參數
        params = {
            'q': keyword,
            'l': location,
            'start': start,
            'sort': 'date',  # 按日期排序
        }
        
        # 從 config 讀取額外篩選條件
        
        # 1. 時間範圍 (fromage: 過去幾天)
        # 1 = 24小時, 3 = 3天, 7 = 7天, 14 = 14天
        if 'fromage' in self.config:
            params['fromage'] = self.config['fromage']
        
        # 2. 職位類型 (sc: 職位類型篩選)
        # 格式: 0kf:attr(CODE1|CODE2,OR)
        # 常見代碼:
        #   4HKF7 = Internship (實習)
        #   75GKK = Entry Level (入門級)
        #   VDTG7 = Mid Level (中階)
        #   HFDVW = Senior Level (資深)
        if 'job_types' in self.config:
            job_types = self.config['job_types']
            if job_types:
                # 職位類型代碼對應
                type_codes = {
                    'internship': '4HKF7',
                    'entry_level': '75GKK',
                    'mid_level': 'VDTG7',
                    'senior_level': 'HFDVW'
                }
                
                codes = []
                for job_type in job_types:
                    if job_type.lower() in type_codes:
                        codes.append(type_codes[job_type.lower()])
                
                if codes:
                    params['sc'] = f"0kf:attr({('|'.join(codes))},OR);"
        
        # 3. 遠端工作 (remote)
        if self.config.get('remote_only', False):
            params['remotejob'] = '032b3046-06a3-4876-8dfd-474eb5e7ed11'
        
        # 4. 薪資範圍 (salary)
        if 'salary_min' in self.config:
            params['salary'] = self.config['salary_min']
        
        # 5. 全職/兼職等 (jt: job type)
        # fulltime, parttime, contract, temporary, internship
        if 'employment_type' in self.config:
            params['jt'] = self.config['employment_type']
        
        return f"{self.SEARCH_URL}?{urlencode(params)}"
    
    def _wait_for_jobs_to_load(self, timeout: int = 15):
        """等待職缺列表載入完成"""
        try:
            # 等待職缺卡片出現
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.job_seen_beacon, li.css-5lfssm"))
            )
            
            # 額外等待確保 JavaScript 完全執行
            time.sleep(random.uniform(2, 4))
            
            return True
        except TimeoutException:
            self.logger.warning("等待職缺載入逾時")
            return False
    
    def _parse_job_card_selenium(self, card_element) -> Optional[Dict]:
        """
        解析單個職缺卡片 (從 Selenium WebElement)
        
        Args:
            card_element: Selenium WebElement
        
        Returns:
            職缺資料字典
        """
        try:
            job_data = {}
            
            # 使用 BeautifulSoup 解析 (更方便)
            card_html = card_element.get_attribute('outerHTML')
            soup = BeautifulSoup(card_html, 'html.parser')
            
            # 1. 職位標題和 URL
            title_elem = soup.find('h2', class_=re.compile('jobTitle|job-title', re.I))
            if not title_elem:
                title_elem = soup.find('a', class_=re.compile('jcs-JobTitle'))
            
            if title_elem:
                title_link = title_elem.find('a') if title_elem.name != 'a' else title_elem
                if title_link:
                    job_data['job_title'] = title_link.get_text(strip=True)
                    href = title_link.get('href', '')
                    
                    if href:
                        if href.startswith('http'):
                            job_data['job_url'] = href
                        else:
                            job_data['job_url'] = f"{self.BASE_URL}{href}"
                        
                        # 提取 job_id
                        if 'jk=' in href:
                            job_id = href.split('jk=')[1].split('&')[0]
                            job_data['job_id'] = job_id
                        else:
                            job_data['job_id'] = href.split('/')[-1].split('?')[0]
            
            # 2. 公司名稱
            company_elem = soup.find('span', {'data-testid': 'company-name'})
            if not company_elem:
                company_elem = soup.find('span', class_=re.compile('companyName'))
            if company_elem:
                job_data['company_name'] = company_elem.get_text(strip=True)
            
            # 3. 地點
            location_elem = soup.find('div', {'data-testid': 'text-location'})
            if not location_elem:
                location_elem = soup.find('div', class_=re.compile('companyLocation'))
            if location_elem:
                job_data['location'] = location_elem.get_text(strip=True)
            
            # 4. 薪資
            salary_elem = soup.find('div', class_=re.compile('salary|metadata'))
            if salary_elem:
                salary_text = salary_elem.get_text(strip=True)
                if '$' in salary_text or 'hour' in salary_text.lower():
                    job_data['salary_text'] = salary_text
                    job_data['has_salary'] = True
                else:
                    job_data['salary_text'] = None
                    job_data['has_salary'] = False
            else:
                job_data['salary_text'] = None
                job_data['has_salary'] = False
            
            # 5. 職缺摘要
            snippet_elem = soup.find('div', class_=re.compile('snippet|job-snippet'))
            if snippet_elem:
                job_data['description_snippet'] = snippet_elem.get_text(strip=True)
            
            # 6. 職缺類型
            metadata_elem = soup.find('div', class_=re.compile('metadata'))
            if metadata_elem:
                job_data['metadata_text'] = metadata_elem.get_text(strip=True)
            
            # 7. 其他標記
            job_data['urgently_hiring'] = soup.find(string=re.compile('urgently', re.I)) is not None
            job_data['is_new'] = soup.find('span', string=re.compile('new', re.I)) is not None
            
            # 8. 爬取時間戳
            job_data['scraped_at'] = datetime.now().isoformat()
            job_data['source'] = 'indeed'
            job_data['scraper_type'] = 'selenium'
            
            # 驗證必要欄位
            required_fields = ['job_title', 'company_name']
            if all(field in job_data for field in required_fields):
                return job_data
            else:
                self.logger.warning(f"職缺卡片缺少必要欄位")
                return None
            
        except Exception as e:
            self.logger.error(f"解析職缺卡片失敗: {str(e)}")
            return None
    
    def _scrape_search_page(self, url: str) -> List[Dict]:
        """
        爬取單個搜尋結果頁 (Selenium)
        
        Args:
            url: 搜尋頁 URL
        
        Returns:
            職缺列表
        """
        jobs = []
        
        try:
            self.logger.info(f"🌐 正在爬取: {url}")
            
            # 載入頁面
            self.driver.get(url)
            
            # 等待職缺載入
            if not self._wait_for_jobs_to_load():
                self.logger.warning("職缺載入失敗或頁面無職缺")
                return jobs
            
            # 滾動頁面確保所有內容載入
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(1)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # 找到所有職缺卡片
            job_cards = self.driver.find_elements(By.CSS_SELECTOR, "div.job_seen_beacon, li.css-5lfssm, div.cardOutline")
            
            self.logger.info(f"📋 找到 {len(job_cards)} 個職缺卡片")
            
            # 解析每個職缺
            for i, card in enumerate(job_cards, 1):
                try:
                    job_data = self._parse_job_card_selenium(card)
                    if job_data:
                        jobs.append(job_data)
                        self.success_count += 1
                        self.logger.info(f"  ✅ 職缺 {i}/{len(job_cards)}: {job_data.get('job_title', 'Unknown')}")
                    else:
                        self.failed_count += 1
                        self.logger.warning(f"  ⚠️  職缺 {i}/{len(job_cards)}: 解析失敗")
                    
                    self.total_attempts += 1
                    
                except Exception as e:
                    self.logger.error(f"  ❌ 職缺 {i}/{len(job_cards)}: {str(e)}")
                    self.failed_count += 1
                    self.total_attempts += 1
            
        except Exception as e:
            self.logger.error(f"❌ 爬取頁面失敗: {str(e)}")
        
        return jobs
    
    def scrape_jobs(self) -> List[Dict]:
        """
        主要爬取方法
        
        Returns:
            職缺列表
        """
        self.logger.info("🚀 開始使用 Selenium 爬取 Indeed 職缺...")
        
        search_keywords = self.config.get('search_keywords', ['data engineer'])
        locations = self.config.get('locations', ['United States'])
        target_jobs = self.config.get('target_jobs', 50)
        max_pages = self.config.get('max_pages', 5)
        
        all_jobs = []
        
        try:
            # 遍歷每個搜尋組合
            for keyword in search_keywords:
                for location in locations:
                    self.logger.info(f"📍 搜尋: {keyword} @ {location}")
                    
                    page = 0
                    jobs_for_this_search = 0
                    
                    while page < max_pages and len(all_jobs) < target_jobs:
                        # 建構搜尋 URL
                        start = page * 10
                        search_url = self._build_search_url(keyword, location, start)
                        
                        # 爬取頁面
                        jobs = self._scrape_search_page(search_url)
                        
                        if not jobs:
                            self.logger.info(f"  📭 第 {page + 1} 頁沒有職缺,停止搜尋")
                            break
                        
                        all_jobs.extend(jobs)
                        jobs_for_this_search += len(jobs)
                        
                        self.logger.info(
                            f"  ✅ 第 {page + 1} 頁完成: {len(jobs)} 個職缺 "
                            f"(本次: {jobs_for_this_search}, 總計: {len(all_jobs)})"
                        )
                        
                        # 達到目標
                        if len(all_jobs) >= target_jobs:
                            break
                        
                        # 延遲避免封鎖
                        self._apply_rate_limiting()
                        page += 1
                    
                    if len(all_jobs) >= target_jobs:
                        break
                
                if len(all_jobs) >= target_jobs:
                    break
            
            self.scraped_jobs = all_jobs[:target_jobs]
            
            self.logger.info(f"\n🎉 爬取完成!")
            self.logger.info(f"   總計職缺: {len(self.scraped_jobs)}")
            self.logger.info(f"   成功: {self.success_count}")
            self.logger.info(f"   失敗: {self.failed_count}")
            self.logger.info(f"   成功率: {self.get_success_rate():.1%}")
            
        except Exception as e:
            self.logger.error(f"❌ 爬取過程發生錯誤: {str(e)}")
        finally:
            # 確保關閉瀏覽器
            self.close()
        
        return self.scraped_jobs
    
    def get_success_rate(self) -> float:
        """計算成功率"""
        if self.total_attempts == 0:
            return 0.0
        return self.success_count / self.total_attempts
    
    def get_scraping_stats(self) -> Dict:
        """取得爬取統計"""
        return {
            'total_attempts': self.total_attempts,
            'successful_jobs': self.success_count,
            'failed_jobs': self.failed_count,
            'success_rate': self.get_success_rate(),
            'scraped_jobs_count': len(self.scraped_jobs),
            'source': 'indeed',
            'scraper_type': 'selenium'
        }
    
    def close(self):
        """關閉 WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("🔒 WebDriver 已關閉")
            except Exception as e:
                self.logger.error(f"關閉 WebDriver 失敗: {str(e)}")
    
    def __del__(self):
        """析構函數 - 確保資源釋放"""
        self.close()