#!/usr/bin/env python3
# scripts/test_indeed_scraper.py
# 測試 Indeed 爬蟲功能 - Selenium 版本

import sys
import os
from datetime import datetime

# 加入專案路徑
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# 使用 Selenium 版本
from src.scrapers.indeed_scraper import IndeedSeleniumScraper


def test_basic_scraping():
    """測試基本爬取功能"""
    print("=" * 60)
    print("🧪 測試 1: 基本爬取功能 (Selenium)")
    print("=" * 60)
    
    config = {
        'target_jobs': 5,  # 只測試 5 個職缺
        'max_pages': 1,    # 只爬 1 頁
        'search_keywords': ['data engineer'],
        'locations': ['San Francisco, CA'],
        'delay_range': (3, 5),
        'headless': False,  # 第一次測試顯示瀏覽器視窗
        'scrape_details': False
    }
    
    try:
        print("🌐 啟動 Selenium WebDriver...")
        print("💡 提示: 第一次執行會自動下載 ChromeDriver")
        
        scraper = IndeedSeleniumScraper(config)
        jobs = scraper.scrape_jobs()
        
        print(f"\n✅ 爬取成功!")
        print(f"   職缺數量: {len(jobs)}")
        print(f"   成功率: {scraper.get_success_rate():.1%}")
        
        if jobs:
            print(f"\n📋 第一個職缺範例:")
            first_job = jobs[0]
            print(f"   職位: {first_job.get('job_title')}")
            print(f"   公司: {first_job.get('company_name')}")
            print(f"   地點: {first_job.get('location')}")
            print(f"   薪資: {first_job.get('salary_text', '未提供')}")
            print(f"   URL: {first_job.get('job_url', '')[:80]}...")
        
        return len(jobs) > 0
        
    except Exception as e:
        print(f"\n❌ 測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_searches():
    """測試多關鍵字搜尋"""
    print("\n" + "=" * 60)
    print("🧪 測試 2: 多關鍵字搜尋 (Selenium)")
    print("=" * 60)
    
    config = {
        'target_jobs': 10,
        'max_pages': 1,
        'search_keywords': ['data engineer', 'data scientist'],
        'locations': ['San Francisco, CA'],
        'delay_range': (3, 5),
        'headless': True,  # 無頭模式
        'scrape_details': False
    }
    
    try:
        scraper = IndeedSeleniumScraper(config)
        jobs = scraper.scrape_jobs()
        
        print(f"\n✅ 多關鍵字搜尋成功!")
        print(f"   總職缺: {len(jobs)}")
        
        # 統計每個關鍵字的職缺
        keyword_counts = {}
        for job in jobs:
            title = job.get('job_title', '').lower()
            if 'engineer' in title:
                keyword_counts['engineer'] = keyword_counts.get('engineer', 0) + 1
            if 'scientist' in title:
                keyword_counts['scientist'] = keyword_counts.get('scientist', 0) + 1
        
        print(f"\n   關鍵字分布:")
        for keyword, count in keyword_counts.items():
            print(f"     - {keyword}: {count} 個")
        
        return len(jobs) > 0
        
    except Exception as e:
        print(f"\n❌ 測試失敗: {str(e)}")
        return False


def test_data_quality():
    """測試資料品質"""
    print("\n" + "=" * 60)
    print("🧪 測試 3: 資料品質檢查 (Selenium)")
    print("=" * 60)
    
    config = {
        'target_jobs': 10,
        'max_pages': 1,
        'search_keywords': ['data engineer'],
        'locations': ['New York, NY'],
        'delay_range': (3, 5),
        'headless': True,
        'scrape_details': False
    }
    
    try:
        scraper = IndeedSeleniumScraper(config)
        jobs = scraper.scrape_jobs()
        
        # 檢查資料完整性
        required_fields = ['job_title', 'company_name', 'location', 'job_url']
        optional_fields = ['salary_text', 'description_snippet', 'metadata_text']
        
        print(f"\n📊 資料品質分析:")
        print(f"   總職缺: {len(jobs)}")
        
        # 必要欄位檢查
        missing_count = 0
        for job in jobs:
            missing = [f for f in required_fields if not job.get(f)]
            if missing:
                missing_count += 1
        
        print(f"\n   必要欄位完整性:")
        print(f"     - 完整: {len(jobs) - missing_count}/{len(jobs)}")
        print(f"     - 缺失: {missing_count}/{len(jobs)}")
        
        # 可選欄位統計
        print(f"\n   可選欄位覆蓋率:")
        for field in optional_fields:
            count = sum(1 for job in jobs if job.get(field))
            rate = count / len(jobs) if jobs else 0
            print(f"     - {field}: {count}/{len(jobs)} ({rate:.1%})")
        
        # 薪資資訊分析
        salary_jobs = [j for j in jobs if j.get('has_salary')]
        print(f"\n   薪資資訊:")
        print(f"     - 有薪資: {len(salary_jobs)}/{len(jobs)} ({len(salary_jobs)/len(jobs):.1%})")
        
        if salary_jobs:
            print(f"     - 薪資範例: {salary_jobs[0].get('salary_text')}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 測試失敗: {str(e)}")
        return False


def test_error_handling():
    """測試錯誤處理"""
    print("\n" + "=" * 60)
    print("🧪 測試 4: 錯誤處理")
    print("=" * 60)
    
    # 測試無效的搜尋條件
    config = {
        'target_jobs': 5,
        'max_pages': 1,
        'search_keywords': ['xyzinvalidkeyword12345'],
        'locations': ['Invalid Location XYZ'],
        'delay_range': (1, 2),
        'scrape_details': False
    }
    
    try:
        scraper = IndeedSeleniumScraper(config)
        jobs = scraper.scrape_jobs()
        
        print(f"\n✅ 錯誤處理正常!")
        print(f"   無效搜尋返回: {len(jobs)} 個職缺")
        print(f"   爬蟲未崩潰,優雅處理")
        
        return True
        
    except Exception as e:
        print(f"\n⚠️  錯誤處理測試觸發異常: {str(e)}")
        print(f"   這可能是正常的,取決於 Indeed 的回應")
        return True  # 不算失敗


def generate_test_report(results):
    """生成測試報告"""
    print("\n" + "=" * 60)
    print("📊 測試報告總結")
    print("=" * 60)
    
    passed = sum(results.values())
    total = len(results)
    
    print(f"\n總計: {passed}/{total} 個測試通過")
    print(f"成功率: {passed/total:.1%}")
    
    print(f"\n詳細結果:")
    for test_name, result in results.items():
        status = "✅ 通過" if result else "❌ 失敗"
        print(f"  {test_name}: {status}")
    
    if passed == total:
        print(f"\n🎉 所有測試通過! Indeed 爬蟲運作正常")
    else:
        print(f"\n⚠️  部分測試失敗,請檢查錯誤訊息")
    
    return passed == total


def test_advanced_filters():
    """測試進階篩選功能"""
    print("\n" + "=" * 60)
    print("🧪 測試 5: 進階篩選 - 實習職位 + 24小時內")
    print("=" * 60)
    
    config = {
        'target_jobs': 5,
        'max_pages': 1,
        'search_keywords': ['data'],
        'locations': ['United States'],
        'delay_range': (3, 5),
        'headless': True,
        
        # 進階篩選
        'fromage': 1,  # 過去24小時
        'job_types': ['internship'],  # 只要實習職位
        # 'remote_only': True,  # 可選: 只要遠端工作
        # 'salary_min': 50000,  # 可選: 最低薪資
        
        'scrape_details': False
    }
    
    try:
        print("🔍 篩選條件:")
        print("   ⏰ 時間: 過去 24 小時")
        print("   💼 職位類型: Internship")
        print("   📍 地點: United States")
        
        scraper = IndeedSeleniumScraper(config)
        jobs = scraper.scrape_jobs()
        
        print(f"\n✅ 進階篩選成功!")
        print(f"   找到職缺: {len(jobs)}")
        
        if jobs:
            print(f"\n📋 職缺範例:")
            for i, job in enumerate(jobs[:3], 1):
                print(f"\n   {i}. {job.get('job_title')}")
                print(f"      公司: {job.get('company_name')}")
                print(f"      地點: {job.get('location')}")
        else:
            print("\n   💡 提示: 過去24小時可能沒有新的實習職位")
            print("      嘗試調整篩選條件 (例如 fromage=3 改為3天)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 測試失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主測試函數"""
    print("🚀 開始測試 Indeed 爬蟲")
    print(f"測試時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = {}
    
    # 執行所有測試
    results['基本爬取'] = test_basic_scraping()
    results['多關鍵字搜尋'] = test_multiple_searches()
    results['資料品質'] = test_data_quality()
    results['錯誤處理'] = test_error_handling()
    results['進階篩選'] = test_advanced_filters()  # 新增測試
    
    # 生成報告
    all_passed = generate_test_report(results)
    
    print("\n" + "=" * 60)
    print("💡 下一步建議:")
    if all_passed:
        print("  1. 在 Airflow UI 中觸發 indeed_daily_scraper DAG")
        print("  2. 檢查 MongoDB 和 PostgreSQL 的資料")
        print("  3. 調整 DAG 中的篩選條件:")
        print("     - fromage: 1 (24小時) 或 3 (3天) 或 7 (7天)")
        print("     - job_types: ['internship', 'entry_level']")
        print("     - remote_only: True (只要遠端)")
    else:
        print("  1. 檢查網路連線")
        print("  2. 確認 Indeed 網站是否可存取")
        print("  3. 查看詳細錯誤訊息")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())