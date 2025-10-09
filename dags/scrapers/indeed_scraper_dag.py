# dags/scrapers/indeed_scraper_dag.py
# Indeed 職缺每日爬蟲 DAG

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import json

# 加入 src 路徑
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

# ============================================================================
# DAG 配置
# ============================================================================

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'indeed_daily_scraper',
    default_args=default_args,
    description='Indeed 職缺每日爬蟲 - 完整數據收集流程',
    schedule='0 2 * * *',  # 每天凌晨 2 點執行 (台灣時間 10 點)
    max_active_runs=1,
    catchup=False,
    tags=['scraper', 'indeed', 'daily', 'jobs']
)

# ============================================================================
# Task 函數
# ============================================================================

def setup_scraper_config(**context):
    """設定今日爬蟲配置"""
    execution_date = context['ds']
    batch_id = f"indeed_{execution_date.replace('-', '')}"
    
    config = {
        'batch_id': batch_id,
        'execution_date': execution_date,
        
        # 爬取目標
        'target_jobs': 5,
        'max_pages': 1,
        
        # 搜尋條件
        'search_keywords': [
            'data engineer',
            # 'data scientist',
            # 'machine learning engineer',
            # 'data analyst',
            # 'analytics engineer',
            'backend engineer data'
        ],
        # 'locations': [],  # 不設定 = 不限地區
        # 或者設定為空列表/空字串也可以
        'locations': [''],  # 空字串 = 不限地區 (Indeed 會顯示所有地區)
        
        # 進階篩選
        'fromage': 3,  # 過去3天
        'job_types': ['entry_level', 'mid_level'],  # 入門和中階
        # 'remote_only': False,  # 不限遠端
        
        # 反爬設定
        'delay_range': (4, 7),  # Selenium 可以稍快一點
        'headless': False,  # 背景執行
        'request_timeout': 30,
        'max_retries': 3,
        'scrape_details': False,
        
        # 品質控制
        'min_required_fields': ['job_title', 'company_name', 'job_url'],
        'skip_duplicates': True
    }
    
    print(f"✅ 爬蟲配置已生成:")
    print(f"   批次 ID: {config['batch_id']}")
    print(f"   目標職缺: {config['target_jobs']}")
    print(f"   搜尋關鍵字: {len(config['search_keywords'])} 個")
    
    # 地區資訊 (可選)
    locations = config.get('locations', [''])
    if locations and locations[0]:
        print(f"   目標城市: {len(locations)} 個 - {', '.join(locations[:3])}")
    else:
        print(f"   目標城市: 不限地區 (全美國)")
    
    print(f"   時間範圍: 過去 {config['fromage']} 天")
    print(f"   職位類型: {config['job_types']}")
    
    context['task_instance'].xcom_push(key='scraper_config', value=config)
    return f"Config ready for batch {config['batch_id']}"


def scrape_indeed_jobs(**context):
    """執行 Indeed 職缺爬蟲 - 使用 Selenium 版本"""
    from scrapers.indeed_scraper import IndeedSeleniumScraper
    
    # 取得配置
    config = context['task_instance'].xcom_pull(
        task_ids='setup_scraper_config',
        key='scraper_config'
    )
    
    print(f"🚀 開始爬取 Indeed 職缺 (Selenium)...")
    print(f"   批次 ID: {config['batch_id']}")
    print(f"   模式: 無頭瀏覽器")
    
    try:
        scraper = IndeedSeleniumScraper(config)
        jobs_data = scraper.scrape_jobs()
        
        # 統計結果
        total_jobs = len(jobs_data)
        success_rate = scraper.get_success_rate()
        
        print(f"✅ 爬取完成:")
        print(f"   總計職缺: {total_jobs}")
        print(f"   成功率: {success_rate:.1%}")
        
        # 儲存結果到 XCom
        result = {
            'batch_id': config['batch_id'],
            'jobs_data': jobs_data,
            'total_jobs': total_jobs,
            'success_rate': success_rate,
            'scrape_timestamp': datetime.now().isoformat(),
            'scraper_type': 'selenium'
        }
        
        context['task_instance'].xcom_push(key='scrape_result', value=result)
        return f"Successfully scraped {total_jobs} jobs"
        
    except Exception as e:
        print(f"❌ 爬取失敗: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def validate_scraped_data(**context):
    """驗證爬取資料品質"""
    scrape_result = context['task_instance'].xcom_pull(
        task_ids='scrape_indeed_jobs',
        key='scrape_result'
    )
    
    if not scrape_result or not scrape_result.get('jobs_data'):
        raise ValueError("No scraped data found")
    
    jobs_data = scrape_result['jobs_data']
    
    print(f"🔍 開始驗證 {len(jobs_data)} 筆職缺資料...")
    
    validation_results = {
        'total_jobs': len(jobs_data),
        'valid_jobs': 0,
        'invalid_jobs': 0,
        'completeness_scores': [],
        'quality_flags': []
    }
    
    valid_jobs = []
    
    for i, job in enumerate(jobs_data):
        # 檢查必要欄位
        required_fields = ['job_title', 'company_name', 'location', 'job_url']
        missing_fields = [field for field in required_fields if not job.get(field)]
        
        # 計算完整性分數
        all_fields = required_fields + ['salary_text', 'description_snippet', 'metadata_text']
        filled_fields = sum(1 for field in all_fields if job.get(field))
        completeness_score = filled_fields / len(all_fields)
        
        validation_results['completeness_scores'].append(completeness_score)
        
        if missing_fields:
            validation_results['invalid_jobs'] += 1
            validation_results['quality_flags'].append({
                'job_index': i,
                'missing_fields': missing_fields,
                'completeness_score': completeness_score
            })
        else:
            validation_results['valid_jobs'] += 1
            job['completeness_score'] = completeness_score
            valid_jobs.append(job)
    
    avg_completeness = sum(validation_results['completeness_scores']) / len(validation_results['completeness_scores'])
    validation_results['average_completeness'] = avg_completeness
    
    print(f"✅ 資料驗證完成:")
    print(f"   有效職缺: {validation_results['valid_jobs']}")
    print(f"   無效職缺: {validation_results['invalid_jobs']}")
    print(f"   平均完整性: {avg_completeness:.2%}")
    
    # 更新結果
    validated_result = scrape_result.copy()
    validated_result['jobs_data'] = valid_jobs
    validated_result['validation_results'] = validation_results
    
    context['task_instance'].xcom_push(key='validated_data', value=validated_result)
    
    return f"Validated {validation_results['valid_jobs']} valid jobs"


def store_to_mongodb(**context):
    """儲存資料到 MongoDB Atlas"""
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
    import os
    
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_scraped_data',
        key='validated_data'
    )
    
    if not validated_data or not validated_data.get('jobs_data'):
        print("⚠️  沒有有效資料需要儲存")
        return "No data to store"
    
    jobs_data = validated_data['jobs_data']
    batch_id = validated_data['batch_id']
    
    print(f"💾 開始儲存 {len(jobs_data)} 筆資料到 MongoDB Atlas...")
    
    try:
        mongodb_url = os.getenv('MONGODB_ATLAS_URL')
        db_name = os.getenv('MONGODB_ATLAS_DB_NAME', 'job_market_data')
        
        client = MongoClient(mongodb_url, server_api=ServerApi('1'))
        db = client[db_name]
        collection = db['raw_jobs_data']
        
        # 批次插入/更新資料
        operations = []
        for job in jobs_data:
            document = {
                'source': 'indeed',
                'job_data': job,
                'metadata': {
                    'scraped_at': datetime.utcnow(),  # 使用 Date 物件
                    'batch_id': batch_id,
                    'scraper_version': '1.0.0-indeed',
                    'source_url': job.get('job_url', '')
                },
                'data_quality': {
                    'completeness_score': job.get('completeness_score', 0),
                    'flags': []
                }
            }
            
            filter_condition = {
                'job_data.job_url': job.get('job_url'),
                'source': 'indeed'
            }
            
            operations.append({
                'filter': filter_condition,
                'document': document,
                'upsert': True
            })
        
        # 執行批次 upsert
        if operations:
            results = []
            for op in operations:
                result = collection.replace_one(
                    op['filter'],
                    op['document'],
                    upsert=op['upsert']
                )
                results.append(result)
            
            inserted_count = sum(1 for r in results if r.upserted_id)
            updated_count = sum(1 for r in results if r.modified_count > 0)
            
            print(f"✅ MongoDB 儲存完成:")
            print(f"   新增: {inserted_count} 筆")
            print(f"   更新: {updated_count} 筆")
            
            storage_stats = {
                'mongodb_inserted': inserted_count,
                'mongodb_updated': updated_count,
                'mongodb_total': len(operations)
            }
            
            context['task_instance'].xcom_push(key='mongodb_stats', value=storage_stats)
            client.close()
            
            return f"Stored {len(operations)} jobs to MongoDB"
        
    except Exception as e:
        print(f"❌ MongoDB 儲存失敗: {str(e)}")
        raise


def store_to_postgres_raw(**context):
    """儲存資料到 PostgreSQL Raw Staging"""
    import psycopg2
    import os
    
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_scraped_data',
        key='validated_data'
    )
    
    if not validated_data or not validated_data.get('jobs_data'):
        print("⚠️  沒有有效資料需要儲存")
        return "No data to store"
    
    jobs_data = validated_data['jobs_data']
    batch_id = validated_data['batch_id']
    
    print(f"🐘 開始儲存 {len(jobs_data)} 筆資料到 PostgreSQL Raw Staging...")
    
    try:
        supabase_url = os.getenv('SUPABASE_DB_URL')
        
        conn = psycopg2.connect(supabase_url)
        cur = conn.cursor()
        
        # 準備插入資料
        insert_sql = """
        INSERT INTO raw_staging.indeed_jobs_raw (
            source_job_id, source_url, job_title, company_name,
            location_raw, job_description, salary_range, 
            employment_type, work_arrangement, raw_json, 
            batch_id, scraped_at
        ) VALUES (
            %(source_job_id)s, %(source_url)s, %(job_title)s, %(company_name)s,
            %(location_raw)s, %(job_description)s, %(salary_range)s,
            %(employment_type)s, %(work_arrangement)s, %(raw_json)s,
            %(batch_id)s, %(scraped_at)s
        ) ON CONFLICT (source_job_id, batch_id) DO UPDATE SET
            job_title = EXCLUDED.job_title,
            company_name = EXCLUDED.company_name,
            location_raw = EXCLUDED.location_raw,
            job_description = EXCLUDED.job_description,
            salary_range = EXCLUDED.salary_range,
            raw_json = EXCLUDED.raw_json,
            scraped_at = EXCLUDED.scraped_at
        """
        
        inserted_count = 0
        for job in jobs_data:
            # Indeed job_id
            job_id = job.get('job_id', f"indeed_{batch_id}_{inserted_count}")
            
            row_data = {
                'source_job_id': job_id,
                'source_url': job.get('job_url', ''),
                'job_title': job.get('job_title', ''),
                'company_name': job.get('company_name', ''),
                'location_raw': job.get('location', ''),
                'job_description': job.get('description_snippet', ''),
                'salary_range': job.get('salary_text', ''),
                'employment_type': job.get('metadata_text', ''),
                'work_arrangement': 'unknown',  # Indeed 列表頁通常沒這個資訊
                'raw_json': json.dumps(job),
                'batch_id': batch_id,
                'scraped_at': datetime.now()
            }
            
            cur.execute(insert_sql, row_data)
            inserted_count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ PostgreSQL 儲存完成: {inserted_count} 筆")
        
        storage_stats = {
            'postgres_inserted': inserted_count
        }
        
        context['task_instance'].xcom_push(key='postgres_stats', value=storage_stats)
        
        return f"Stored {inserted_count} jobs to PostgreSQL"
        
    except Exception as e:
        print(f"❌ PostgreSQL 儲存失敗: {str(e)}")
        raise


def log_scraping_metrics(**context):
    """記錄爬取指標和統計"""
    
    # 收集所有 Task 的執行結果
    scrape_result = context['task_instance'].xcom_pull(
        task_ids='scrape_indeed_jobs',
        key='scrape_result'
    ) or {}
    
    validated_data = context['task_instance'].xcom_pull(
        task_ids='validate_scraped_data',
        key='validated_data'
    ) or {}
    
    mongodb_stats = context['task_instance'].xcom_pull(
        task_ids='store_to_mongodb',
        key='mongodb_stats'
    ) or {}
    
    postgres_stats = context['task_instance'].xcom_pull(
        task_ids='store_to_postgres_raw',
        key='postgres_stats'
    ) or {}
    
    # 編譯完整的執行報告
    execution_report = {
        'dag_id': context['dag'].dag_id,
        'execution_date': context['ds'],
        'batch_id': scrape_result.get('batch_id', 'unknown'),
        'platform': 'indeed',
        
        # 爬取統計
        'scraping': {
            'total_scraped': scrape_result.get('total_jobs', 0),
            'success_rate': scrape_result.get('success_rate', 0),
            'scrape_timestamp': scrape_result.get('scrape_timestamp')
        },
        
        # 驗證統計
        'validation': validated_data.get('validation_results', {}),
        
        # 儲存統計
        'storage': {
            'mongodb': mongodb_stats,
            'postgresql': postgres_stats
        },
        
        # 執行時間
        'execution_time': {
            'start_time': context['task_instance'].start_date.isoformat() if context['task_instance'].start_date else None,
            'end_time': datetime.now().isoformat()
        }
    }
    
    print(f"📊 Indeed 爬取執行報告:")
    print(f"=" * 50)
    print(f"批次 ID: {execution_report['batch_id']}")
    print(f"執行日期: {execution_report['execution_date']}")
    print(f"平台: Indeed")
    print(f"爬取職缺: {execution_report['scraping']['total_scraped']}")
    print(f"成功率: {execution_report['scraping']['success_rate']:.1%}")
    print(f"有效職缺: {execution_report['validation'].get('valid_jobs', 0)}")
    print(f"MongoDB 儲存: {execution_report['storage']['mongodb'].get('mongodb_total', 0)}")
    print(f"PostgreSQL 儲存: {execution_report['storage']['postgresql'].get('postgres_inserted', 0)}")
    print(f"=" * 50)
    
    # 儲存報告
    context['task_instance'].xcom_push(key='execution_report', value=execution_report)
    
    return "Metrics logged successfully"


# ============================================================================
# Task 定義
# ============================================================================

# Task 1: 設定爬蟲配置
setup_config_task = PythonOperator(
    task_id='setup_scraper_config',
    python_callable=setup_scraper_config,
    dag=dag
)

# Task 2: 執行爬蟲
scrape_task = PythonOperator(
    task_id='scrape_indeed_jobs',
    python_callable=scrape_indeed_jobs,
    dag=dag
)

# Task 3: 驗證資料
validate_task = PythonOperator(
    task_id='validate_scraped_data',
    python_callable=validate_scraped_data,
    dag=dag
)

# Task 4: 儲存到 MongoDB
mongodb_task = PythonOperator(
    task_id='store_to_mongodb',
    python_callable=store_to_mongodb,
    dag=dag
)

# Task 5: 儲存到 PostgreSQL
postgres_task = PythonOperator(
    task_id='store_to_postgres_raw',
    python_callable=store_to_postgres_raw,
    dag=dag
)

# Task 6: 記錄指標
metrics_task = PythonOperator(
    task_id='log_scraping_metrics',
    python_callable=log_scraping_metrics,
    dag=dag
)

# ============================================================================
# Task 依賴關係
# ============================================================================

# 線性執行流程
setup_config_task >> scrape_task >> validate_task

# 並行儲存 (MongoDB 和 PostgreSQL 可同時進行)
validate_task >> [mongodb_task, postgres_task]

# 最終指標記錄 (等待所有儲存完成)
[mongodb_task, postgres_task] >> metrics_task