"""
나라장터 입찰공고 뷰어 웹 서버
Flask를 사용하여 수집된 공고 데이터를 웹 페이지로 표시합니다.
"""

import os
from flask import Flask, render_template, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, template_folder='templates', static_folder='static')

def init_db():
    """데이터베이스 테이블 초기화"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 기존 테이블 삭제 (스키마 변경 적용을 위해)
        print("🗑️  기존 테이블 삭제 중...")
        cur.execute("DROP TABLE IF EXISTS audit_notices;")
        conn.commit()
        print("✅ 기존 테이블 삭제 완료")
        
        # 새 테이블 생성
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS audit_notices (
            id SERIAL PRIMARY KEY,
            notice_id VARCHAR(100) UNIQUE,
            title VARCHAR(500) NOT NULL,
            organization VARCHAR(200),
            publish_date DATE,
            deadline_date TIMESTAMP,
            estimated_price BIGINT,
            contract_method VARCHAR(100),
            notice_url TEXT,
            detail_content TEXT,
            raw_data JSONB,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ 데이터베이스 테이블 초기화 완료")
    except Exception as e:
        print(f"❌ 테이블 초기화 실패: {e}")

def get_db_connection():
    """데이터베이스 연결 - Railway DATABASE_URL 또는 개별 변수 지원"""
    database_url = os.getenv("DATABASE_URL")
    
    if database_url:
        # DATABASE_URL 파싱하여 개별 파라미터로 전달
        from urllib.parse import urlparse
        parsed = urlparse(database_url)
        db_name = parsed.path[1:]  # 앞의 '/' 제거
        print(f"🔗 DB 연결: host={parsed.hostname}, database={db_name}")
        return psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=db_name,
            user=parsed.username,
            password=parsed.password,
            cursor_factory=RealDictCursor
        )
    else:
        # 개별 환경변수 사용 (로컬 개발용)
        db_name = os.getenv("POSTGRES_DB", "railway")
        print(f"🔗 DB 연결 (로컬): database={db_name}")
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=db_name,
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            cursor_factory=RealDictCursor
        )

# 앱 시작 시 테이블 초기화 (첫 요청 전)
with app.app_context():
    init_db()

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')

@app.route('/api/notices')
def get_notices():
    """공고 목록 API"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 검색 파라미터
        keyword = request.args.get('keyword', '')
        
        query = """
            SELECT 
                notice_id,
                title,
                organization,
                publish_date,
                deadline_date,
                estimated_price,
                contract_method,
                notice_url,
                scraped_at
            FROM audit_notices
            WHERE 1=1
        """
        params = []
        
        if keyword:
            query += " AND title ILIKE %s"
            params.append(f"%{keyword}%")
        
        query += " ORDER BY publish_date DESC, scraped_at DESC"
        
        cur.execute(query, params)
        notices = cur.fetchall()
        
        # 날짜 및 금액 포맷팅
        result = []
        for notice in notices:
            item = dict(notice)
            if item['publish_date']:
                item['publish_date'] = str(item['publish_date'])
            if item['deadline_date']:
                item['deadline_date'] = str(item['deadline_date'])
            if item['scraped_at']:
                item['scraped_at'] = str(item['scraped_at'])
            if item['estimated_price']:
                item['estimated_price_formatted'] = f"{item['estimated_price']:,}원"
            else:
                item['estimated_price_formatted'] = '-'
            result.append(item)
        
        cur.close()
        conn.close()
        
        return jsonify({
            "success": True,
            "count": len(result),
            "data": result
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "data": []
        }), 500

@app.route('/api/stats')
def get_stats():
    """통계 API"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) as total FROM audit_notices")
        total = cur.fetchone()['total']
        
        cur.execute("""
            SELECT organization, COUNT(*) as count 
            FROM audit_notices 
            GROUP BY organization 
            ORDER BY count DESC 
            LIMIT 5
        """)
        top_orgs = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            "success": True,
            "total_notices": total,
            "top_organizations": [dict(o) for o in top_orgs]
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/scrape', methods=['POST'])
def run_scraper():
    """공공데이터포털에서 데이터 수집 실행"""
    try:
        import sys
        import traceback
        from openapi_scraper import NarajangterPipeline
        
        pipeline = NarajangterPipeline()
        result = pipeline.run(max_pages=5)
        
        return jsonify({
            "success": result["success"],
            "scraped_count": result["scraped_count"],
            "inserted_count": result["inserted_count"],
            "errors": result["errors"][:5] if result["errors"] else []
        })
        
    except Exception as e:
        # 상세 에러 로그 출력
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "scraped_count": 0,
            "inserted_count": 0
        }), 500


if __name__ == '__main__':
    init_db()  # 테이블 초기화
    app.run(debug=True, host='0.0.0.0', port=5000)

