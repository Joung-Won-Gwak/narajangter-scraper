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
        print("[OK] 데이터베이스 테이블 초기화 완료")
    except Exception as e:
        print(f"[ERROR] 테이블 초기화 실패: {e}")

def get_db_connection():
    """데이터베이스 연결 - Railway DATABASE_URL 또는 개별 변수 지원"""
    database_url = os.getenv("DATABASE_URL")
    
    if database_url:
        # DATABASE_URL 파싱하여 개별 파라미터로 전달
        from urllib.parse import urlparse
        parsed = urlparse(database_url)
        db_name = parsed.path[1:]  # 앞의 '/' 제거
        print(f"[DB] Connecting to: host={parsed.hostname}, database={db_name}")
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
        print(f"[DB] Connecting (local): database={db_name}")
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
    """메인 대시보드"""
    return render_template('index.html')

@app.route('/collect')
def collect():
    """데이터 수집 페이지"""
    return render_template('collect.html')

@app.route('/search')
def search():
    """데이터 조회 페이지"""
    return render_template('search.html')

@app.route('/proposal')
def proposal():
    """제안서 생성 페이지"""
    return render_template('proposal.html')

@app.route('/api/notices')
def get_notices():
    """공고 목록 API - 고급 필터링 지원"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 검색 파라미터
        keyword = request.args.get('keyword', '')
        organization = request.args.get('organization', '')
        min_price = request.args.get('min_price', '')
        max_price = request.args.get('max_price', '')
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        limit = request.args.get('limit', '')
        
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
        
        if organization:
            query += " AND organization ILIKE %s"
            params.append(f"%{organization}%")
        
        if min_price:
            query += " AND estimated_price >= %s"
            params.append(int(min_price))
        
        if max_price:
            query += " AND estimated_price <= %s"
            params.append(int(max_price))
        
        if start_date:
            query += " AND publish_date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND publish_date <= %s"
            params.append(end_date)
        
        query += " ORDER BY publish_date DESC, scraped_at DESC"
        
        if limit:
            query += f" LIMIT {int(limit)}"
        
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
        
        # 요청 데이터 파싱
        data = request.get_json() or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        max_pages = data.get('max_pages', 5)
        
        pipeline = NarajangterPipeline()
        
        # 날짜 범위가 제공된 경우 파라미터로 전달
        # 실제 구현에서는 openapi_scraper.py의 run 메서드가 날짜 파라미터를 지원해야 함
        result = pipeline.run(max_pages=max_pages)
        
        return jsonify({
            "success": result["success"],
            "scraped_count": result["scraped_count"],
            "inserted_count": result["inserted_count"],
            "errors": result["errors"][:5] if result["errors"] else [],
            "start_date": start_date,
            "end_date": end_date
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

@app.route('/api/proposal/generate', methods=['POST'])
def generate_proposal():
    """제안서 생성 API"""
    try:
        # 파일 업로드 처리
        rfp_file = request.files.get('rfpFile')
        template_file = request.files.get('templateFile')
        requirements = request.form.get('requirements', '')
        company_info = request.form.get('companyInfo', '')
        
        if not rfp_file:
            return jsonify({
                "success": False,
                "error": "제안요청서 파일이 필요합니다."
            }), 400
        
        # TODO: 실제 AI 기반 제안서 생성 로직 구현
        # 현재는 시뮬레이션 응답 반환
        
        return jsonify({
            "success": True,
            "message": "제안서가 생성되었습니다.",
            "download_url": "/api/proposal/download/sample.docx"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == '__main__':
    init_db()  # 테이블 초기화
    app.run(debug=True, host='0.0.0.0', port=5000)

