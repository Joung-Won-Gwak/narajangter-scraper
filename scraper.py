"""
나라장터 정보시스템 감리 공고 스크래퍼
Firecrawl을 사용하여 나라장터에서 '정보시스템 감리' 공고를 수집하고 PostgreSQL에 저장합니다.
"""

import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

import psycopg2
from psycopg2.extras import execute_values
from firecrawl import FirecrawlApp

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgreSQLConnector:
    """PostgreSQL 데이터베이스 연결 및 조작 클래스"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "postgres"
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """데이터베이스 연결"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            logger.info("PostgreSQL 데이터베이스에 연결되었습니다.")
        except psycopg2.Error as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            raise
    
    def disconnect(self):
        """데이터베이스 연결 해제"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("데이터베이스 연결이 종료되었습니다.")
    
    def create_tables(self):
        """필요한 테이블 생성"""
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
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_audit_notices_title ON audit_notices(title);
        CREATE INDEX IF NOT EXISTS idx_audit_notices_publish_date ON audit_notices(publish_date);
        CREATE INDEX IF NOT EXISTS idx_audit_notices_organization ON audit_notices(organization);
        """
        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info("테이블이 성공적으로 생성되었습니다.")
        except psycopg2.Error as e:
            logger.error(f"테이블 생성 실패: {e}")
            self.connection.rollback()
            raise
    
    def insert_notice(self, notice_data: Dict[str, Any]) -> bool:
        """공고 데이터 삽입 또는 업데이트"""
        upsert_sql = """
        INSERT INTO audit_notices (
            notice_id, title, organization, publish_date, deadline_date,
            estimated_price, contract_method, notice_url, detail_content, raw_data
        ) VALUES (
            %(notice_id)s, %(title)s, %(organization)s, %(publish_date)s, %(deadline_date)s,
            %(estimated_price)s, %(contract_method)s, %(notice_url)s, %(detail_content)s, %(raw_data)s
        )
        ON CONFLICT (notice_id) DO UPDATE SET
            title = EXCLUDED.title,
            organization = EXCLUDED.organization,
            publish_date = EXCLUDED.publish_date,
            deadline_date = EXCLUDED.deadline_date,
            estimated_price = EXCLUDED.estimated_price,
            contract_method = EXCLUDED.contract_method,
            notice_url = EXCLUDED.notice_url,
            detail_content = EXCLUDED.detail_content,
            raw_data = EXCLUDED.raw_data,
            updated_at = CURRENT_TIMESTAMP
        """
        try:
            self.cursor.execute(upsert_sql, notice_data)
            self.connection.commit()
            return True
        except psycopg2.Error as e:
            logger.error(f"데이터 삽입 실패: {e}")
            self.connection.rollback()
            return False
    
    def insert_notices_batch(self, notices: List[Dict[str, Any]]) -> int:
        """여러 공고 데이터 일괄 삽입"""
        inserted_count = 0
        for notice in notices:
            if self.insert_notice(notice):
                inserted_count += 1
        return inserted_count


class NarajangterScraper:
    """나라장터 정보시스템 감리 공고 스크래퍼"""
    
    # 나라장터 입찰공고 검색 URL (정보시스템 감리 키워드 포함)
    SEARCH_URL = "https://www.g2b.go.kr:8081/ep/tbid/tbidList.do?taskClCds=&bidNm=정보시스템+감리&searchDtType=1&fromBidDt=&toBidDt=&fromOpenBidDt=&toOpenBidDt=&radOrgan=1&instNm=&area=&regYn=Y&bidSearchType=1&searchType=1"
    
    def __init__(self, firecrawl_api_key: Optional[str] = None):
        """
        Args:
            firecrawl_api_key: Firecrawl API 키. 없으면 환경변수 FIRECRAWL_API_KEY 사용
        """
        self.api_key = firecrawl_api_key or os.getenv("FIRECRAWL_API_KEY")
        if not self.api_key:
            raise ValueError("Firecrawl API 키가 필요합니다. FIRECRAWL_API_KEY 환경변수를 설정하세요.")
        
        self.firecrawl = FirecrawlApp(api_key=self.api_key)
        logger.info("Firecrawl 클라이언트가 초기화되었습니다.")
    
    def scrape_notice_list(self) -> Dict[str, Any]:
        """공고 목록 페이지 스크래핑"""
        logger.info(f"공고 목록 스크래핑 시작: {self.SEARCH_URL}")
        
        try:
            result = self.firecrawl.scrape(
                self.SEARCH_URL,
                formats=['markdown', 'html'],
                wait_for=3000,  # 동적 콘텐츠 로딩 대기
            )
            logger.info("공고 목록 스크래핑 완료")
            return result
        except Exception as e:
            logger.error(f"공고 목록 스크래핑 실패: {e}")
            raise
    
    def scrape_notice_detail(self, notice_url: str) -> Dict[str, Any]:
        """개별 공고 상세 페이지 스크래핑"""
        logger.info(f"공고 상세 스크래핑: {notice_url}")
        
        try:
            result = self.firecrawl.scrape(
                notice_url,
                formats=['markdown', 'html'],
                wait_for=2000,
            )
            return result
        except Exception as e:
            logger.error(f"공고 상세 스크래핑 실패: {e}")
            return {}
    
    def crawl_notices(self, max_pages: int = 5) -> List[Dict[str, Any]]:
        """
        여러 페이지의 공고를 크롤링
        
        Args:
            max_pages: 크롤링할 최대 페이지 수
        
        Returns:
            스크래핑된 공고 데이터 리스트
        """
        logger.info(f"크롤링 시작 (최대 {max_pages} 페이지)")
        
        try:
            # Firecrawl v4.x에서는 scrape 메서드 사용
            result = self.firecrawl.scrape(
                self.SEARCH_URL,
                formats=['markdown', 'html'],
                wait_for=5000,  # 동적 콘텐츠 로딩 대기
            )
            
            if result:
                logger.info(f"스크래핑 완료: 1개 페이지 수집")
                return [result]  # 단일 결과를 리스트로 반환
            return []
            
        except Exception as e:
            logger.error(f"크롤링 실패: {e}")
            return []
    
    def parse_notice_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        스크래핑된 원본 데이터를 정형화된 형식으로 변환
        
        Args:
            raw_data: Firecrawl에서 반환된 원본 데이터
        
        Returns:
            정형화된 공고 데이터
        """
        # 기본 데이터 구조
        parsed = {
            'notice_id': None,
            'title': None,
            'organization': None,
            'publish_date': None,
            'deadline_date': None,
            'estimated_price': None,
            'contract_method': None,
            'notice_url': raw_data.get('metadata', {}).get('sourceURL', ''),
            'detail_content': raw_data.get('markdown', ''),
            'raw_data': json.dumps(raw_data, ensure_ascii=False)
        }
        
        # 메타데이터에서 제목 추출
        metadata = raw_data.get('metadata', {})
        parsed['title'] = metadata.get('title', '')
        
        # 마크다운 내용에서 추가 정보 추출 시도
        markdown_content = raw_data.get('markdown', '')
        
        # 공고번호 추출 (패턴: 20XXXXXX-XXXX 형식)
        import re
        notice_id_pattern = r'(\d{8,}-\d+)'
        notice_id_match = re.search(notice_id_pattern, markdown_content)
        if notice_id_match:
            parsed['notice_id'] = notice_id_match.group(1)
        else:
            # URL에서 추출 시도
            url = parsed['notice_url']
            if 'bidno=' in url.lower():
                bid_match = re.search(r'bidno=([^&]+)', url, re.IGNORECASE)
                if bid_match:
                    parsed['notice_id'] = bid_match.group(1)
        
        return parsed


class NarajangterPipeline:
    """전체 파이프라인 관리"""
    
    def __init__(
        self,
        firecrawl_api_key: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        self.scraper = NarajangterScraper(firecrawl_api_key)
        
        # 데이터베이스 설정
        default_db_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "database": os.getenv("POSTGRES_DB", "narajangter"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "")
        }
        
        if db_config:
            default_db_config.update(db_config)
        
        self.db = PostgreSQLConnector(**default_db_config)
    
    def run(self, max_pages: int = 5) -> Dict[str, Any]:
        """
        전체 파이프라인 실행
        
        Args:
            max_pages: 크롤링할 최대 페이지 수
        
        Returns:
            실행 결과 요약
        """
        result = {
            "success": False,
            "scraped_count": 0,
            "inserted_count": 0,
            "errors": []
        }
        
        try:
            # 데이터베이스 연결
            self.db.connect()
            self.db.create_tables()
            
            # 공고 목록 스크래핑
            scraped_data = self.scraper.crawl_notices(max_pages)
            result["scraped_count"] = len(scraped_data)
            
            # 데이터 파싱 및 저장
            for raw_notice in scraped_data:
                try:
                    parsed_notice = self.scraper.parse_notice_data(raw_notice)
                    if parsed_notice.get('notice_id') or parsed_notice.get('title'):
                        if self.db.insert_notice(parsed_notice):
                            result["inserted_count"] += 1
                except Exception as e:
                    error_msg = f"공고 처리 중 오류: {e}"
                    logger.warning(error_msg)
                    result["errors"].append(error_msg)
            
            result["success"] = True
            logger.info(f"파이프라인 완료: {result['inserted_count']}/{result['scraped_count']}개 저장")
            
        except Exception as e:
            error_msg = f"파이프라인 실행 실패: {e}"
            logger.error(error_msg)
            result["errors"].append(error_msg)
        
        finally:
            self.db.disconnect()
        
        return result


def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='나라장터 정보시스템 감리 공고 스크래퍼')
    parser.add_argument(
        '--max-pages',
        type=int,
        default=5,
        help='크롤링할 최대 페이지 수 (기본값: 5)'
    )
    parser.add_argument(
        '--db-host',
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help='PostgreSQL 호스트'
    )
    parser.add_argument(
        '--db-port',
        type=int,
        default=int(os.getenv("POSTGRES_PORT", 5432)),
        help='PostgreSQL 포트'
    )
    parser.add_argument(
        '--db-name',
        default=os.getenv("POSTGRES_DB", "narajangter"),
        help='데이터베이스 이름'
    )
    parser.add_argument(
        '--db-user',
        default=os.getenv("POSTGRES_USER", "postgres"),
        help='데이터베이스 사용자'
    )
    parser.add_argument(
        '--db-password',
        default=os.getenv("POSTGRES_PASSWORD", ""),
        help='데이터베이스 비밀번호'
    )
    
    args = parser.parse_args()
    
    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password
    }
    
    # 파이프라인 실행
    pipeline = NarajangterPipeline(db_config=db_config)
    result = pipeline.run(max_pages=args.max_pages)
    
    # 결과 출력
    print("\n" + "="*50)
    print("실행 결과")
    print("="*50)
    print(f"성공 여부: {'성공' if result['success'] else '실패'}")
    print(f"스크래핑된 공고 수: {result['scraped_count']}")
    print(f"저장된 공고 수: {result['inserted_count']}")
    if result['errors']:
        print(f"오류 수: {len(result['errors'])}")
        for error in result['errors'][:5]:  # 최대 5개 오류 출력
            print(f"  - {error}")


if __name__ == "__main__":
    main()
