"""
나라장터 정보시스템 감리 공고 스크래퍼 (공공데이터포털 Open API 버전)
공공데이터포털 API를 사용하여 나라장터에서 '정보시스템 감리' 공고를 수집하고 PostgreSQL에 저장합니다.
"""

import os
import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from urllib.parse import quote

from dotenv import load_dotenv
import psycopg2

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgreSQLConnector:
    """PostgreSQL 데이터베이스 연결 및 조작 클래스"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        self.connection_params = None
        
        if self.database_url:
            # DATABASE_URL 파싱
            from urllib.parse import urlparse
            parsed = urlparse(self.database_url)
            self.connection_params = {
                "host": parsed.hostname,
                "port": parsed.port or 5432,
                "database": parsed.path[1:],  # 앞의 '/' 제거
                "user": parsed.username,
                "password": parsed.password
            }
        else:
            # 로컬 개발용 기본값
            self.connection_params = {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "database": os.getenv("POSTGRES_DB", "railway"),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD", "postgres")
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


class NarajangterOpenAPI:
    """나라장터 공공데이터포털 Open API 클라이언트"""
    
    # 입찰공고정보서비스 Base URL
    BASE_URL = "https://apis.data.go.kr/1230000/ad/BidPublicInfoService"
    
    def __init__(self, service_key: Optional[str] = None):
        """
        Args:
            service_key: 공공데이터포털 서비스 키. 없으면 환경변수 DATA_GO_KR_SERVICE_KEY 사용
        """
        self.service_key = service_key or os.getenv("DATA_GO_KR_SERVICE_KEY")
        if not self.service_key:
            raise ValueError("서비스 키가 필요합니다. DATA_GO_KR_SERVICE_KEY 환경변수를 설정하세요.")
        
        logger.info("공공데이터포털 API 클라이언트가 초기화되었습니다.")
    
    def search_bid_notices(
        self,
        keyword: str = "정보시스템 감리",
        page_no: int = 1,
        num_of_rows: int = 100,
        days_back: int = 30
    ) -> Dict[str, Any]:
        """
        입찰공고 검색
        
        Args:
            keyword: 검색 키워드
            page_no: 페이지 번호
            num_of_rows: 한 페이지 결과 수 (최대 999)
            days_back: 검색할 과거 일수 (최대 31일)
        
        Returns:
            API 응답 데이터
        """
        # 용역 입찰공고 목록 조회 엔드포인트
        endpoint = f"{self.BASE_URL}/getBidPblancListInfoServcPPSSrch"
        
        # 날짜 범위 설정
        end_date = datetime.now()
        start_date = end_date - timedelta(days=min(days_back, 31))  # 최대 1개월
        
        params = {
            "serviceKey": self.service_key,
            "numOfRows": str(num_of_rows),
            "pageNo": str(page_no),
            "inqryDiv": "1",  # 조회구분: 공고게시일시
            "inqryBgnDt": start_date.strftime("%Y%m%d") + "0000",  # 조회시작일시
            "inqryEndDt": end_date.strftime("%Y%m%d") + "2359",  # 조회종료일시
            "type": "json",
            "indstrytyCd": "6146",  # 정보시스템 감리법인
        }
        
        logger.info(f"입찰공고 검색: 키워드='{keyword}', 페이지={page_no}")
        
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # 응답 구조 확인
            if 'response' in data:
                header = data['response'].get('header', {})
                result_code = header.get('resultCode', '')
                
                if result_code == '00':
                    body = data['response'].get('body', {})
                    items = body.get('items', [])
                    total_count = body.get('totalCount', 0)
                    
                    logger.info(f"검색 완료: 총 {total_count}건, 현재 페이지 {len(items) if items else 0}건")
                    return {
                        "success": True,
                        "total_count": total_count,
                        "items": items if items else [],
                        "page_no": page_no,
                        "num_of_rows": num_of_rows
                    }
                else:
                    error_msg = header.get('resultMsg', '알 수 없는 오류')
                    logger.error(f"API 오류: {error_msg}")
                    return {"success": False, "error": error_msg, "items": []}
            
            return {"success": False, "error": "Invalid response format", "items": []}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API 요청 실패: {e}")
            return {"success": False, "error": str(e), "items": []}
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 실패: {e}")
            return {"success": False, "error": str(e), "items": []}
    
    def get_all_notices(
        self,
        keyword: str = "정보시스템 감리",
        max_pages: int = 5,
        num_of_rows: int = 100
    ) -> List[Dict[str, Any]]:
        """
        여러 페이지의 입찰공고 수집
        
        Args:
            keyword: 검색 키워드
            max_pages: 최대 페이지 수
            num_of_rows: 페이지당 결과 수
        
        Returns:
            입찰공고 목록
        """
        all_items = []
        
        for page in range(1, max_pages + 1):
            result = self.search_bid_notices(keyword, page, num_of_rows)
            
            if not result["success"]:
                logger.warning(f"페이지 {page} 수집 실패: {result.get('error', 'Unknown error')}")
                break
            
            items = result.get("items", [])
            if not items:
                logger.info(f"페이지 {page}: 더 이상 결과 없음")
                break
            
            all_items.extend(items)
            logger.info(f"페이지 {page}: {len(items)}건 수집 (누적: {len(all_items)}건)")
            
            # 전체 결과보다 많이 가져왔으면 중단
            total_count = result.get("total_count", 0)
            if len(all_items) >= total_count:
                break
        
        return all_items
    
    def parse_notice_data(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        API 응답 데이터를 DB 저장용으로 변환
        
        Args:
            raw_item: API에서 받은 원본 데이터
        
        Returns:
            정형화된 공고 데이터
        """
        # 날짜 파싱 함수
        def parse_date(date_str) -> Optional[str]:
            if not date_str:
                return None
            date_str = str(date_str).strip()
            if not date_str or len(date_str) < 8:
                return None
            try:
                # YYYYMMDD 형식 (숫자만 있는 경우)
                if date_str.isdigit() and len(date_str) >= 8:
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                # YYYY-MM-DD 형식 (이미 하이픈 포함)
                if '-' in date_str and len(date_str) >= 10:
                    return date_str[:10]
                # YYYYMMDDHHMM 또는 YYYYMMDDHHMMSS 형식
                if date_str.isdigit() and len(date_str) >= 8:
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except Exception:
                pass
            return None
        
        def parse_datetime(datetime_str) -> Optional[str]:
            if not datetime_str:
                return None
            datetime_str = str(datetime_str).strip()
            if not datetime_str or len(datetime_str) < 8:
                return None
            try:
                # YYYYMMDDHHMM 형식
                if datetime_str.isdigit():
                    if len(datetime_str) >= 12:
                        return f"{datetime_str[:4]}-{datetime_str[4:6]}-{datetime_str[6:8]} {datetime_str[8:10]}:{datetime_str[10:12]}:00"
                    elif len(datetime_str) >= 8:
                        return f"{datetime_str[:4]}-{datetime_str[4:6]}-{datetime_str[6:8]} 00:00:00"
                # 이미 포맷된 경우
                if '-' in datetime_str:
                    return datetime_str[:19] if len(datetime_str) >= 19 else None
            except Exception:
                pass
            return None
        
        # 금액 파싱
        def parse_price(price_str) -> Optional[int]:
            if not price_str:
                return None
            try:
                return int(float(str(price_str).replace(",", "")))
            except:
                return None
        
        parsed = {
            'notice_id': raw_item.get('bidNtceNo', ''),  # 입찰공고번호
            'title': raw_item.get('bidNtceNm', ''),  # 입찰공고명
            'organization': raw_item.get('dminsttNm', '') or raw_item.get('ntceInsttNm', ''),  # 수요기관명 또는 공고기관명
            'publish_date': parse_date(raw_item.get('bidNtceDt', '')),  # 입찰공고일자
            'deadline_date': parse_datetime(raw_item.get('bidClseDt', '')),  # 입찰마감일시
            'estimated_price': parse_price(raw_item.get('presmptPrce', '')),  # 추정가격
            'contract_method': raw_item.get('cntrctMthdNm', ''),  # 계약방법명
            'notice_url': raw_item.get('bidNtceDtlUrl', ''),  # 입찰공고상세URL
            'detail_content': raw_item.get('bidNtceDtlCntnts', ''),  # 상세내용
            'raw_data': json.dumps(raw_item, ensure_ascii=False)
        }
        
        return parsed


class NarajangterPipeline:
    """전체 파이프라인 관리"""
    
    def __init__(
        self,
        service_key: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None
    ):
        self.api = NarajangterOpenAPI(service_key)
        
        # 데이터베이스 연결 (DATABASE_URL 또는 환경변수 사용)
        self.db = PostgreSQLConnector()
    
    def run(self, keyword: str = "정보시스템 감리", max_pages: int = 5) -> Dict[str, Any]:
        """
        전체 파이프라인 실행
        
        Args:
            keyword: 검색 키워드
            max_pages: 수집할 최대 페이지 수
        
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
            
            # 공고 수집
            notices = self.api.get_all_notices(keyword, max_pages)
            result["scraped_count"] = len(notices)
            
            # 첫 번째 공고의 원본 데이터 로깅 (디버깅용)
            if notices:
                logger.info(f"첫 번째 공고 원본 데이터 샘플: {notices[0]}")
            
            # 데이터 파싱 및 저장
            for idx, raw_notice in enumerate(notices, 1):
                try:
                    parsed_notice = self.api.parse_notice_data(raw_notice)
                    notice_id = parsed_notice.get('notice_id', 'N/A')
                    title = parsed_notice.get('title', 'N/A')
                    
                    if parsed_notice.get('notice_id') or parsed_notice.get('title'):
                        if self.db.insert_notice(parsed_notice):
                            result["inserted_count"] += 1
                            logger.info(f"[{idx}/{len(notices)}] 저장 성공: {notice_id} - {title[:50]}")
                        else:
                            logger.warning(f"[{idx}/{len(notices)}] 저장 실패: {notice_id} - {title[:50]}")
                    else:
                        logger.warning(f"[{idx}/{len(notices)}] 공고 ID/제목 없음: {raw_notice}")
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
    
    parser = argparse.ArgumentParser(description='나라장터 정보시스템 감리 공고 스크래퍼 (Open API 버전)')
    parser.add_argument(
        '--keyword',
        default='정보시스템 감리',
        help='검색 키워드 (기본값: 정보시스템 감리)'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=5,
        help='수집할 최대 페이지 수 (기본값: 5)'
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
    
    # 파이프라인 실행 (DATABASE_URL 또는 환경변수에서 DB 설정 자동 로드)
    pipeline = NarajangterPipeline()
    result = pipeline.run(keyword=args.keyword, max_pages=args.max_pages)
    
    # 결과 출력
    print("\n" + "="*50)
    print("실행 결과")
    print("="*50)
    print(f"성공 여부: {'성공' if result['success'] else '실패'}")
    print(f"수집된 공고 수: {result['scraped_count']}")
    print(f"저장된 공고 수: {result['inserted_count']}")
    if result['errors']:
        print(f"오류 수: {len(result['errors'])}")
        for error in result['errors'][:5]:  # 최대 5개 오류 출력
            print(f"  - {error}")


if __name__ == "__main__":
    main()
