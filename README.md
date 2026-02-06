# 나라장터 정보시스템 감리 공고 스크래퍼

나라장터(G2B)에서 '정보시스템 감리' 관련 입찰공고를 Firecrawl을 사용하여 스크래핑하고 PostgreSQL 데이터베이스에 저장하는 파이썬 스크립트입니다.

## 기능

- 🔍 나라장터에서 '정보시스템 감리' 키워드로 입찰공고 검색
- 🕷️ Firecrawl API를 사용한 웹 스크래핑
- 💾 PostgreSQL 데이터베이스에 공고 데이터 저장
- 🔄 중복 공고 자동 업데이트 (UPSERT)
- 📊 스크래핑 결과 로깅

## 설치

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경변수 설정

`.env.example` 파일을 `.env`로 복사하고 필요한 값을 설정합니다:

```bash
cp .env.example .env
```

필수 환경변수:
- `FIRECRAWL_API_KEY`: Firecrawl API 키 ([firecrawl.dev](https://firecrawl.dev)에서 발급)
- `POSTGRES_HOST`: PostgreSQL 호스트
- `POSTGRES_PORT`: PostgreSQL 포트
- `POSTGRES_DB`: 데이터베이스 이름
- `POSTGRES_USER`: 데이터베이스 사용자
- `POSTGRES_PASSWORD`: 데이터베이스 비밀번호

### 3. PostgreSQL 데이터베이스 생성

```sql
CREATE DATABASE narajangter;
```

테이블은 스크립트 실행 시 자동으로 생성됩니다.

## 사용법

### 기본 실행

```bash
python scraper.py
```

### 옵션 사용

```bash
# 최대 10 페이지 크롤링
python scraper.py --max-pages 10

# 데이터베이스 설정 지정
python scraper.py --db-host localhost --db-port 5432 --db-name mydb --db-user myuser --db-password mypass
```

### 사용 가능한 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--max-pages` | 크롤링할 최대 페이지 수 | 5 |
| `--db-host` | PostgreSQL 호스트 | localhost |
| `--db-port` | PostgreSQL 포트 | 5432 |
| `--db-name` | 데이터베이스 이름 | narajangter |
| `--db-user` | 데이터베이스 사용자 | postgres |
| `--db-password` | 데이터베이스 비밀번호 | (없음) |

## 데이터베이스 스키마

### audit_notices 테이블

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | SERIAL | 기본 키 |
| notice_id | VARCHAR(100) | 공고번호 (유니크) |
| title | VARCHAR(500) | 공고 제목 |
| organization | VARCHAR(200) | 공고 기관 |
| publish_date | DATE | 공고 게시일 |
| deadline_date | TIMESTAMP | 입찰 마감일시 |
| estimated_price | BIGINT | 추정 가격 |
| contract_method | VARCHAR(100) | 계약 방법 |
| notice_url | TEXT | 공고 URL |
| detail_content | TEXT | 상세 내용 (Markdown) |
| raw_data | JSONB | 원본 스크래핑 데이터 |
| scraped_at | TIMESTAMP | 스크래핑 시간 |
| updated_at | TIMESTAMP | 업데이트 시간 |

## 프로젝트 구조

```
narajangter_scraper/
├── scraper.py           # 메인 스크래퍼 스크립트
├── requirements.txt     # Python 의존성
├── .env.example         # 환경변수 예시
└── README.md            # 이 파일
```

## 주의사항

1. **Firecrawl API 키 필요**: [firecrawl.dev](https://firecrawl.dev)에서 API 키를 발급받아야 합니다.
2. **나라장터 이용 약관**: 웹 스크래핑 시 나라장터의 이용약관을 준수해주세요.
3. **요청 제한**: Firecrawl API의 요청 제한을 확인하고 적절한 간격으로 실행해주세요.

## 라이선스

MIT License
