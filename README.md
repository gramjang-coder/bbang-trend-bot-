# 빵모닝 트렌드 수집 봇

매일 오전 8시(KST) 인스타그램·트위터·유튜브·네이버 트렌드를 수집해서 노션에 자동 저장합니다.

---

## 세팅 방법

### 1. GitHub Secrets 등록
레포 → Settings → Secrets and variables → Actions → New repository secret

| Secret 이름 | 값 |
|---|---|
| `NOTION_API_KEY` | 빵모닝 콘텐츠 컨버터 integration 키 (`secret_xxx`) |
| `APIFY_API_KEY` | Apify 콘솔 → Settings → Integrations |
| `ANTHROPIC_API_KEY` | Anthropic 콘솔 API 키 |
| `YOUTUBE_API_KEY` | Google Cloud Console → YouTube Data API v3 |
| `NAVER_CLIENT_ID` | 네이버 개발자센터 앱 Client ID |
| `NAVER_CLIENT_SECRET` | 네이버 개발자센터 앱 Client Secret |

### 2. 경쟁 계정 추가/삭제
`main.py` 상단 `COMPETITOR_ACCOUNTS` 리스트 수정

### 3. 수동 실행
GitHub → Actions → Daily Trend Collector → Run workflow

---

## Notion DB 구조

| DB | ID |
|---|---|
| 트렌딩 해시태그 | `3377ed94f69880e29ef0c905265f0514` |
| 경쟁 계정 성과 | `3377ed94f69880c1b899c7b2ca598aab` |
| F&B 키워드 버즈량 | `3377ed94f69880cfbfc5ce6d36ca61ae` |
| 급상승 콘텐츠 | `3377ed94f698803c9c0bdae3115e9156` |

---

## 아직 필요한 API 키

- [ ] YouTube Data API v3 (Google Cloud Console, 무료)
- [ ] 네이버 DataLab API (네이버 개발자센터, 무료)
