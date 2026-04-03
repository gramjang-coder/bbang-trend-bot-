import os
import json
import requests
from datetime import date
from anthropic import Anthropic
 
# ── 환경변수 ──────────────────────────────────────────────────────
NOTION_KEY    = os.environ['NOTION_API_KEY']
APIFY_KEY     = os.environ['APIFY_API_KEY']
ANTHROPIC_KEY = os.environ['ANTHROPIC_API_KEY']
YOUTUBE_KEY   = os.environ['YOUTUBE_API_KEY']
NAVER_ID      = os.environ['NAVER_CLIENT_ID']
NAVER_SECRET  = os.environ['NAVER_CLIENT_SECRET']
 
TODAY = date.today().isoformat()
 
# ── Notion DB IDs ─────────────────────────────────────────────────
DB = {
    'hashtag':    '3377ed94f69880e29ef0c905265f0514',  # 트렌딩 해시태그
    'competitor': '3377ed94f69880c1b899c7b2ca598aab',  # 경쟁 계정 성과
    'keyword':    '3377ed94f69880cfbfc5ce6d36ca61ae',  # F&B 키워드 버즈량
    'viral':      '3377ed94f698803c9c0bdae3115e9156',  # 급상승 콘텐츠
}
 
# ── 경쟁 계정 리스트 (추가/삭제 여기서) ─────────────────────────
COMPETITOR_ACCOUNTS = [
    'knewnew.official',
    'omuck.official',
    'eyesmag',
    'dailyfood_news',
    'daily_fnb',
    'idea82people',
]
 
# ── 수집 해시태그 키워드 ──────────────────────────────────────────
HASHTAGS = [
    '빵', '베이커리', '소금빵', '크루아상', '디저트', '식빵',
    '도넛', '마카롱', '케이크', '빵집', '브런치카페', '카페투어',
    'bakery', 'bread', 'croissant', 'sourdough', 'pastry',
]
 
BUZZ_KEYWORDS = ['빵', '베이커리', '소금빵', '크루아상', '디저트카페', '빵집투어', '브런치']
 
 
# ── Notion DB 속성 초기화 ─────────────────────────────────────────
NOTION_HEADERS = {
    'Authorization': f'Bearer {NOTION_KEY}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json',
}
 
def setup_db_properties(db_id, properties):
    """DB에 없는 속성만 추가"""
    # 현재 속성 조회
    resp = requests.get(f'https://api.notion.com/v1/databases/{db_id}', headers=NOTION_HEADERS)
    existing = set(resp.json().get('properties', {}).keys())
    
    # 없는 속성만 추가
    new_props = {k: v for k, v in properties.items() if k not in existing}
    if new_props:
        requests.patch(
            f'https://api.notion.com/v1/databases/{db_id}',
            headers=NOTION_HEADERS,
            json={'properties': new_props},
        )
 
def setup_all_dbs():
    print('🔧 Notion DB 속성 초기화 중...')
    setup_db_properties(DB['hashtag'], {
        '수집 날짜':   {'date': {}},
        '플랫폼':      {'select': {}},
        '게시물 수':   {'number': {}},
        '순위':        {'number': {}},
        '원본 링크':   {'url': {}},
        '대표 게시물': {'url': {}},
    })
    setup_db_properties(DB['competitor'], {
        '수집 날짜':    {'date': {}},
        '캡션':         {'rich_text': {}},
        '좋아요':       {'number': {}},
        '댓글':         {'number': {}},
        '조회수':       {'number': {}},
        '순위':         {'number': {}},
        '원본 링크':    {'url': {}},
        '사용 해시태그':{'rich_text': {}},
    })
    setup_db_properties(DB['keyword'], {
        '수집 날짜': {'date': {}},
        '플랫폼':    {'select': {}},
        '언급량':    {'number': {}},
        '순위':      {'number': {}},
        '원본 링크': {'url': {}},
    })
    setup_db_properties(DB['viral'], {
        '수집 날짜': {'date': {}},
        '플랫폼':    {'select': {}},
        '계정명':    {'rich_text': {}},
        '조회수':    {'number': {}},
        '순위':      {'number': {}},
        '원본 링크': {'url': {}},
        '키워드':    {'rich_text': {}},
    })
    print('  → 완료')
 
 
# ── Apify 실행 ────────────────────────────────────────────────────
def run_apify(actor_id, input_data, timeout=180):
    # Apify API는 액터 ID에서 '/'를 '~'로 변환 필요
    actor_id = actor_id.replace('/', '~')
    url = f'https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items'
    resp = requests.post(
        url,
        params={'token': APIFY_KEY, 'timeout': 120, 'memory': 256},
        json=input_data,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()
 
 
# ── 1. 트렌딩 해시태그 수집 (인스타그램) ─────────────────────────
def collect_hashtags():
    print('📌 해시태그 수집 중...')
    results = []
    for tag in HASHTAGS:
        try:
            data = run_apify('apify/instagram-hashtag-scraper', {
                'hashtags': [tag],
                'resultsLimit': 10,
            })
            if data:
                item = data[0]
                results.append({
                    'hashtag': f'#{tag}',
                    'post_count': item.get('postsCount', 0),
                    'platform': 'Instagram',
                    'url': f'https://www.instagram.com/explore/tags/{tag}/',
                    'top_post_url': item.get('url', ''),
                })
        except Exception as e:
            print(f'  ⚠️ #{tag} 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results
 
 
# ── 2. 경쟁 계정 성과 수집 ────────────────────────────────────────
def collect_competitors():
    print('👥 경쟁 계정 수집 중...')
    results = []
    try:
        data = run_apify('apify/instagram-scraper', {
            'directUrls': [f'https://www.instagram.com/{acc}/' for acc in COMPETITOR_ACCOUNTS],
            'resultsType': 'posts',
            'resultsLimit': 10,
        })
        for item in data:
            results.append({
                'account': item.get('ownerUsername', ''),
                'caption': (item.get('caption', '') or '')[:200],
                'likes': item.get('likesCount', 0),
                'comments': item.get('commentsCount', 0),
                'views': item.get('videoViewCount', 0),
                'url': item.get('url', ''),
                'hashtags': ', '.join((item.get('hashtags', []) or [])[:10]),
                'platform': 'Instagram',
            })
    except Exception as e:
        print(f'  ⚠️ 경쟁 계정 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results
 
 
# ── 3. 트위터 버즈량 수집 ─────────────────────────────────────────
def collect_twitter_buzz():
    print('🐦 트위터 버즈량 수집 중...')
    results = []
    try:
        data = run_apify('quacker/twitter-scraper', {
            'searchTerms': BUZZ_KEYWORDS,
            'maxItems': 100,
            'sort': 'Latest',
        })
        buzz = {}
        for item in data:
            text = item.get('full_text', '') or item.get('text', '')
            for kw in BUZZ_KEYWORDS:
                if kw in text:
                    buzz[kw] = buzz.get(kw, 0) + 1
        for kw, count in sorted(buzz.items(), key=lambda x: -x[1]):
            results.append({
                'keyword': kw,
                'mention_count': count,
                'platform': 'Twitter/X',
                'url': f'https://twitter.com/search?q={kw}',
            })
    except Exception as e:
        print(f'  ⚠️ 트위터 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results
 
 
# ── 4. 유튜브 급상승 수집 ─────────────────────────────────────────
def collect_youtube():
    print('📺 유튜브 수집 중...')
    results = []
    for kw in BUZZ_KEYWORDS[:5]:
        try:
            resp = requests.get(
                'https://www.googleapis.com/youtube/v3/search',
                params={
                    'key': YOUTUBE_KEY,
                    'q': kw,
                    'type': 'video',
                    'order': 'viewCount',
                    'regionCode': 'KR',
                    'relevanceLanguage': 'ko',
                    'maxResults': 5,
                    'part': 'snippet',
                },
                timeout=30,
            )
            for item in resp.json().get('items', []):
                vid_id = item['id']['videoId']
                results.append({
                    'title': item['snippet']['title'],
                    'channel': item['snippet']['channelTitle'],
                    'keyword': kw,
                    'views': 0,  # 조회수는 videos.list API 별도 호출 필요
                    'url': f'https://www.youtube.com/watch?v={vid_id}',
                    'platform': 'YouTube',
                })
        except Exception as e:
            print(f'  ⚠️ 유튜브 {kw} 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results
 
 
# ── 5. 네이버 트렌드 수집 ─────────────────────────────────────────
def collect_naver():
    print('🔍 네이버 트렌드 수집 중...')
    results = []
    try:
        resp = requests.post(
            'https://openapi.naver.com/v1/datalab/search',
            headers={
                'X-Naver-Client-Id': NAVER_ID,
                'X-Naver-Client-Secret': NAVER_SECRET,
                'Content-Type': 'application/json',
            },
            json={
                'startDate': TODAY,
                'endDate': TODAY,
                'timeUnit': 'date',
                'keywordGroups': [
                    {'groupName': kw, 'keywords': [kw]}
                    for kw in BUZZ_KEYWORDS
                ],
            },
            timeout=30,
        )
        for result in resp.json().get('results', []):
            for point in result.get('data', []):
                results.append({
                    'keyword': result['title'],
                    'mention_count': point.get('ratio', 0),
                    'platform': 'Naver',
                    'url': f'https://search.naver.com/search.naver?query={result["title"]}',
                })
    except Exception as e:
        print(f'  ⚠️ 네이버 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results
 
 
# ── 6. Claude로 순위 산정 ─────────────────────────────────────────
def rank_with_claude(category, items):
    if not items:
        return []
    print(f'🤖 Claude 순위 산정: {category}')
    client = Anthropic(api_key=ANTHROPIC_KEY)
 
    # 최대 30개만 전달 (JSON 파싱 안정성)
    items = items[:30]
 
    prompt = f"""아래는 오늘({TODAY}) 수집된 [{category}] 데이터입니다.
순위를 매기고, 각 항목에 "rank" 필드(1부터 시작)를 추가해서 JSON 배열로만 반환하세요.
 
순위 기준:
- 트렌딩 해시태그: post_count 높은 순
- 경쟁 계정 성과: likes + comments + views 합산 높은 순
- F&B 키워드 버즈량: mention_count 높은 순
- 급상승 콘텐츠: views 높은 순
 
데이터:
{json.dumps(items, ensure_ascii=False)}
 
반드시 JSON 배열([...])만 반환. 설명 텍스트, 마크다운 코드블록 없이."""
 
    resp = client.messages.create(
        model='claude-sonnet-4-20250514',
        max_tokens=8000,
        messages=[{'role': 'user', 'content': prompt}],
    )
    text = resp.content[0].text.strip()
 
    # 코드블록 제거
    if '```' in text:
        parts = text.split('```')
        for part in parts:
            part = part.strip()
            if part.startswith('json'):
                part = part[4:].strip()
            if part.startswith('['):
                text = part
                break
 
    # [ ] 범위만 추출
    start = text.find('[')
    end = text.rfind(']')
    if start != -1 and end != -1:
        text = text[start:end+1]
 
    return json.loads(text)
 
 
# ── 7. Notion 저장 헬퍼 ───────────────────────────────────────────
def notion_post(db_id, properties):
    resp = requests.post(
        'https://api.notion.com/v1/pages',
        headers=NOTION_HEADERS,
        json={'parent': {'database_id': db_id}, 'properties': properties},
        timeout=30,
    )
    if resp.status_code != 200:
        print(f'  ⚠️ Notion 저장 실패: {resp.status_code} {resp.text[:100]}')
 
 
def safe_url(val):
    """빈 문자열이나 None은 None으로 반환 (Notion URL 필드는 null 허용)"""
    return val if val and val.startswith('http') else None
 
 
def save_hashtags(items):
    print(f'  💾 트렌딩 해시태그 {len(items)}개 저장...')
    for item in items:
        notion_post(DB['hashtag'], {
            'Name':        {'title': [{'text': {'content': item.get('hashtag', '')}}]},
            '수집 날짜':   {'date': {'start': TODAY}},
            '플랫폼':      {'select': {'name': item.get('platform', 'Instagram')}},
            '게시물 수':   {'number': item.get('post_count', 0)},
            '순위':        {'number': item.get('rank', 0)},
            '원본 링크':   {'url': safe_url(item.get('url'))},
            '대표 게시물': {'url': safe_url(item.get('top_post_url'))},
        })
 
 
def save_competitors(items):
    print(f'  💾 경쟁 계정 성과 {len(items)}개 저장...')
    for item in items:
        notion_post(DB['competitor'], {
            'Name':         {'title': [{'text': {'content': item.get('account', '')}}]},
            '수집 날짜':    {'date': {'start': TODAY}},
            '캡션':         {'rich_text': [{'text': {'content': item.get('caption', '')}}]},
            '좋아요':       {'number': item.get('likes', 0)},
            '댓글':         {'number': item.get('comments', 0)},
            '조회수':       {'number': item.get('views', 0)},
            '순위':         {'number': item.get('rank', 0)},
            '원본 링크':    {'url': safe_url(item.get('url'))},
            '사용 해시태그':{'rich_text': [{'text': {'content': item.get('hashtags', '')}}]},
        })
 
 
def save_keywords(items):
    print(f'  💾 F&B 키워드 버즈량 {len(items)}개 저장...')
    for item in items:
        notion_post(DB['keyword'], {
            'Name':      {'title': [{'text': {'content': item.get('keyword', '')}}]},
            '수집 날짜': {'date': {'start': TODAY}},
            '플랫폼':    {'select': {'name': item.get('platform', '')}},
            '언급량':    {'number': item.get('mention_count', 0)},
            '순위':      {'number': item.get('rank', 0)},
            '원본 링크': {'url': safe_url(item.get('url'))},
        })
 
 
def save_viral(items):
    print(f'  💾 급상승 콘텐츠 {len(items)}개 저장...')
    for item in items:
        notion_post(DB['viral'], {
            'Name':      {'title': [{'text': {'content': item.get('title', '')}}]},
            '수집 날짜': {'date': {'start': TODAY}},
            '플랫폼':    {'select': {'name': item.get('platform', '')}},
            '계정명':    {'rich_text': [{'text': {'content': item.get('channel', item.get('account', ''))}}]},
            '조회수':    {'number': item.get('views', 0)},
            '순위':      {'number': item.get('rank', 0)},
            '원본 링크': {'url': safe_url(item.get('url'))},
            '키워드':    {'rich_text': [{'text': {'content': item.get('keyword', '')}}]},
        })
 
 
# ── 메인 ──────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f'🚀 트렌드 수집 시작: {TODAY}\n')
    setup_all_dbs()
 
    # 수집
    hashtag_data    = collect_hashtags()
    competitor_data = collect_competitors()
    buzz_data       = collect_twitter_buzz() + collect_naver()
    viral_data      = collect_youtube()
 
    print(f'\n📊 수집 완료 — 해시태그 {len(hashtag_data)} | 경쟁계정 {len(competitor_data)} | 버즈량 {len(buzz_data)} | 급상승 {len(viral_data)}\n')
 
    # Claude 순위 산정
    hashtag_ranked    = rank_with_claude('트렌딩 해시태그', hashtag_data)
    competitor_ranked = rank_with_claude('경쟁 계정 성과', competitor_data)
    buzz_ranked       = rank_with_claude('F&B 키워드 버즈량', buzz_data)
    viral_ranked      = rank_with_claude('급상승 콘텐츠', viral_data)
 
    # Notion 저장
    print('\n💾 Notion 저장 중...')
    save_hashtags(hashtag_ranked)
    save_competitors(competitor_ranked)
    save_keywords(buzz_ranked)
    save_viral(viral_ranked)
 
    print('\n✅ 완료!')
