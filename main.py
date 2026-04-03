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
    'hashtag':    '3377ed94f6988051bbe4c9009e728452',  # 트렌딩 해시태그
    'competitor': '3377ed94f698805a8368e5f1ab379723',  # 경쟁 계정 성과
    'keyword':    '3377ed94f69880dea57dd5bc9cda2054',  # F&B 키워드 버즈량
    'viral':      '3377ed94f698800f974ad4c87881bb53',  # 급상승 콘텐츠
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
    if resp.status_code != 200:
        print(f'  ⚠️ DB 조회 실패 ({db_id[:8]}...): {resp.status_code} {resp.text[:200]}')
        return
    existing = set(resp.json().get('properties', {}).keys())
 
    # 없는 속성만 추가
    new_props = {k: v for k, v in properties.items() if k not in existing}
    if new_props:
        print(f'  → 추가할 속성: {list(new_props.keys())}')
        r = requests.patch(
            f'https://api.notion.com/v1/databases/{db_id}',
            headers=NOTION_HEADERS,
            json={'properties': new_props},
        )
        if r.status_code != 200:
            print(f'  ⚠️ 속성 추가 실패: {r.status_code} {r.text[:300]}')
        else:
            print(f'  ✅ 속성 추가 완료')
    else:
        print(f'  → 이미 모든 속성 존재')
 
def setup_all_dbs():
    print('🔧 Notion DB 속성 초기화 중...')
    setup_db_properties(DB['hashtag'], {
        '수집 날짜':   {'date': {}},
        '플랫폼':      {'select': {}},
        '게시물수':   {'number': {}},
        '순위':        {'number': {}},
        '원본 링크':   {'url': {}},
        '대표 게시물': {'url': {}},
    })
    setup_db_properties(DB['competitor'], {
        '수집 날짜':    {'date': {}},
        '발행일자':     {'rich_text': {}},
        '기간':         {'select': {}},
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
 
 
# ── 1. 경쟁사 게시물에서 해시태그 집계 ──────────────────────────
def extract_hashtags_from_competitors(competitor_data):
    """경쟁사 게시물의 해시태그를 집계해서 상위 30개 반환"""
    print('📌 경쟁사 해시태그 집계 중...')
    from collections import Counter
    counter = Counter()
    post_examples = {}  # 해시태그별 대표 게시물 URL
 
    for item in competitor_data:
        tags_raw = item.get('hashtags', '') or ''
        post_url = item.get('url', '')
        period = item.get('period', '')
        # 문자열로 저장된 경우 파싱
        if isinstance(tags_raw, str):
            tags = [t.strip() for t in tags_raw.split(',') if t.strip()]
        else:
            tags = tags_raw
 
        for tag in tags:
            tag = tag.strip().lstrip('#')
            if not tag:
                continue
            key = f'#{tag}'
            counter[key] += 1
            if key not in post_examples:
                post_examples[key] = post_url
 
    results = []
    for rank, (tag, count) in enumerate(counter.most_common(30), 1):
        results.append({
            'hashtag': tag,
            'post_count': count,
            'platform': 'Instagram (경쟁사)',
            'url': f'https://www.instagram.com/explore/tags/{tag.lstrip("#")}/',
            'top_post_url': post_examples.get(tag, ''),
            'rank': rank,
        })
 
    print(f'  → {len(results)}개 해시태그 집계 완료')
    return results
 
 
# ── 2. 경쟁 계정 성과 수집 ────────────────────────────────────────
from datetime import timedelta
 
def _fetch_competitor_posts(label, newer_than=None, older_than=None):
    # 긴 기간 구간은 더 많은 게시물 수집
    limit = 30 if label in ('1년전~1년1개월전 전체', '현재부터 1달 전') else 10
    params = {
        'directUrls': [f'https://www.instagram.com/{acc}/' for acc in COMPETITOR_ACCOUNTS],
        'resultsType': 'posts',
        'resultsLimit': limit,
    }
    if newer_than:
        params['onlyPostsNewerThan'] = newer_than
    if older_than:
        params['onlyPostsOlderThan'] = older_than
    data = run_apify('apify/instagram-scraper', params)
    results = []
    seen_urls = set()
    for item in data:
        url = item.get('url', '')
        if url in seen_urls:
            continue
        seen_urls.add(url)
 
        # 캡션 필드명 다양하게 시도
        caption = (
            item.get('caption') or
            item.get('text') or
            item.get('description') or
            item.get('accessibility_caption') or ''
        )
 
        # 해시태그: 리스트 또는 캡션에서 추출
        hashtags_raw = item.get('hashtags') or []
        if not hashtags_raw and caption:
            import re
            hashtags_raw = re.findall(r'#(\w+)', caption)
 
        # 발행일자 파싱
        published = (
            item.get('timestamp') or
            item.get('taken_at_timestamp') or
            item.get('takenAtTimestamp') or
            item.get('date') or
            item.get('publishedAt') or ''
        )
        # Unix timestamp면 날짜로 변환
        if isinstance(published, (int, float)) and published > 1000000000:
            from datetime import datetime
            published = datetime.utcfromtimestamp(published).strftime('%Y-%m-%d')
        elif isinstance(published, str) and 'T' in published:
            published = published[:10]
 
        # 썸네일 이미지 URL
        thumbnail = (
            item.get('displayUrl') or
            item.get('thumbnailUrl') or
            item.get('thumbnail_url') or
            item.get('imageUrl') or
            item.get('previewUrl') or ''
        )
 
        results.append({
            'account': item.get('ownerUsername', '') or item.get('username', ''),
            'caption': caption[:200],
            'likes': item.get('likesCount', 0) or item.get('likes', 0),
            'comments': item.get('commentsCount', 0) or item.get('comments', 0),
            'views': item.get('videoViewCount', 0) or item.get('videoPlayCount', 0) or 0,
            'url': url,
            'hashtags': ', '.join((hashtags_raw or [])[:10]),
            'platform': 'Instagram',
            'period': label,
            'published_at': str(published) if published else '',
            'thumbnail': thumbnail,
        })
    return results
 
 
def collect_competitors():
    print('👥 경쟁 계정 수집 중...')
    today = date.today()
    periods = [
        # 현재: 최근 7일
        ('현재',
         (today - timedelta(days=7)).isoformat(), today.isoformat()),
        # 현재부터 1달 전: 최근 30일 전체
        ('현재부터 1달 전',
         (today - timedelta(days=30)).isoformat(), today.isoformat()),
        # 1년 전: 작년 동일 시점 ±7일
        ('1년 전',
         (today - timedelta(days=372)).isoformat(), (today - timedelta(days=358)).isoformat()),
        # 1년 1개월 전: 13개월 전 동일 시점 ±7일
        ('1년 1개월 전',
         (today - timedelta(days=403)).isoformat(), (today - timedelta(days=389)).isoformat()),
        # 1년 전 ~ 1년 1개월 전 사이 전체 (작년 해당 월 게시글 모두)
        ('1년전~1년1개월전 전체',
         (today - timedelta(days=396)).isoformat(), (today - timedelta(days=335)).isoformat()),
    ]
    results = []
    for label, newer, older in periods:
        print(f'  📅 {label} ({newer} ~ {older or "현재"}) ...')
        try:
            items = _fetch_competitor_posts(label, newer_than=newer, older_than=older)
            results.extend(items)
            print(f'     → {len(items)}개')
        except Exception as e:
            print(f'  ⚠️ {label} 실패: {e}')
    print(f'  → 합계 {len(results)}개 수집')
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
 
 
# ── 6. 순위 산정 (Python 정렬) ───────────────────────────────────
def rank_items(category, items):
    if not items:
        return []
    print(f'📊 순위 산정: {category}')
    if category == '트렌딩 해시태그':
        key = lambda x: x.get('post_count', 0)
    elif category == '경쟁 계정 성과':
        key = lambda x: x.get('likes', 0) + x.get('comments', 0) + x.get('views', 0)
    elif category == 'F&B 키워드 버즈량':
        key = lambda x: x.get('mention_count', 0)
    else:
        key = lambda x: x.get('views', 0)
    sorted_items = sorted(items, key=key, reverse=True)[:30]
    for i, item in enumerate(sorted_items, 1):
        item['rank'] = i
    return sorted_items
 
 
# ── 7. Notion 저장 헬퍼 ───────────────────────────────────────────
def notion_post(db_id, properties):
    resp = requests.post(
        'https://api.notion.com/v1/pages',
        headers=NOTION_HEADERS,
        json={'parent': {'database_id': db_id}, 'properties': properties},
        timeout=30,
    )
    if resp.status_code != 200:
        print(f'  ⚠️ Notion 저장 실패: {resp.status_code} {resp.text[:300]}')
 
 
def safe_url(val):
    """빈 문자열이나 None은 None으로 반환 (Notion URL 필드는 null 허용)"""
    return val if val and val.startswith('http') else None
 
 
def save_hashtags(items):
    print(f'  💾 트렌딩 해시태그 {len(items)}개 저장...')
    for item in items:
        notion_post(DB['hashtag'], {
            '이름':        {'title': [{'text': {'content': item.get('hashtag', '')}}]},
            '수집 날짜':   {'date': {'start': TODAY}},
            '플랫폼':      {'select': {'name': item.get('platform', 'Instagram')}},
            '게시물수':   {'number': item.get('post_count', 0)},
            '순위':        {'number': item.get('rank', 0)},
            '원본 링크':   {'url': safe_url(item.get('url'))},
            '대표 게시물': {'url': safe_url(item.get('top_post_url'))},
        })
 
 
def save_competitors(items):
    print(f'  💾 경쟁 계정 성과 {len(items)}개 저장...')
    for item in items:
        thumbnail = safe_url(item.get('thumbnail'))
 
        # 페이지 본문에 썸네일 이미지 블록 추가
        children = []
        if thumbnail:
            children = [{
                'object': 'block',
                'type': 'image',
                'image': {
                    'type': 'external',
                    'external': {'url': thumbnail}
                }
            }]
 
        resp = requests.post(
            'https://api.notion.com/v1/pages',
            headers=NOTION_HEADERS,
            json={
                'parent': {'database_id': DB['competitor']},
                'properties': {
                    '이름':         {'title': [{'text': {'content': item.get('account', '')}}]},
                    '수집 날짜':    {'date': {'start': TODAY}},
                    '발행일자':     {'rich_text': [{'text': {'content': item.get('published_at', '')}}]},
                    '기간':         {'select': {'name': item.get('period', '현재')}},
                    '캡션':         {'rich_text': [{'text': {'content': item.get('caption', '')}}]},
                    '좋아요':       {'number': item.get('likes', 0)},
                    '댓글':         {'number': item.get('comments', 0)},
                    '조회수':       {'number': item.get('views', 0)},
                    '순위':         {'number': item.get('rank', 0)},
                    '원본 링크':    {'url': safe_url(item.get('url'))},
                    '사용 해시태그':{'rich_text': [{'text': {'content': item.get('hashtags', '')}}]},
                },
                'children': children,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            print(f'  ⚠️ Notion 저장 실패: {resp.status_code} {resp.text[:300]}')
 
 
def save_keywords(items):
    print(f'  💾 F&B 키워드 버즈량 {len(items)}개 저장...')
    for item in items:
        notion_post(DB['keyword'], {
            '이름':      {'title': [{'text': {'content': item.get('keyword', '')}}]},
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
            '이름':      {'title': [{'text': {'content': item.get('title', '')}}]},
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
 
    # 수집 (경쟁사 먼저 → 해시태그 집계)
    competitor_data = collect_competitors()
    hashtag_data    = extract_hashtags_from_competitors(competitor_data)
    buzz_data       = collect_twitter_buzz() + collect_naver()
    viral_data      = collect_youtube()
 
    print(f'\n📊 수집 완료 — 해시태그 {len(hashtag_data)} | 경쟁계정 {len(competitor_data)} | 버즈량 {len(buzz_data)} | 급상승 {len(viral_data)}\n')
 
    # 순위 산정 (해시태그는 이미 rank 포함, 나머지만 정렬)
    hashtag_ranked    = hashtag_data  # 이미 집계 시 순위 매김
    competitor_ranked = rank_items('경쟁 계정 성과', competitor_data)
    buzz_ranked       = rank_items('F&B 키워드 버즈량', buzz_data)
    viral_ranked      = rank_items('급상승 콘텐츠', viral_data)
 
    # Notion 저장
    print('\n💾 Notion 저장 중...')
    save_hashtags(hashtag_ranked)
    save_competitors(competitor_ranked)
    save_keywords(buzz_ranked)
    save_viral(viral_ranked)
 
    print('\n✅ 완료!')
 
