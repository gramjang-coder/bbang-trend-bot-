import os
import json
import re
import requests
from datetime import date, timedelta, datetime
from collections import Counter
import gspread
from google.oauth2.service_account import Credentials

APIFY_KEY    = os.environ['APIFY_API_KEY']
YOUTUBE_KEY  = os.environ['YOUTUBE_API_KEY']
NAVER_ID     = os.environ.get('NAVER_CLIENT_ID', '')
NAVER_SECRET = os.environ.get('NAVER_CLIENT_SECRET', '')
GCP_JSON     = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']
SHEET_ID     = '1Z0MsWDAOpIXzC6kA3RO5igkapu5UMCV4yC0KQsb9XXw'

TODAY = date.today().isoformat()

COMPETITOR_ACCOUNTS = [
    'knewnew.official',
    'omuck.official',
    'eyesmag',
    'dailyfood_news',
    'daily_fnb',
    'idea82people',
]

BUZZ_KEYWORDS = ['빵', '베이커리', '소금빵', '크루아상', '디저트카페', '빵집투어', '브런치']


def get_sheet():
    creds_info = json.loads(GCP_JSON)
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
    # 시트 이름이 다르면 변경
    if spreadsheet.title != '빵모닝 기획 아이디어 수집':
        spreadsheet.update_title('빵모닝 기획 아이디어 수집')
    return spreadsheet


def get_or_create_sheet(workbook, title, headers):
    try:
        ws = workbook.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = workbook.add_worksheet(title=title, rows=2000, cols=len(headers))
        ws.append_row(headers, value_input_option='USER_ENTERED')
        ws.format('1:1', {
            'backgroundColor': {'red': 0.15, 'green': 0.15, 'blue': 0.15},
            'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
        })
    return ws


def run_apify(actor_id, input_data, timeout=240):
    actor_id = actor_id.replace('/', '~')
    url = f'https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items'
    resp = requests.post(
        url,
        params={'token': APIFY_KEY, 'timeout': 180, 'memory': 512},
        json=input_data,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def parse_post(item, period):
    caption = (
        item.get('caption') or item.get('text') or
        item.get('description') or item.get('accessibility_caption') or ''
    )
    hashtags_raw = item.get('hashtags') or []
    if not hashtags_raw and caption:
        hashtags_raw = re.findall(r'#(\w+)', caption)

    published = (
        item.get('timestamp') or item.get('taken_at_timestamp') or
        item.get('takenAtTimestamp') or item.get('date') or
        item.get('publishedAt') or ''
    )
    if isinstance(published, (int, float)) and published > 1000000000:
        published = datetime.utcfromtimestamp(published).strftime('%Y-%m-%d')
    elif isinstance(published, str) and 'T' in published:
        published = published[:10]

    thumbnail = (
        item.get('displayUrl') or item.get('thumbnailUrl') or
        item.get('thumbnail_url') or item.get('imageUrl') or
        item.get('previewUrl') or ''
    )

    return {
        'account':      item.get('ownerUsername', '') or item.get('username', ''),
        'caption':      caption[:300],
        'likes':        item.get('likesCount', 0) or item.get('likes', 0) or 0,
        'comments':     item.get('commentsCount', 0) or item.get('comments', 0) or 0,
        'views':        item.get('videoViewCount', 0) or item.get('videoPlayCount', 0) or item.get('viewCount', 0) or 0,
        'url':          item.get('url', ''),
        'hashtags':     ', '.join((hashtags_raw or [])[:15]),
        'period':       period,
        'published_at': str(published) if published else '',
        'thumbnail':    thumbnail,
    }


def _within_days(pub_str, min_days, max_days, today):
    if not pub_str:
        return False
    try:
        pub_dt = date.fromisoformat(pub_str[:10])
        delta = (today - pub_dt).days
        return min_days <= delta <= max_days
    except:
        return False


def fetch_posts_apify(label, newer_than=None, older_than=None, limit=30):
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
    seen = set()
    results = []
    for item in data:
        url = item.get('url', '')
        if url in seen:
            continue
        seen.add(url)
        results.append(parse_post(item, label))
    return results


def collect_competitors():
    print('👥 경쟁 계정 수집 중...')
    today = date.today()
    results = []

    # 현재 + 1달 전: 최신 50개 수집 후 날짜로 분류
    print(f'  📥 최근 게시물 수집 중...')
    try:
        recent = fetch_posts_apify('', limit=50)
        for p in recent:
            if _within_days(p.get('published_at',''), 0, 7, today):
                p['period'] = '현재'
                results.append(p)
            elif _within_days(p.get('published_at',''), 8, 30, today):
                p['period'] = '현재부터 1달 전'
                results.append(p)
            # 기간 범위 밖 게시물은 저장하지 않음
        print(f'     → 현재/1달전 {len(results)}개')
    except Exception as e:
        print(f'  ⚠️ 최근 수집 실패: {e}')

    # 역사적 기간 정의 (겹치지 않게)
    historical = [
        ('1년전~1년1개월전 전체', today - timedelta(days=396), today - timedelta(days=335)),
        ('1년 전',               today - timedelta(days=372), today - timedelta(days=358)),
        ('1년 1개월 전',         today - timedelta(days=403), today - timedelta(days=389)),
    ]

    seen_historical = set()
    for label, start_dt, end_dt in historical:
        s = start_dt.isoformat()
        e = end_dt.isoformat()
        print(f'  📅 {label} ({s} ~ {e})...')
        try:
            posts = fetch_posts_apify(label, newer_than=s, older_than=e, limit=50)
            valid = []
            for p in posts:
                url = p.get('url', '')
                pub = p.get('published_at', '')
                if url in seen_historical:
                    continue
                if pub:
                    try:
                        pub_dt = date.fromisoformat(pub[:10])
                        if not (start_dt <= pub_dt <= end_dt):
                            continue
                    except:
                        pass
                seen_historical.add(url)
                valid.append(p)
            results.extend(valid)
            print(f'     → {len(valid)}개 (날짜 검증 후)')
        except Exception as e:
            print(f'  ⚠️ {label} 실패: {e}')

    print(f'  → 합계 {len(results)}개 수집')
    return results



# 제거할 무의미한 태그 패턴
FILTER_TAGS = {
    # 광고/협찬
    '광고', '협찬', '제작지원', 'ad', 'sponsored', 'pr', 'collaboration', 'partnership',
    # 계정명/브랜드 자기언급
    'eyesmag', 'knewnew', 'omuck', 'dailyfood', 'daily_fnb', 'idea82people', 'idea82',
    # 일반 노출용
    '맞팔', '좋아요', '팔로우', '선팔', 'follow', 'like', 'likes', 'instagood',
    'instadaily', 'photooftheday', 'love', 'beautiful',
    # F&B와 무관한 태그
    '러닝', '운동', '헬스', '요가', '필라테스', '여행', '일상', '패션', '뷰티', '메이크업',
    '미술', '예술', '아트', 'artwork', 'aesthetic', 'art', 'beauty', 'fashion',
    '음악', '영화', '드라마', '배우', '아이돌', '연예인',
    '롯데마트', '롯데슈퍼', '이마트', '홈플러스',
    'veuveClicquot', 'simonportejacquemus',
    # 사람 이름 패턴 (짧은 영문 이름)
    'jw', 'sson',
}

def _is_meaningful_tag(tag):
    """의미 없는 태그 필터링"""
    tag_lower = tag.lower().lstrip('#')
    # 필터 목록에 있으면 제거
    if tag_lower in FILTER_TAGS:
        return False
    # 숫자만 있으면 제거
    if tag_lower.isdigit():
        return False
    # 너무 짧으면 제거 (1자)
    if len(tag_lower) <= 1:
        return False
    return True


def extract_hashtags_from_competitors(competitor_data):
    print('📌 경쟁사 해시태그 집계 중...')

    # 기간별로 분리
    periods = ['현재', '현재부터 1달 전', '1년 전', '1년 1개월 전', '1년전~1년1개월전 전체']
    period_data = {p: [] for p in periods}
    for item in competitor_data:
        p = item.get('period', '')
        if p in period_data:
            period_data[p].append(item)

    all_results = []

    for period, items in period_data.items():
        if not items:
            continue
        counter = Counter()
        post_examples = {}
        for item in items:
            tags_raw = item.get('hashtags', '') or ''
            post_url = item.get('url', '')
            tags = [t.strip() for t in tags_raw.split(',') if t.strip()] if isinstance(tags_raw, str) else tags_raw
            for tag in tags:
                tag = tag.strip().lstrip('#')
                if not tag:
                    continue
                key = f'#{tag}'
                if not _is_meaningful_tag(key):
                    continue
                counter[key] += 1
                if key not in post_examples:
                    post_examples[key] = post_url

        # 최소 3회 이상 + 상위 30개
        qualified = [(tag, count) for tag, count in counter.most_common() if count >= 50][:30]
        for rank, (tag, count) in enumerate(qualified, 1):
            all_results.append({
                'rank': rank,
                'period': period,
                'hashtag': tag,
                'count': count,
                'example_url': post_examples.get(tag, ''),
            })

    print(f'  → {len(all_results)}개 집계 완료')
    return all_results


def collect_youtube_buzz():
    """유튜브에서 키워드별 최근 인기 영상 수 집계 → 버즈량으로 활용"""
    print('📺 유튜브 키워드 버즈량 수집 중...')
    results = []
    for kw in BUZZ_KEYWORDS:
        try:
            resp = requests.get(
                'https://www.googleapis.com/youtube/v3/search',
                params={
                    'key': YOUTUBE_KEY, 'q': kw, 'type': 'video',
                    'order': 'date', 'regionCode': 'KR',
                    'relevanceLanguage': 'ko', 'maxResults': 10,
                    'part': 'snippet',
                    'publishedAfter': f'{(date.today() - timedelta(days=7)).isoformat()}T00:00:00Z',
                },
                timeout=30,
            )
            count = len(resp.json().get('items', []))
            results.append({
                'keyword': kw,
                'mention_count': count,
                'platform': 'YouTube',
                'url': f'https://www.youtube.com/results?search_query={kw}&sp=EgIIAg%3D%3D',
            })
        except Exception as e:
            print(f'  ⚠️ 유튜브 버즈 {kw} 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results


def collect_naver_blog():
    """네이버 블로그 검색으로 키워드별 최근 언급량 수집"""
    print('🔍 네이버 블로그 버즈량 수집 중...')
    results = []
    if not NAVER_ID:
        print('  → 네이버 키 없음, 스킵')
        return results
    for kw in BUZZ_KEYWORDS:
        try:
            resp = requests.get(
                'https://openapi.naver.com/v1/search/blog.json',
                headers={'X-Naver-Client-Id': NAVER_ID, 'X-Naver-Client-Secret': NAVER_SECRET},
                params={'query': kw, 'display': 10, 'sort': 'date'},
                timeout=30,
            )
            items = resp.json().get('items', [])
            # 오늘 날짜 게시글 수 카운트
            today_count = sum(1 for i in items if TODAY.replace('-','') in i.get('postdate',''))
            results.append({
                'keyword': kw,
                'mention_count': today_count if today_count > 0 else len(items),
                'platform': 'Naver Blog',
                'url': f'https://search.naver.com/search.naver?where=blog&query={kw}&st=date',
            })
        except Exception as e:
            print(f'  ⚠️ 네이버 블로그 {kw} 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results


# 유튜브 수집 기준
YOUTUBE_KEYWORDS   = ['빵', '떡', '여행']
YOUTUBE_MIN_VIEWS  = 300000   # 30만 이상
YOUTUBE_DAYS       = 3        # 최근 3일 이내


def collect_youtube():
    print('📺 유튜브 급상승 수집 중...')
    published_after = (date.today() - timedelta(days=YOUTUBE_DAYS)).isoformat() + 'T00:00:00Z'
    candidate_ids = []
    id_to_meta = {}

    # 1단계: 키워드별 최근 영상 검색 (최대 50개씩)
    for kw in YOUTUBE_KEYWORDS:
        try:
            resp = requests.get(
                'https://www.googleapis.com/youtube/v3/search',
                params={
                    'key': YOUTUBE_KEY, 'q': kw, 'type': 'video',
                    'order': 'viewCount', 'regionCode': 'KR',
                    'relevanceLanguage': 'ko', 'maxResults': 50,
                    'part': 'snippet', 'publishedAfter': published_after,
                },
                timeout=30,
            )
            for item in resp.json().get('items', []):
                vid_id = item['id']['videoId']
                if vid_id not in id_to_meta:
                    candidate_ids.append(vid_id)
                    id_to_meta[vid_id] = {
                        'title':     item['snippet']['title'],
                        'channel':   item['snippet']['channelTitle'],
                        'keyword':   kw,
                        'url':       f'https://www.youtube.com/watch?v={vid_id}',
                        'platform':  'YouTube',
                        'thumbnail': item['snippet'].get('thumbnails', {}).get('high', {}).get('url', ''),
                    }
        except Exception as e:
            print(f'  ⚠️ 검색 실패 ({kw}): {e}')

    print(f'  → 후보 {len(candidate_ids)}개, 조회수 확인 중...')

    # 2단계: videos API로 조회수 일괄 조회 (50개씩 배치)
    results = []
    for i in range(0, len(candidate_ids), 50):
        batch = candidate_ids[i:i+50]
        try:
            stats_resp = requests.get(
                'https://www.googleapis.com/youtube/v3/videos',
                params={'key': YOUTUBE_KEY, 'id': ','.join(batch), 'part': 'statistics'},
                timeout=30,
            )
            for s in stats_resp.json().get('items', []):
                vid_id = s['id']
                views = int(s['statistics'].get('viewCount', 0))
                if views >= YOUTUBE_MIN_VIEWS:
                    meta = id_to_meta[vid_id].copy()
                    meta['views'] = views
                    results.append(meta)
        except Exception as e:
            print(f'  ⚠️ 조회수 조회 실패: {e}')

    # 조회수 내림차순 정렬
    results.sort(key=lambda x: x['views'], reverse=True)
    print(f'  → 조회수 {YOUTUBE_MIN_VIEWS:,}회 이상 {len(results)}개 수집')
    return results


def rank_items(category, items):
    if not items:
        return []
    if category == '경쟁 계정 성과':
        key = lambda x: x.get('likes', 0) + x.get('comments', 0) + x.get('views', 0)
    elif category == 'F&B 키워드 버즈량':
        key = lambda x: x.get('mention_count', 0)
    else:
        key = lambda x: x.get('views', 0)
    sorted_items = sorted(items, key=key, reverse=True)[:30]
    for i, item in enumerate(sorted_items, 1):
        item['rank'] = i
    return sorted_items


def set_row_heights(workbook, ws, start_row, end_row, height=150):
    try:
        workbook.batch_update({'requests': [{
            'updateDimensionProperties': {
                'range': {'sheetId': ws.id, 'dimension': 'ROWS',
                          'startIndex': start_row - 1, 'endIndex': end_row},
                'properties': {'pixelSize': height},
                'fields': 'pixelSize',
            }
        }]})
    except Exception as e:
        print(f'  ⚠️ 행 높이 설정 실패: {e}')


def save_to_sheets(workbook, competitor_data, hashtag_data, viral_data):

    # ① 경쟁 계정 성과
    ws1 = get_or_create_sheet(workbook, '인스타그램 레퍼런스 계정 성과', [
        '순위', '수집날짜', '발행일자', '기간', '계정명',
        '좋아요', '댓글', '조회수', '인게이지먼트', '캡션', '사용해시태그', '원본링크',
    ])
    rows1 = []
    for item in competitor_data:
        url = item.get('url', '')
        likes = item.get('likes', 0)
        comments = item.get('comments', 0)
        views = item.get('views', 0)
        link_formula = f'=HYPERLINK("{url}","링크")' if url else ''
        rows1.append([
            item.get('rank', ''), TODAY, item.get('published_at', ''), item.get('period', ''),
            item.get('account', ''), likes, comments, views,
            likes + comments, item.get('caption', ''), item.get('hashtags', ''), link_formula,
        ])
    if rows1:
        ws1.append_rows(rows1, value_input_option='USER_ENTERED')
    print(f'  ✅ 인스타그램 레퍼런스 계정 성과 {len(rows1)}행 저장')

    # ② 트렌딩 해시태그
    ws2 = get_or_create_sheet(workbook, '언급 많은 해시태그', ['순위', '수집날짜', '기간', '해시태그', '언급횟수', '대표게시물링크'])
    rows2 = [[i['rank'], TODAY, i.get('period',''), i['hashtag'], i['count'], i['example_url']] for i in hashtag_data]
    if rows2:
        ws2.append_rows(rows2, value_input_option='USER_ENTERED')
    print(f'  ✅ 언급 많은 해시태그 {len(rows2)}행 저장')

    # ④ 급상승 콘텐츠
    ws4 = get_or_create_sheet(workbook, '유튜브 급상승 콘텐츠', [
        '순위', '수집날짜', '플랫폼', '채널명', '제목', '조회수', '키워드', '링크', '썸네일',
    ])
    rows4 = []
    for item in viral_data:
        thumb = item.get('thumbnail', '')
        img_formula = f'=IMAGE("{thumb}",2)' if thumb else ''
        rows4.append([
            item.get('rank', ''), TODAY, item.get('platform', ''), item.get('channel', ''),
            item.get('title', ''), item.get('views', 0), item.get('keyword', ''),
            item.get('url', ''), img_formula,
        ])
    if rows4:
        start = len(ws4.get_all_values()) + 1
        ws4.append_rows(rows4, value_input_option='USER_ENTERED')
        set_row_heights(workbook, ws4, start, start + len(rows4) - 1, 150)
        ws4.spreadsheet.batch_update({'requests': [{
            'updateDimensionProperties': {
                'range': {'sheetId': ws4.id, 'dimension': 'COLUMNS', 'startIndex': 8, 'endIndex': 9},
                'properties': {'pixelSize': 200},
                'fields': 'pixelSize',
            }
        }]})
    print(f'  ✅ 유튜브 급상승 콘텐츠 {len(rows4)}행 저장')


if __name__ == '__main__':
    print(f'🚀 트렌드 수집 시작: {TODAY}\n')

    competitor_data = collect_competitors()
    hashtag_data    = extract_hashtags_from_competitors(competitor_data)
    viral_data      = rank_items('급상승 콘텐츠', collect_youtube())
    competitor_data = rank_items('경쟁 계정 성과', competitor_data)

    print(f'\n📊 수집 완료 — 경쟁계정 {len(competitor_data)} | 해시태그 {len(hashtag_data)} | 급상승 {len(viral_data)}\n')

    print('💾 Google Sheets 저장 중...')
    workbook = get_sheet()
    save_to_sheets(workbook, competitor_data, hashtag_data, viral_data)

    print('\n✅ 완료!')
