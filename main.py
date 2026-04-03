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
    return gc.open_by_key(SHEET_ID)


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
        'views':        item.get('videoViewCount', 0) or item.get('videoPlayCount', 0) or 0,
        'url':          item.get('url', ''),
        'hashtags':     ', '.join((hashtags_raw or [])[:15]),
        'period':       period,
        'published_at': str(published) if published else '',
        'thumbnail':    thumbnail,
    }


def collect_competitors():
    print('👥 경쟁 계정 수집 중...')
    today = date.today()

    # 각 계정별로 최대한 많은 게시물 한 번에 수집 후 기간으로 분류
    all_posts = {}  # url -> post

    print('  📥 전체 게시물 수집 중 (계정당 최대 50개)...')
    try:
        data = run_apify('apify/instagram-scraper', {
            'directUrls': [f'https://www.instagram.com/{acc}/' for acc in COMPETITOR_ACCOUNTS],
            'resultsType': 'posts',
            'resultsLimit': 50,
        })
        for item in data:
            url = item.get('url', '')
            if url and url not in all_posts:
                all_posts[url] = parse_post(item, '')
        print(f'  → {len(all_posts)}개 고유 게시물 수집')
    except Exception as e:
        print(f'  ⚠️ 수집 실패: {e}')
        return []

    # 발행일자 기준으로 기간 분류
    today_dt = date.today()
    # 기간을 겹치지 않게 정의 (우선순위 순서대로 매칭)
    periods_range = [
        ('현재',                today_dt - timedelta(days=7),   today_dt),
        ('현재부터 1달 전',     today_dt - timedelta(days=30),  today_dt - timedelta(days=8)),
        ('1년전~1년1개월전 전체', today_dt - timedelta(days=396), today_dt - timedelta(days=335)),
        ('1년 전',              today_dt - timedelta(days=372), today_dt - timedelta(days=358)),
        ('1년 1개월 전',        today_dt - timedelta(days=403), today_dt - timedelta(days=389)),
    ]

    results = []
    for url, post in all_posts.items():
        pub = post.get('published_at', '')
        period_label = '기타'
        if pub:
            try:
                pub_dt = date.fromisoformat(pub[:10])
                for period_name, start, end in periods_range:
                    if start <= pub_dt <= end:
                        period_label = period_name
                        break
            except:
                pass
        p = post.copy()
        p['period'] = period_label
        results.append(p)

    # 기간별 카운트 출력
    from collections import Counter as C
    period_counts = C(r['period'] for r in results)
    for period, count in period_counts.items():
        print(f'  📅 {period}: {count}개')
    print(f'  → 합계 {len(results)}개')
    return results


def extract_hashtags_from_competitors(competitor_data):
    print('📌 경쟁사 해시태그 집계 중...')
    counter = Counter()
    post_examples = {}
    for item in competitor_data:
        tags_raw = item.get('hashtags', '') or ''
        post_url = item.get('url', '')
        tags = [t.strip() for t in tags_raw.split(',') if t.strip()] if isinstance(tags_raw, str) else tags_raw
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
        results.append({'rank': rank, 'hashtag': tag, 'count': count, 'example_url': post_examples.get(tag, '')})
    print(f'  → {len(results)}개 집계 완료')
    return results


def collect_twitter_buzz():
    print('🐦 트위터 버즈량 수집 중...')
    results = []
    try:
        data = run_apify('quacker/twitter-scraper', {'searchTerms': BUZZ_KEYWORDS, 'maxItems': 100, 'sort': 'Latest'})
        buzz = {}
        for item in data:
            text = item.get('full_text', '') or item.get('text', '')
            for kw in BUZZ_KEYWORDS:
                if kw in text:
                    buzz[kw] = buzz.get(kw, 0) + 1
        for kw, count in sorted(buzz.items(), key=lambda x: -x[1]):
            results.append({'keyword': kw, 'mention_count': count, 'platform': 'Twitter/X', 'url': f'https://twitter.com/search?q={kw}'})
    except Exception as e:
        print(f'  ⚠️ 트위터 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results


def collect_naver():
    print('🔍 네이버 트렌드 수집 중...')
    results = []
    if not NAVER_ID:
        return results
    try:
        resp = requests.post(
            'https://openapi.naver.com/v1/datalab/search',
            headers={'X-Naver-Client-Id': NAVER_ID, 'X-Naver-Client-Secret': NAVER_SECRET, 'Content-Type': 'application/json'},
            json={'startDate': TODAY, 'endDate': TODAY, 'timeUnit': 'date',
                  'keywordGroups': [{'groupName': kw, 'keywords': [kw]} for kw in BUZZ_KEYWORDS]},
            timeout=30,
        )
        for result in resp.json().get('results', []):
            for point in result.get('data', []):
                results.append({'keyword': result['title'], 'mention_count': point.get('ratio', 0),
                                'platform': 'Naver', 'url': f'https://search.naver.com/search.naver?query={result["title"]}'})
    except Exception as e:
        print(f'  ⚠️ 네이버 실패: {e}')
    print(f'  → {len(results)}개 수집')
    return results


def collect_youtube():
    print('📺 유튜브 수집 중...')
    results = []
    for kw in BUZZ_KEYWORDS[:5]:
        try:
            resp = requests.get(
                'https://www.googleapis.com/youtube/v3/search',
                params={'key': YOUTUBE_KEY, 'q': kw, 'type': 'video', 'order': 'viewCount',
                        'regionCode': 'KR', 'relevanceLanguage': 'ko', 'maxResults': 5, 'part': 'snippet'},
                timeout=30,
            )
            for item in resp.json().get('items', []):
                vid_id = item['id']['videoId']
                results.append({
                    'title': item['snippet']['title'], 'channel': item['snippet']['channelTitle'],
                    'keyword': kw, 'views': 0, 'url': f'https://www.youtube.com/watch?v={vid_id}',
                    'platform': 'YouTube',
                    'thumbnail': item['snippet'].get('thumbnails', {}).get('high', {}).get('url', ''),
                })
        except Exception as e:
            print(f'  ⚠️ 유튜브 {kw} 실패: {e}')
    print(f'  → {len(results)}개 수집')
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


def save_to_sheets(workbook, competitor_data, hashtag_data, buzz_data, viral_data):

    # ① 경쟁 계정 성과
    ws1 = get_or_create_sheet(workbook, '경쟁계정성과', [
        '순위', '수집날짜', '발행일자', '기간', '계정명',
        '좋아요', '댓글', '조회수', '캡션', '사용해시태그', '원본링크',
    ])
    rows1 = []
    for item in competitor_data:
        url = item.get('url', '')
        link_formula = f'=HYPERLINK("{url}","링크")' if url else ''
        rows1.append([
            item.get('rank', ''), TODAY, item.get('published_at', ''), item.get('period', ''),
            item.get('account', ''), item.get('likes', 0), item.get('comments', 0), item.get('views', 0),
            item.get('caption', ''), item.get('hashtags', ''), link_formula,
        ])
    if rows1:
        ws1.append_rows(rows1, value_input_option='USER_ENTERED')
    print(f'  ✅ 경쟁계정성과 {len(rows1)}행 저장')

    # ② 트렌딩 해시태그
    ws2 = get_or_create_sheet(workbook, '트렌딩해시태그', ['순위', '수집날짜', '해시태그', '언급횟수', '대표게시물링크'])
    rows2 = [[i['rank'], TODAY, i['hashtag'], i['count'], i['example_url']] for i in hashtag_data]
    if rows2:
        ws2.append_rows(rows2, value_input_option='USER_ENTERED')
    print(f'  ✅ 트렌딩해시태그 {len(rows2)}행 저장')

    # ③ F&B 키워드 버즈량
    ws3 = get_or_create_sheet(workbook, 'FB키워드버즈량', ['순위', '수집날짜', '키워드', '언급량', '플랫폼', '링크'])
    rows3 = [[i.get('rank',''), TODAY, i['keyword'], i['mention_count'], i['platform'], i['url']] for i in buzz_data]
    if rows3:
        ws3.append_rows(rows3, value_input_option='USER_ENTERED')
    print(f'  ✅ FB키워드버즈량 {len(rows3)}행 저장')

    # ④ 급상승 콘텐츠
    ws4 = get_or_create_sheet(workbook, '급상승콘텐츠', [
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
    print(f'  ✅ 급상승콘텐츠 {len(rows4)}행 저장')


if __name__ == '__main__':
    print(f'🚀 트렌드 수집 시작: {TODAY}\n')

    competitor_data = collect_competitors()
    hashtag_data    = extract_hashtags_from_competitors(competitor_data)
    buzz_data       = rank_items('F&B 키워드 버즈량', collect_twitter_buzz() + collect_naver())
    viral_data      = rank_items('급상승 콘텐츠', collect_youtube())
    competitor_data = rank_items('경쟁 계정 성과', competitor_data)

    print(f'\n📊 수집 완료 — 경쟁계정 {len(competitor_data)} | 해시태그 {len(hashtag_data)} | 버즈량 {len(buzz_data)} | 급상승 {len(viral_data)}\n')

    print('💾 Google Sheets 저장 중...')
    workbook = get_sheet()
    save_to_sheets(workbook, competitor_data, hashtag_data, buzz_data, viral_data)

    print('\n✅ 완료!')
