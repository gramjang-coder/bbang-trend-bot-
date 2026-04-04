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
    'cbi.busan',
    'dailyfashion_news',
    'yeomi.travel',
    'daytripkorea',
    'luxmag.kr',
    'seoulhotple',
    'hweekmag',
    'artart.today',
    'yomagazine_',
    'seoul_thehotple',
    '_tripgoing',
    'all.about.20s',
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
            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
            'textFormat': {'bold': True, 'fontSize': 12, 'foregroundColor': {'red': 0, 'green': 0, 'blue': 0}},
        })
    return ws


def run_apify(actor_id, input_data, timeout=240):
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
    # 계정 10개씩 배치 처리 (400 에러 방지)
    BATCH_SIZE = 10
    all_data = []
    accounts = COMPETITOR_ACCOUNTS
    for i in range(0, len(accounts), BATCH_SIZE):
        batch = accounts[i:i+BATCH_SIZE]
        params = {
            'directUrls': [f'https://www.instagram.com/{acc}/' for acc in batch],
            'resultsType': 'posts',
            'resultsLimit': limit,
        }
        if newer_than:
            params['onlyPostsNewerThan'] = newer_than
        if older_than:
            params['onlyPostsOlderThan'] = older_than
        try:
            data = run_apify('apify/instagram-scraper', params)
            all_data.extend(data)
        except Exception as e:
            print(f'  ⚠️ 배치 {i//BATCH_SIZE+1} 실패: {e}')

    seen = set()
    results = []
    for item in all_data:
        url = item.get('url', '')
        if url in seen:
            continue
        seen.add(url)
        results.append(parse_post(item, label))
    return results


def collect_competitors():
    print('👥 레퍼런스 계정 수집 중...')
    today = date.today()
    results = []

    print(f'  📥 최근 3일 게시물 수집 중...')
    try:
        recent = fetch_posts_apify('현재', limit=200)
        for p in recent:
            # Apify는 최신순으로 반환 → 날짜 필터 불필요
            # 인게이지먼트 조건만 체크
            if (p.get('views', 0) >= 100000 or
                    p.get('likes', 0) >= 1000 or
                    p.get('comments', 0) >= 50):
                results.append(p)
        print(f'     → {len(results)}개 (조건 충족)')
    except Exception as e:
        print(f'  ⚠️ 수집 실패: {e}')

    print(f'  → 합계 {len(results)}개 수집')
    return results

def extract_hashtags_from_competitors(competitor_data):
    print('📌 경쟁사 해시태그 집계 중...')

    # 기간별로 분리
    periods = ['현재']
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
        qualified = [(tag, count) for tag, count in counter.most_common() if count >= 2][:30]
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
YOUTUBE_KEYWORDS   = ['빵', '떡', '여행', '베이커리', '카페', '맛집', '디저트', '소금빵', '크루아상']
YOUTUBE_MIN_VIEWS  = 200000   # 20만 이상 (부족 시 10만으로 자동 완화)
YOUTUBE_DAYS       = 3        # 최근 3일 이내
YOUTUBE_TARGET     = 30       # 목표 수집 개수


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
                        'title':        item['snippet']['title'],
                        'channel':      item['snippet']['channelTitle'],
                        'keyword':      kw,
                        'published_at': item['snippet'].get('publishedAt', '')[:10],
                        'url':          f'https://www.youtube.com/watch?v={vid_id}',
                        'platform':     'YouTube',
                        'thumbnail':    item['snippet'].get('thumbnails', {}).get('high', {}).get('url', ''),
                    }
        except Exception as e:
            print(f'  ⚠️ 유튜브 검색 실패 ({kw}): {e}')
            import traceback; traceback.print_exc()

    print(f'  → 후보 {len(candidate_ids)}개, 조회수 확인 중...')

    # 2단계: videos API로 조회수 일괄 조회 (50개씩 배치)
    results = []
    for i in range(0, len(candidate_ids), 50):
        batch = candidate_ids[i:i+50]
        try:
            stats_resp = requests.get(
                'https://www.googleapis.com/youtube/v3/videos',
                params={'key': YOUTUBE_KEY, 'id': ','.join(batch), 'part': 'statistics,contentDetails'},
                timeout=30,
            )
            for s in stats_resp.json().get('items', []):
                vid_id = s['id']
                views = int(s['statistics'].get('viewCount', 0))
                if views < 100000:
                    continue

                # 영상 길이 파싱 (ISO 8601: PT1M30S 등)
                duration_str = s.get('contentDetails', {}).get('duration', 'PT0S')
                import re as _re
                hours   = int((_re.search(r'(\d+)H', duration_str) or type('', (), {'group': lambda *a: 0})()).group(1) or 0)
                minutes = int((_re.search(r'(\d+)M', duration_str) or type('', (), {'group': lambda *a: 0})()).group(1) or 0)
                seconds = int((_re.search(r'(\d+)S', duration_str) or type('', (), {'group': lambda *a: 0})()).group(1) or 0)
                total_sec = hours * 3600 + minutes * 60 + seconds
                is_short = total_sec <= 60

                meta = id_to_meta[vid_id].copy()
                meta['views']    = views
                meta['is_short'] = is_short
                meta['duration'] = duration_str
                results.append(meta)
        except Exception as e:
            print(f'  ⚠️ 조회수 조회 실패: {e}')

    # 조회수 내림차순 정렬
    results.sort(key=lambda x: x['views'], reverse=True)
    print(f'  → 10만 이상 후보 {len(results)}개')

    def dedup_by_channel(items, target):
        seen_channels = set()
        deduped = []
        for r in items:
            ch = r.get('channel', '')
            if ch in seen_channels:
                continue
            seen_channels.add(ch)
            deduped.append(r)
            if len(deduped) >= target:
                break
        return deduped

    # 한국 채널 필터 (제목 또는 채널명에 한글 포함)
    def has_korean(text):
        return bool(re.search(r'[가-힣]', text or ''))

    korean = [r for r in results if has_korean(r.get('title','')) or has_korean(r.get('channel',''))]
    print(f'  → 한국 채널 필터 후 {len(korean)}개')

    # 숏폼 / 롱폼 분리
    shorts   = [r for r in korean if r.get('is_short')]
    longform = [r for r in korean if not r.get('is_short')]

    # 롱폼: 20만 우선, 부족하면 10만으로 채움 (최대 20개)
    lf_above = [r for r in longform if r['views'] >= YOUTUBE_MIN_VIEWS]
    lf_dedup = dedup_by_channel(lf_above, 20)
    if len(lf_dedup) < 20:
        lf_dedup = dedup_by_channel(longform, 20)

    # 숏폼: 최대 10개
    sf_dedup = dedup_by_channel(shorts, 10)

    # 합치기 (롱폼 먼저)
    final = lf_dedup + sf_dedup
    # 순위 재부여
    for i, item in enumerate(final, 1):
        item['rank'] = i

    print(f'  → 롱폼 {len(lf_dedup)}개 + 숏폼 {len(sf_dedup)}개 = 최종 {len(final)}개')
    return final


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
        start_row = len(ws1.get_all_values()) + 1
        ws1.append_rows(rows1, value_input_option='USER_ENTERED')
        end_row = start_row + len(rows1) - 1
        # 글자 크기 12, 숫자 포맷
        ws1.format(f'A{start_row}:L{end_row}', {'textFormat': {'fontSize': 12}})
        ws1.format(f'F{start_row}:H{end_row}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
        ws1.format(f'I{start_row}:I{end_row}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
    print(f'  ✅ 인스타그램 레퍼런스 계정 성과 {len(rows1)}행 저장')

    # ② 트렌딩 해시태그
    ws2 = get_or_create_sheet(workbook, '언급 많은 해시태그', ['순위', '수집날짜', '기간', '해시태그', '언급횟수', '대표게시물링크'])
    rows2 = [[i['rank'], TODAY, i.get('period',''), i['hashtag'], i['count'], i['example_url']] for i in hashtag_data]
    if rows2:
        start_row2 = len(ws2.get_all_values()) + 1
        ws2.append_rows(rows2, value_input_option='USER_ENTERED')
        end_row2 = start_row2 + len(rows2) - 1
        ws2.format(f'A{start_row2}:F{end_row2}', {'textFormat': {'fontSize': 12}})
        ws2.format(f'E{start_row2}:E{end_row2}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
    print(f'  ✅ 언급 많은 해시태그 {len(rows2)}행 저장')

    # ④ 급상승 콘텐츠
    ws4 = get_or_create_sheet(workbook, '유튜브 급상승 콘텐츠', [
        '순위', '수집날짜', '업로드일자', '유형', '채널명', '제목', '조회수', '키워드', '링크', '썸네일',
    ])
    rows4 = []
    for item in viral_data:
        thumb = item.get('thumbnail', '')
        img_formula = f'=IMAGE("{thumb}",2)' if thumb else ''
        rows4.append([
            item.get('rank', ''), TODAY, item.get('published_at', ''),
            '숏폼' if item.get('is_short') else '롱폼',
            item.get('channel', ''), item.get('title', ''), item.get('views', 0),
            item.get('keyword', ''), item.get('url', ''), img_formula,
        ])
    if rows4:
        start = len(ws4.get_all_values()) + 1
        ws4.append_rows(rows4, value_input_option='USER_ENTERED')
        end_row4 = start + len(rows4) - 1
        set_row_heights(workbook, ws4, start, end_row4, 150)
        ws4.format(f'A{start}:I{end_row4}', {'textFormat': {'fontSize': 12}})
        ws4.format(f'G{start}:G{end_row4}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
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
    # 발행일자 최신순 정렬
    competitor_data.sort(key=lambda x: x.get('published_at', ''), reverse=True)
    for i, item in enumerate(competitor_data, 1):
        item['rank'] = i

    print(f'\n📊 수집 완료 — 경쟁계정 {len(competitor_data)} | 해시태그 {len(hashtag_data)} | 급상승 {len(viral_data)}\n')

    print('💾 Google Sheets 저장 중...')
    workbook = get_sheet()
    save_to_sheets(workbook, competitor_data, hashtag_data, viral_data)

    print('\n✅ 완료!')
