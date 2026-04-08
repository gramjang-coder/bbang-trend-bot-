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

TODAY = (datetime.utcnow() + timedelta(hours=9)).date().isoformat()

COMPETITOR_ACCOUNTS = [
    'knewnew.official', 'omuck.official', 'eyesmag', 'dailyfood_news',
    'daily_fnb', 'idea82people', 'cbi.busan', 'dailyfashion_news',
    'yeomi.travel', 'daytripkorea', 'luxmag.kr', 'seoulhotple',
    'hweekmag', 'artart.today', 'yomagazine_', 'seoul_thehotple',
    '_tripgoing', 'all.about.20s',
]

BUZZ_KEYWORDS = ['빵', '베이커리', '소금빵', '크루아상', '디저트카페', '빵집투어', '브런치']


def get_sheet():
    creds_info = json.loads(GCP_JSON)
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
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
        'likes':        max(0, item.get('likesCount', 0) or item.get('likes', 0) or 0),
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


def fetch_posts_apify(label, newer_than=None, older_than=None, limit=15):
    BATCH_SIZE = 3
    all_data = []
    for i in range(0, len(COMPETITOR_ACCOUNTS), BATCH_SIZE):
        batch = COMPETITOR_ACCOUNTS[i:i+BATCH_SIZE]
        params = {
            'directUrls': [f'https://www.instagram.com/{acc}/' for acc in batch],
            'resultsType': 'posts',
            'resultsLimit': limit,
        }
        try:
            data = run_apify('apify/instagram-scraper', params)
            all_data.extend(data)
            print(f'  배치 {i//BATCH_SIZE+1}: {len(data)}개')
        except Exception:
            # 배치 실패 시 계정 개별 재시도
            for acc in batch:
                try:
                    data = run_apify('apify/instagram-scraper', {
                        'directUrls': [f'https://www.instagram.com/{acc}/'],
                        'resultsType': 'posts',
                        'resultsLimit': limit,
                    })
                    all_data.extend(data)
                    print(f'  {acc}: {len(data)}개 (개별)')
                except Exception as e2:
                    print(f'  ⚠️ {acc} 실패: {e2}')
    seen = set()
    results = []
    for item in all_data:
        url = item.get('url', '')
        if url in seen:
            continue
        seen.add(url)
        results.append(parse_post(item, label))
    return results


def collect_competitors(target_date=None):
    print('👥 레퍼런스 계정 수집 중...')
    today = (datetime.utcnow() + timedelta(hours=9)).date()
    collect_date = target_date or (today - timedelta(days=1)).isoformat()
    print(f'  📥 {collect_date} 발행된 게시물 수집 중...')
    results = []
    try:
        recent = fetch_posts_apify('현재', limit=15)
        for p in recent:
            pub = p.get('published_at', '')
            if not pub:
                continue
            if pub[:10] != collect_date:
                continue
            results.append(p)
        print(f'     → {len(results)}개 (조건 충족)')
    except Exception as e:
        print(f'  ⚠️ 수집 실패: {e}')
    print(f'  → 합계 {len(results)}개 수집')
    return results


FILTER_TAGS = {
    '광고', '협찬', '제작지원', 'ad', 'sponsored', 'pr', 'collaboration', 'partnership',
    'eyesmag', 'knewnew', 'omuck', 'dailyfood', 'daily_fnb', 'idea82people', 'idea82',
    'cbi', 'dailyfashion', 'yeomi', 'daytripkorea', 'luxmag', 'seoulhotple',
    'hweekmag', 'artart', 'yomagazine', 'seoul_thehotple', 'tripgoing', 'all_about_20s',
    '맞팔', '좋아요', '팔로우', '선팔', 'follow', 'like', 'likes', 'instagood',
    'instadaily', 'photooftheday', 'love', 'beautiful',
}

def _is_meaningful_tag(tag):
    tag_lower = tag.lower().lstrip('#')
    if tag_lower in FILTER_TAGS:
        return False
    if tag_lower.isdigit():
        return False
    if len(tag_lower) <= 1:
        return False
    return True


def extract_keywords_from_captions(competitor_data):
    print('📌 캡션 키워드 추출 중...')
    STOP_WORDS = {
        '것', '수', '때', '곳', '등', '제', '저', '그', '이', '저희',
        '정말', '너무', '진짜', '아주', '매우', '더', '또', '도', '만',
        '광고', '협찬', '제작지원', '맞팔', '좋아요', '팔로우',
        '있는', '하는', '없는', '되는', '이번', '오늘', '지금', '여기',
    }
    counter = Counter()
    post_examples = {}
    for item in competitor_data:
        caption = item.get('caption', '') or ''
        post_url = item.get('url', '')
        if not caption:
            continue
        # 공백/특수문자 기준 분리, 한글 2자 이상 단어만
        words = [w for w in re.split(r'[\s\W#@]+', caption)
                 if len(w) >= 2 and re.search(r'^[가-힣]+$', w)]
        for word in words:
            if word in STOP_WORDS:
                continue
            counter[word] += 1
            if word not in post_examples:
                post_examples[word] = post_url
    qualified = [(w, c) for w, c in counter.most_common() if c >= 2][:30]
    results = []
    for rank, (w, count) in enumerate(qualified, 1):
        results.append({'rank': rank, 'keyword': w, 'count': count, 'example_url': post_examples.get(w, '')})
    print(f'  → {len(results)}개 키워드 추출 완료')
    return results


def collect_youtube():
    print('📺 유튜브 급상승 수집 중...')
    candidate_ids = []
    id_to_meta = {}

    for kw in ['빵', '떡', '여행']:
        for days_ago in range(0, 3):
            day_start = (date.today() - timedelta(days=days_ago+1)).isoformat() + 'T00:00:00Z'
            day_end   = (date.today() - timedelta(days=days_ago)).isoformat() + 'T00:00:00Z'
            try:
                resp = requests.get(
                    'https://www.googleapis.com/youtube/v3/search',
                    params={
                        'key': YOUTUBE_KEY, 'q': kw, 'type': 'video',
                        'order': 'viewCount', 'regionCode': 'KR',
                        'relevanceLanguage': 'ko', 'maxResults': 50,
                        'part': 'snippet',
                        'publishedAfter': day_start,
                        'publishedBefore': day_end,
                    },
                    timeout=30,
                )
                data = resp.json()
                if 'error' in data:
                    print(f'  ⚠️ [{kw} -{days_ago}일] 에러: {data["error"].get("message","")}')
                    continue
                items = data.get('items', [])
                print(f'  [{kw} -{days_ago}일]: {len(items)}개')
                for item in items:
                    vid_id = item.get('id', {}).get('videoId')
                    if not vid_id or vid_id in id_to_meta:
                        continue
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
                print(f'  ⚠️ [{kw} -{days_ago}일] 실패: {e}')

    print(f'  → 후보 {len(candidate_ids)}개, 조회수/길이 확인 중...')
    if not candidate_ids:
        return []

    all_videos = []
    for i in range(0, len(candidate_ids), 50):
        batch = candidate_ids[i:i+50]
        try:
            resp = requests.get(
                'https://www.googleapis.com/youtube/v3/videos',
                params={'key': YOUTUBE_KEY, 'id': ','.join(batch), 'part': 'statistics,contentDetails'},
                timeout=30,
            )
            data = resp.json()
            if 'error' in data:
                print(f'  ⚠️ videos API 에러: {data["error"].get("message","")}')
                continue
            for s in data.get('items', []):
                vid_id = s['id']
                views = int(s.get('statistics', {}).get('viewCount', 0))
                dur = s.get('contentDetails', {}).get('duration', 'PT0S')
                h = int(re.search(r'(\d+)H', dur).group(1)) if re.search(r'(\d+)H', dur) else 0
                m = int(re.search(r'(\d+)M', dur).group(1)) if re.search(r'(\d+)M', dur) else 0
                sec = int(re.search(r'(\d+)S', dur).group(1)) if re.search(r'(\d+)S', dur) else 0
                total_sec = h * 3600 + m * 60 + sec
                meta = id_to_meta[vid_id].copy()
                meta['views'] = views
                meta['is_short'] = total_sec <= 60
                all_videos.append(meta)
        except Exception as e:
            print(f'  ⚠️ videos API 실패: {e}')

    print(f'  → 전체 {len(all_videos)}개')
    korean = [v for v in all_videos if re.search(r'[가-힣]', v.get('title','') + v.get('channel',''))]
    print(f'  → 한국 콘텐츠 {len(korean)}개')
    korean.sort(key=lambda x: x['views'], reverse=True)  # 조회수 내림차순 (pick용)

    def pick(items, n):
        seen = set()
        out = []
        for v in items:
            ch = v.get('channel', '')
            if ch not in seen:
                seen.add(ch)
                out.append(v)
            if len(out) >= n:
                break
        return out

    longform = pick([v for v in korean if not v.get('is_short')], 20)
    shorts   = pick([v for v in korean if v.get('is_short')], 10)
    final = longform + shorts
    print(f'  → 롱폼 {len(longform)}개 + 숏폼 {len(shorts)}개 = 최종 {len(final)}개')
    return final


def rank_items(category, items):
    if not items:
        return []
    if category == '경쟝 계정 성과':
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

    # ① 인스타그램 레퍼런스 계정 성과
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
        rows1.append([
            item.get('rank', ''), TODAY, item.get('published_at', ''), item.get('period', ''),
            item.get('account', ''), likes, comments, views,
            likes + comments, item.get('caption', ''), item.get('hashtags', ''), url,
        ])
    if rows1:
        # 2행에 삽입 (최신 수집날짜가 항상 상단)
        ws1.insert_rows(rows1, row=2, value_input_option='USER_ENTERED')
        end_row = 1 + len(rows1)
        ws1.format(f'A2:L{end_row}', {'textFormat': {'fontSize': 12}})
        ws1.format(f'F2:H{end_row}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
        ws1.format(f'I2:I{end_row}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
    ws1.format('1:1', {'textFormat': {'fontSize': 12, 'bold': True}})
    try:
        ws1.spreadsheet.batch_update({'requests': [{'setBasicFilter': {'filter': {'range': {
            'sheetId': ws1.id, 'startRowIndex': 0, 'startColumnIndex': 0, 'endColumnIndex': 12
        }}}}]})
    except Exception as e:
        print(f'  ⚠️ 인스타 필터 설정 실패: {e}')
    print(f'  ✅ 인스타그램 레퍼런스 계정 성과 {len(rows1)}행 저장')

    # ② 언급 많은 키워드
    ws2 = get_or_create_sheet(workbook, '언급 많은 키워드', ['순위', '수집날짜', '키워드', '언급횟수', '대표게시물링크'])
    rows2 = [[i['rank'], TODAY, i['keyword'], i['count'], i['example_url']] for i in hashtag_data]
    if rows2:
        start_row2 = len(ws2.get_all_values()) + 1
        ws2.append_rows(rows2, value_input_option='USER_ENTERED')
        end_row2 = start_row2 + len(rows2) - 1
        ws2.format(f'A{start_row2}:F{end_row2}', {'textFormat': {'fontSize': 12}})
        ws2.format(f'D{start_row2}:D{end_row2}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
    print(f'  ✅ 언급 많은 키워드 {len(rows2)}행 저장')

    # ③ 유튜브 급상승 콘텐츠
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
        # 2행에 삽입 (최신 날짜가 항상 상단)
        ws4.insert_rows(rows4, row=2, value_input_option='USER_ENTERED')
        end_row4 = 1 + len(rows4)
        set_row_heights(workbook, ws4, 2, end_row4, 150)
        ws4.format(f'A2:I{end_row4}', {'textFormat': {'fontSize': 12}})
        ws4.format(f'G2:G{end_row4}', {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}})
        ws4.spreadsheet.batch_update({'requests': [{
            'updateDimensionProperties': {
                'range': {'sheetId': ws4.id, 'dimension': 'COLUMNS', 'startIndex': 8, 'endIndex': 9},
                'properties': {'pixelSize': 200},
                'fields': 'pixelSize',
            }
        }]})
    ws4.format('1:1', {'textFormat': {'fontSize': 12, 'bold': True}})
    try:
        ws4.spreadsheet.batch_update({'requests': [{'setBasicFilter': {'filter': {'range': {
            'sheetId': ws4.id, 'startRowIndex': 0, 'startColumnIndex': 0, 'endColumnIndex': 10
        }}}}]})
    except Exception as e:
        print(f'  ⚠️ 유튜브 필터 설정 실패: {e}')
    print(f'  ✅ 유튜브 급상승 콘텐츠 {len(rows4)}행 저장')


if __name__ == '__main__':
    print(f'🚀 트렌드 수집 시작: {TODAY}\n')

    # 어제 데이터 수집
    competitor_data = collect_competitors()

    # 누락 날짜 보완 (시트에 없는 날짜 확인 후 추가)
    try:
        workbook_check = get_sheet()
        ws_check = workbook_check.worksheet('인스타그램 레퍼런스 계정 성과')
        existing = ws_check.col_values(2)[1:]  # 수집날짜 컬럼
        today = (datetime.utcnow() + timedelta(hours=9)).date()
        for days_ago in range(2, 5):  # 최대 4일 전까지 보완
            check_date = (today - timedelta(days=days_ago)).isoformat()
            if check_date not in existing:
                print(f'  📅 누락 날짜 {check_date} 보완 수집 중...')
                extra = collect_competitors(target_date=(today - timedelta(days=days_ago+1)).isoformat())
                if extra:
                    # 수집날짜를 check_date로 변경해서 저장
                    for item in extra:
                        item['_collect_date'] = check_date
                    competitor_data.extend(extra)
    except Exception as e:
        print(f'  ⚠️ 누락 보완 실패: {e}')

    hashtag_data    = extract_keywords_from_captions(competitor_data)
    viral_data      = rank_items('급상승 콘텐츠', collect_youtube())

    # 유튜브: 업로드일자 최신순 정렬
    viral_data.sort(key=lambda x: x.get('published_at', ''), reverse=True)
    for i, item in enumerate(viral_data, 1):
        item['rank'] = i

    # 인스타: 발행일자 최신순 정렬
    competitor_data.sort(key=lambda x: x.get('published_at', ''), reverse=True)
    for i, item in enumerate(competitor_data, 1):
        item['rank'] = i

    print(f'\n📊 수집 완료 — 경쟁계정 {len(competitor_data)} | 해시태그 {len(hashtag_data)} | 급상승 {len(viral_data)}\n')

    print('💾 Google Sheets 저장 중...')
    workbook = get_sheet()
    save_to_sheets(workbook, competitor_data, hashtag_data, viral_data)

    print('\n✅ 완료!')
