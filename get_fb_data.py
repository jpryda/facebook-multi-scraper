import datetime
import calendar
import requests
import time
import sys
import os
import threading
import Queue
import math
from dateutil import tz

import pandas as pd
from scipy.stats import norm


#  At the cost of performance, set these to true for more precise data: reaction type breakdown and public shares across Facebook
#  Overall reactions per post are already pulled
GET_SPECIFIC_REACTIONS_BOOL = False
GET_PUBLIC_SHARES_BOOL = False

# Page IDs to be scraped, defined by page's Facebook handle.  
PAGE_IDS_TO_SCRAPE = [
        'nytimes',
        'vicenews',
        'bbcnews',
        'TheSkimm',
        'cnn',
        'NBCNews',
        'financialtimes',
        'washingtonpost',
        'theguardian',
        'timesandsundaytimes',
        ]

# Additional personal metrics are pulled for owned pages in keys below who also exist in PAGE_IDS_TO_SCRAPE
# Temporary token: https://developers.facebook.com/tools/explorer
# Permanent/Business Page token: https://stackoverflow.com/questions/17197970/facebook-permanent-page-access-token/28418469#28418469
OWNED_PAGES_TOKENS = {
    'jpryda': os.environ['MY_TOKEN'],       # Token as an environmental variable: export MY_TOKEN = 'abc-my-token'
    # 'MyPage1': 'my-hardcoded-token'       # Hardcoded token
}

TIMEZONE = 'America/New_York'
API_VERSION = '2.7'


# Set display precision when printing Pandas dataframes
pd.set_option('precision',1)
# Deal with scientific notation
pd.options.display.float_format = '{:20,.0f}'.format
# Don't wrap dataframe when printing to console
pd.set_option('display.expand_frame_repr', False)


def request_until_succeed(url):
    max_attempts = 3
    attempts = 0
    success = False
    while success == False and attempts < max_attempts:
        attempts = attempts + 1
        try:
            response = requests.get(url)
            if response.status_code == 200:
                success = True
        except Exception as e:
            print e
            print 'Error for URL {} | {} | attempt {} of {}'.format(url, datetime.datetime.now(), attempts, max_attempts)
            if attempts == max_attempts:
                raise Exception('Failed after {} attempts | {}'.format(attempts, url))    
            time.sleep(3)
    return response
     

# Handle non-ASCII characters when writing to csv
def unicode_normalize(text):
    return text.translate({ 0x2018:0x27, 0x2019:0x27, 0x201C:0x22, 0x201D:0x22, 0xa0:0x20 }).encode('utf-8')


def get_fb_page_video_data(page_id, access_token, num_posts=100, until=''):
    base = 'https://graph.facebook.com/v{}'.format(API_VERSION)
    node = '/{}/videos'.format(page_id)
    fields = '/?fields=title,description,created_time,id,comments.limit(0).summary(true),likes.limit(0).summary(true),reactions.limit(0).summary(true),permalink_url,live_status,status'
    parameters = '&limit={}&access_token={}&until={}'.format(num_posts, access_token, until)
    url = base + node + fields + parameters

    data = request_until_succeed(url).json()
    return data


def get_fb_page_post_data(page_id, access_token, num_posts=100, until=''):
    # Shares on videos must be grabbed from the /posts endpoint; unavailable from the /videos endpoint
    base = 'https://graph.facebook.com/v{}'.format(API_VERSION)
    node = '/{}/posts'.format(page_id)
    fields = '/?fields=message,link,created_time,type,name,id,comments.limit(0).summary(true),shares,reactions.limit(0).summary(true)'
    parameters = '&limit={}&access_token={}&until={}'.format(num_posts, access_token, until)
    url = base + node + fields + parameters
    
    data = request_until_succeed(url).json()
    return data
   

def get_specific_reactions_for_post(status_id, access_token):
    # Reaction types are only accessible at an individual post's endpoint
    base = 'https://graph.facebook.com/v{}'.format(API_VERSION)
    node = '/{}'.format(status_id)
    reactions = '/?fields=' \
                    'reactions.type(LIKE).limit(0).summary(total_count).as(like)'\
                    ',reactions.type(LOVE).limit(0).summary(total_count).as(love)'\
                    ',reactions.type(WOW).limit(0).summary(total_count).as(wow)'\
                    ',reactions.type(HAHA).limit(0).summary(total_count).as(haha)'\
                    ',reactions.type(SAD).limit(0).summary(total_count).as(sad)'\
                    ',reactions.type(ANGRY).limit(0).summary(total_count).as(angry)'
    parameters = '&access_token={}'.format(access_token)
    url = base + node + reactions + parameters

    data = request_until_succeed(url).json()
    return data


def get_insights_for_post(object_id, access_token, fields, period='', since=''):
    base = 'https://graph.facebook.com/v{}'.format(API_VERSION)
    node = '/{}/insights/'.format(object_id)
    parameters = '?access_token={}&period={}&since={}&date_format=U'.format(access_token, period, since)
    url = base + node + fields + parameters
    
    data = request_until_succeed(url)
    if data is not None:
        return data.json()
    else:
        raise Exception('No Post Insights Data')


def get_insights_for_video(video_id, access_token, period='lifetime'):
    base = 'https://graph.facebook.com/v{}'.format(API_VERSION)
    node = '/{}/video_insights'.format(video_id)
    fields = ''
    parameters = '?access_token={}&period={}'.format(access_token, period)
    url = base + node + fields + parameters
    
    data = request_until_succeed(url).json()
    return data


def get_fb_url_shares_comments(access_token, url):
    # Remove pound signs from URL which mess up FB API
    url = url.replace('#','')
    base = 'https://graph.facebook.com/v{}'.format(API_VERSION)
    node = ''
    fields = '/?id={}'.format(url)
    parameters = '&access_token={}'.format(access_token)
    url = base + node + fields + parameters

    data = request_until_succeed(url).json()
    return data


def get_insights_for_page(access_token, metrics, page_id, period, start_date, excl_end_date):
    base = 'https://graph.facebook.com/v{}'.format(FB_API_VERSION)
    node = '/{}/insights'.format(page_id)
    fields = '/{}'.format(metrics)
    period_string = 'period={}&since={}&until={}'.format(period, start_date, excl_end_date)
    parameters = '?{}&access_token={}'.format(period_string, access_token)
    
    url = base + node + fields + parameters
    data = request_until_succeed(url).json()
    
    return data


# def posix_to_timezone(posix_int, to_timezone):
#     utc_datetime = datetime.utcfromtimestamp(posix_int)
#     from_zone = tz.gettz('UTC')
#     to_zone = tz.gettz(to_timezone)
#     to_datetime = utc_datetime.replace(tzinfo=from_zone).astimezone(to_zone)
#     return to_datetime.replace(tzinfo=None) #Remove timezone component to allow for comparison with local time


# def posix_to_iso(posix_int):
#     return datetime.datetime.utcfromtimestamp(posix_int).strftime('%Y-%m-%dT%H:%M:%S+0000')


def utc_to_timezone(utc_datetime_string, to_timezone):
    utc_datetime = datetime.datetime.strptime(utc_datetime_string,'%Y-%m-%dT%H:%M:%S+0000')
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz(to_timezone)
    est_datetime = utc_datetime.replace(tzinfo=from_zone).astimezone(to_zone)
    return est_datetime.replace(tzinfo=None) #Remove timezone component to allow for comparison with local time

# Not used right now
def utc_to_local(utc_datetime_string):
    utc_datetime = datetime.datetime.strptime(utc_datetime_string,'%Y-%m-%dT%H:%M:%S+0000')
    from_zone = tz.gettz('UTC')
    to_zone = tz.tzlocal()
    local_datetime = utc_datetime.replace(tzinfo=from_zone).astimezone(to_zone)
    return local_datetime.replace(tzinfo=None) #Remove timezone component to allow for comparison with local time

# For specification of 'until' parameter at commandline
def local_to_utc(local_date):
    from_zone = tz.tzlocal()
    to_zone = tz.gettz('UTC')
    utc_datetime = local_date.replace(tzinfo=from_zone).astimezone(to_zone)
    return utc_datetime.replace(tzinfo=None) #Remove timezone component to allow for comparison with local time

'''
Calculate confidence interval lower bound as scoring system to balance balance proportion of successes (e.g. clicks) with the uncertainty of a small number
i.e. ci_lower_bound(5, 10, 0.95) < ci_lower_bound(100, 200, 0.95).  For more info see http://www.evanmiller.org/how-not-to-sort-by-average-rating.html
'''
def ci_lower_bound(pos, n, confidence):
    if n == 0:
        return 0
    elif n > pos:
        z = norm.ppf((1-(1-confidence)/2), loc=0, scale=1)
        phat = float(pos)/n
        return (phat + z*z/(2*n) - z * math.sqrt((phat*(1-phat)+z*z/(4*n))/n)) / (1+z*z/n)
    else:
        return 0


def process_fb_page_video(video, access_token, page_id):
    if video.get('status').get('video_status') == 'expired':
        return None

    timestamp = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + '+0000'
    video_id = video['id']
    utc_video_published = video['created_time']

    video_title = None if 'title' not in video.keys() else unicode_normalize(video['title']).decode('utf-8','ignore').encode('utf-8')
    video_description = None if 'description' not in video.keys() else unicode_normalize(video['description']).decode('utf-8','ignore').encode('utf-8')
    video_permalink = video['permalink_url']

    num_likes = 0 if 'likes' not in video else video['likes']['summary']['total_count']
    num_reactions = 0 if 'reactions' not in video else video['reactions']['summary']['total_count']
    num_comments = 0 if 'comments' not in video or video.get('comments').get('summary').get('total_count') is None else video['comments']['summary']['total_count']

    live_boolean = False if video.get('live_status') is None else True

    # Set Insights default values if a competitor or a Facebook Live Video
    total_3s_views = None
    total_10s_views = None
    total_complete_views = None
    total_video_impressions = None
    total_video_avg_time_watched = None
    ten_three_s_ratio = None
    complete_three_s_ratio = None
    total_video_impressions_fan = None
    total_non_fan_impressions_rate = None
    total_video_views_paid = None

    # Get insights for videos iff they are our OWN and also NOT Live videos which have no data
    if page_id.lower() in [x.lower() for x in OWNED_PAGES_TOKENS.keys()]:
        video_insights = get_insights_for_video(video_id, access_token, 'lifetime')

        if len(video_insights['data']) > 0:
            for metric_result in video_insights['data']:
                if metric_result['name'] == 'total_video_views':
                    total_3s_views =  metric_result['values'][0]['value']
                if metric_result['name'] == 'total_video_10s_views':
                    total_10s_views = metric_result['values'][0]['value']
                if metric_result['name'] == 'total_video_complete_views':
                    total_complete_views = metric_result['values'][0]['value']
                if metric_result['name'] == 'total_video_avg_time_watched':
                    total_video_avg_time_watched = float(metric_result['values'][0]['value'])/1000
                if metric_result['name'] == 'total_video_impressions':
                    total_video_impressions = metric_result['values'][0]['value']
                if metric_result['name'] == 'total_video_impressions_fan':
                    total_video_impressions_fan = metric_result['values'][0]['value']
                if metric_result['name'] == 'total_video_views_paid':
                    total_video_views_paid = metric_result['values'][0]['value']

            total_non_fan_impressions = total_video_impressions - total_video_impressions_fan
            total_non_fan_impressions_rate = None if total_video_impressions == 0 else float(total_non_fan_impressions)/float(total_video_impressions) * 100
            ten_three_s_ratio = None if total_3s_views == 0 else float(total_10s_views)/float(total_3s_views) * 100
            complete_three_s_ratio = None if total_3s_views == 0 else float(total_complete_views)/float(total_3s_views) * 100
            engagement_rate = None if total_3s_views == 0 else float(num_reactions + num_comments)/float(total_3s_views) * 100 # Video endpoint doesn't have shares

    crossposted_boolean = True if total_3s_views is None and live_boolean is False else False

    scraped_row = {
        'Page': page_id,
        'Video ID': video_id,
        'Published': utc_video_published,
        'Live Video': live_boolean,
        'Crossposted Video': crossposted_boolean,
        'Headline': video_title,
        'Caption': video_description,
        'Num Likes': num_likes,
        'Num Reactions': num_reactions,
        'Num Comments': num_comments,
        '3s Views': total_3s_views,
        '10s Views': total_10s_views,
        'Complete Views': total_complete_views,
        'Total Paid Views': total_video_views_paid,
        '10s/3s Views (%)': ten_three_s_ratio,
        'Complete/3s Views (%)': complete_three_s_ratio,
        'Impressions': total_video_impressions,
        'Impression Rate Non-Likers (%)': total_non_fan_impressions_rate,
        'Avg View Time': total_video_avg_time_watched,
        'Link': video_permalink,
        'Timestamp': timestamp
    }
    return scraped_row


def process_fb_page_video_all_metrics(video, access_token, page_id):
    timestamp = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + '+0000'
    video_id = video['id']
    video_title = None if 'title' not in video.keys() else unicode_normalize(video['title']).decode('utf-8','ignore').encode('utf-8')
    video_description = None if 'description' not in video.keys() else unicode_normalize(video['description']).decode('utf-8','ignore').encode('utf-8')
    utc_video_published = video['created_time']
    video_permalink = video['permalink_url']

    num_likes = 0 if 'likes' not in video else video['likes']['summary']['total_count']
    num_reactions = 0 if 'reactions' not in video else video['reactions']['summary']['total_count']
    num_comments = 0 if 'comments' not in video or video.get('comments').get('summary').get('total_count') is None else video['comments']['summary']['total_count']

    live_boolean = False if video.get('live_status') is None else True

    scraped_row = {
        'Page': page_id,
        'Video ID': video_id,
        'Published': utc_video_published,
        'Live Video': live_boolean,
        'Headline': video_title,
        'Caption': video_description,
        'Num Likes': num_likes,
        'Num Reactions': num_reactions,
        'Num Comments': num_comments,
        'Link': video_permalink,
        'Timestamp': timestamp
    }

    if page_id.lower() in [x.lower() for x in OWNED_PAGES_TOKENS.keys()]:
        video_insights = get_insights_for_video(video_id, access_token, 'lifetime')

        if len(video_insights['data']) > 0:
            
            for metric in video_insights['data']:
                
                # Define metric name and add to scraped_row
                metric_name = metric['name'].replace('.','')
                metric_value = metric['values'][0]['value']
                # Elasticsearch doesn't accept periods within keys
                if isinstance(metric_value, dict):
                    metric_value = { x.replace('.', ''): metric_value[x] for x in metric_value.keys() }
                scraped_row[metric_name] = metric_value

            # Unpack dicts of important metrics.  !Actually Kibana unpacks these for us so unnecessary!
            scraped_row['total_video_views_by_crossposted'] = scraped_row['total_video_views_by_distribution_type'].get('crossposted')
            scraped_row['total_video_views_by_page_owned'] = scraped_row['total_video_views_by_distribution_type'].get('page_owned')
            scraped_row['total_video_views_by_page_shared'] = scraped_row['total_video_views_by_distribution_type'].get('shared')
            #del scraped_row['total_video_views_by_distribution_type']

            scraped_row['total_video_impressions_non_fan'] = scraped_row['total_video_impressions'] - scraped_row['total_video_impressions_fan']
            scraped_row['total_non_fan_impressions_rate'] = None if scraped_row['total_video_impressions'] == 0 else float(scraped_row['total_video_impressions_non_fan'])/float(scraped_row['total_video_impressions']) * 100
            scraped_row['ten_three_s_ratio'] = None if scraped_row['total_video_views'] == 0 else float(scraped_row['total_video_10s_views'])/float(scraped_row['total_video_views']) * 100
            scraped_row['complete_three_s_ratio'] = None if scraped_row['total_video_views'] == 0 else float(scraped_row['total_video_complete_views'])/float(scraped_row['total_video_views']) * 100

        scraped_row['Crossposted Video'] = True if scraped_row.get('total_video_views') is None and live_boolean is False else False
        if scraped_row.get('total_video_views') is not None:
            scraped_row['Video Views'] = scraped_row['total_video_views']
            #del scraped_row['total_video_views']
    return scraped_row


def process_fb_page_post(status, access_token, page_id):
    timestamp = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + '+0000'
    status_id = status['id']
    status_message = None if 'message' not in status.keys() else unicode_normalize(status['message']).decode('utf-8','ignore').encode('utf-8')
    post_title = None if 'name' not in status.keys() else unicode_normalize(status['name']).decode('utf-8','ignore').encode('utf-8')
    status_type = status['type']
    status_link = None if 'link' not in status.keys() else unicode_normalize(status['link'])
    
    # Time needs special care since it's in UTC
    utc_status_published = status['created_time']
        
    num_reactions = None if 'reactions' not in status else status['reactions']['summary']['total_count']
    num_comments = None if 'comments' not in status or status.get('comments').get('summary').get('total_count') is None else status['comments']['summary']['total_count']
    num_shares = None if 'shares' not in status else status['shares']['count']


    num_likes = num_loves = num_wows = num_hahas = num_sads = num_angrys = None
    unique_link_clicks = None
    total_unique_impressions = None
    ctr = None
    post_video_views = None
    paid_unique_impressions = None
    non_fan_unique_impressions_rate = None
    hide_clicks = None
    hide_all_clicks = None
    hide_rate = None
    public_num_shares = None
    ctr_lb_confidence = None
    engagement_rate = None
    engage_lb_confidence = None
    organic_unique_impressions = None
    public_num_shares = None
    
    if (GET_PUBLIC_SHARES_BOOL):
        # Get number of shares across all of Facebook
        if status_link is not None:
            public_num_shares_comments = get_fb_url_shares_comments(access_token, status_link)
            if 'share' in public_num_shares_comments:
                public_num_shares = public_num_shares_comments.get('share').get('share_count')


    if (GET_SPECIFIC_REACTIONS_BOOL):
        # Reactions only exists after implementation date: http://newsroom.fb.com/news/2016/02/reactions-now-available-globally/
        reactions = get_specific_reactions_for_post(status_id, access_token) if utc_status_published > '2016-02-24 00:00:00' else {}
        num_likes = 0 if 'like' not in reactions else reactions['like']['summary']['total_count']       
        # Special case: Set number of Likes to Number of reactions for pre-reaction statuses
        num_likes = num_reactions if utc_status_published < '2016-02-24 00:00:00' else num_likes
        
        num_loves = 0 if 'love' not in reactions else reactions['love']['summary']['total_count']
        num_wows = 0 if 'wow' not in reactions else reactions['wow']['summary']['total_count']
        num_hahas = 0 if 'haha' not in reactions else reactions['haha']['summary']['total_count']
        num_sads = 0 if 'sad' not in reactions else reactions['sad']['summary']['total_count']
        num_angrys = 0 if 'angry' not in reactions else reactions['angry']['summary']['total_count']


    # If not one of our own pages or a pesky cover photo
    if (page_id.lower() not in [x.lower() for x in OWNED_PAGES_TOKENS.keys()]) or (post_title is not None and 'cover photo' in post_title and status_type=='photo'):

        scraped_row = {
            'Page': page_id,
            'Published': utc_status_published,
            'Num Shares': num_shares,
            'Num Reactions': num_reactions,
            'Type': status_type,
            'Headline': post_title,
            'Caption': status_message,
            'Link': status_link,
            'Num Likes': num_likes, 
            'Num Comments': num_comments, 
            'Num Loves': num_loves, 
            'Num Wows': num_wows, 
            'Num Hahas': num_hahas, 
            'Num Sads': num_sads, 
            'Num Angrys': num_angrys,
            'Lifetime Public Num Shares': public_num_shares,
            'Post ID': status_id,
            'Timestamp': timestamp
        }
        return scraped_row

    # Iff one of our own pages, read insights too
    elif page_id.lower() in [x.lower() for x in OWNED_PAGES_TOKENS.keys()]:

        fields = 'post_consumptions_by_type_unique'\
                ',post_impressions_by_paid_non_paid_unique'\
                ',post_video_views'\
                ',post_impressions_fan_unique'\
                ',post_negative_feedback_by_type_unique'

        try:
            insights = get_insights_for_post(status_id, access_token, fields, 'lifetime')

            unique_link_clicks = 0 if 'link clicks' not in insights['data'][0]['values'][0]['value'] else insights['data'][0]['values'][0]['value'].get('link clicks')
            total_unique_impressions = insights['data'][1]['values'][0]['value'].get('total')
            ctr = None if total_unique_impressions == 0 else (float(unique_link_clicks)/float(total_unique_impressions)) * 100
            ctr_lb_confidence = None if status_type != 'link' else ci_lower_bound(unique_link_clicks, total_unique_impressions, 0.95) * 100

            paid_unique_impressions = insights['data'][1]['values'][0]['value'].get('paid')
            organic_unique_impressions = insights['data'][1]['values'][0]['value'].get('unpaid')
            post_video_views = insights['data'][2]['values'][0]['value']
            fan_unique_impressions = insights['data'][3]['values'][0]['value']
            non_fan_unique_impressions = total_unique_impressions - fan_unique_impressions
            non_fan_unique_impressions_rate = None if total_unique_impressions == 0 else (float(non_fan_unique_impressions)/float(total_unique_impressions)) * 100
            hide_clicks = 0 if 'hide_clicks' not in insights['data'][4]['values'][0]['value'] else insights['data'][4]['values'][0]['value'].get('hide_clicks')
            hide_all_clicks = 0 if 'hide_all_clicks' not in insights['data'][4]['values'][0]['value'] else insights['data'][4]['values'][0]['value'].get('hide_all_clicks')
            hide_rate = None if total_unique_impressions == 0 else (float(hide_clicks + hide_all_clicks)/float(total_unique_impressions)) * 100

            # Engagement Rate
            if num_shares is not None and num_reactions is not None and num_comments is not None:
                total_engagement = num_shares + num_reactions + num_comments
                if status_type != 'video':
                    engagement_rate = None if total_unique_impressions == 0 else float(total_engagement)/float(total_unique_impressions) * 100
                    engage_lb_confidence = ci_lower_bound(total_engagement, total_unique_impressions, 0.95) * 100
                if status_type == 'video':
                    engagement_rate = None if post_video_views == 0 else float(total_engagement)/float(post_video_views) * 100
                    engage_lb_confidence = ci_lower_bound(total_engagement, post_video_views, 0.95) * 100
    
            ## Counts of each reaction separately.  Can comment out for speed's sake 
            

        except Exception as e:
            print e

    scraped_row = {
            'Page': page_id,
            'Published': utc_status_published,
            'Unique Impressions': total_unique_impressions,
            'Paid Unique Impressions': paid_unique_impressions,
            'Impression Rate Non-Likers (%)': non_fan_unique_impressions_rate,
            'Unique Link Clicks': unique_link_clicks,
            'CTR (%)': ctr,
            'Adjusted CTR (%)': ctr_lb_confidence,
            'Num Shares': num_shares,
            'Num Reactions': num_reactions,
            'Hide Rate (%)': hide_rate,
            'Hide Clicks': hide_clicks,
            'Hide All Clicks': hide_all_clicks, 
            'Type': status_type,
            'Engagement Rate (%)': engagement_rate,
            'Adjusted Engagement Rate (%)': engage_lb_confidence,
            'Video Views': post_video_views,
            'Headline': post_title.decode('utf-8','ignore').encode('utf-8') if post_title is not None else None,
            'Caption': status_message.decode('utf-8','ignore').encode('utf-8') if status_message is not None else None,
            'Link': status_link,
            'Num Likes': num_likes, 
            'Num Comments': num_comments, 
            'Num Loves': num_loves, 
            'Num Wows': num_wows, 
            'Num Hahas': num_hahas, 
            'Num Sads': num_sads, 
            'Num Angrys': num_angrys,
            'Lifetime Public Num Shares': public_num_shares,
            'Post ID': status_id,
            'Organic Unique Impressions': organic_unique_impressions,
            'Timestamp': timestamp
    }
    return scraped_row


def scrape_single_fb_page_items(page_id, from_date, until_date, access_token, scrape_function, process_item_function):
    num_processed = 0   # keep a count on how many we've processed
    scraped_rows_list = []

    scrape_starttime = datetime.datetime.now()

    items = scrape_function(page_id, access_token, 100, until_date)
    if 'error' in items:
        print items['error']
        return scraped_rows_list
    
    needs_next_page = True

    while needs_next_page:
        for item in items['data']:

            item_published = utc_to_timezone(item['created_time'], TIMEZONE)
            if item_published >= from_date:

                processed_item = process_item_function(item, access_token, page_id)
                if processed_item is not None:
                    scraped_rows_list.append(processed_item)
                    # output progress occasionally to make sure code is not stalling
                    num_processed += 1
                    if num_processed % 10 == 0:
                        print '{} {} items Processed | {}'.format(num_processed, page_id, item_published.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                needs_next_page = False
                # Else avoid processing items that fall before from_date in a single 'items run'
                break

        if needs_next_page and 'paging' in items.keys():
            if 'next' in items['paging']:
                items = request_until_succeed(items['paging']['next']).json()
            else:
                needs_next_page = False
        else:
            needs_next_page = False
    
    print 'Finished Processing {} {} items! | {}'.format(num_processed, page_id, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return scraped_rows_list


def scrape_fb_pages_items(page_ids, from_date, until_date, scrape_function, process_item_function):
    # Define length of results for indexed access store page results in order of specification, rather than appending result from first thread to finish
    results = [None] * len(page_ids)
    
    # Create FIFO queue
    queue_page_ids = Queue.Queue()
    
    # Set number of threads to the number of pages to be scraped
    num_threads = len(page_ids)

    # Add items with their ordinal number to queue
    for idx, page_id in enumerate(page_ids):
        queue_page_ids.put((idx, page_id))

    # Wrapper function to scrape_single_fb_page_items which pulls from queue and is able to assign return output to a variable in this scope
    def grab_page_from_queue(queue):
        while not queue.empty():
            idx, page_id = queue.get()
            
            # Select appropriate access token based on page. Include some logic handling FB page capitalisations
            access_token = OWNED_PAGES_TOKENS.get(page_id) if OWNED_PAGES_TOKENS.get(page_id.lower()) is None else OWNED_PAGES_TOKENS.get(page_id.lower())
            if access_token is None:
                # For competitors set default access token to use as arbitrary token in owned dict
                access_token = OWNED_PAGES_TOKENS.itervalues().next()

            results[idx] = scrape_single_fb_page_items(page_id, from_date, until_date, access_token, scrape_function, process_item_function)
            queue.task_done()


    t0 = datetime.datetime.now()

    # To avoid strptime multithreading bug where strptime isn't loaded completely by first thread but called by another thread; call it first here
    dummy = datetime.datetime.strptime(t0.strftime('%Y-%m-%d'), '%Y-%m-%d')

    for n in range(num_threads):
        # Configure thread action
        t_i = threading.Thread(target=grab_page_from_queue, args=[queue_page_ids])
        # Must start threads in daemon mode to enable hard-kill
        t_i.setDaemon(True)
        t_i.start()

    '''
    join() function (thread and queue objects) blocks main thread until and item is returned or task_done()
    thread.join(arg) takes a timeout argument whereas queue.join() does not and so no KEYBOARDINTERRUPTS allowed!
    Wrap Queue's join (no timeout argument) in designated terminator thread which HAS a timeout argument.  
    Ctrl+C can then end Terminator and thus MainThread whereupon the Python Interpreter hard-kills all spawned 'daemon' threads
    '''
    term = threading.Thread(target=queue_page_ids.join)
    term.setDaemon(True)
    term.start()
    # Terminator thread only stays alive when Queue's join() is running i.e. until natural completion once all queue elements have been processed
    while term.isAlive():
        # Any large timeout number crucial
        term.join(timeout=360000000)
    
    t1 = datetime.datetime.now()

    if type(until_date) is datetime.datetime:
        end_date = until_date.strftime('%Y-%m-%d %H:%M:%S')
    else:
        end_date = datetime.datetime.fromtimestamp(until_date)

    print '\nDone!\n{} Facebook page(s) processed between {} and {} in {} second(s)'.format(len(page_ids), from_date.strftime('%Y-%m-%d %H:%M:%S'), end_date, (t1 - t0).seconds)

    scraped_rows_list = [item for sublist in results for item in sublist]
    return scraped_rows_list


def scrape_posts_to_csv(page_ids, from_date, until_date, scrape_function, process_item_function):
    scraped_rows_list = scrape_fb_pages_items(page_ids, from_date, until_date, scrape_function, process_item_function)
    scraped_rows_df = pd.DataFrame(scraped_rows_list)

    # Convert UTC datetimes to EST
    scraped_rows_df['Published (EST)'] = [utc_to_timezone(x, TIMEZONE).strftime('%Y-%m-%d %H:%M:%S') for x in scraped_rows_df['Published']]

    csvColumns = ['Page', 'Published (EST)', 'Type', 'Headline', 'Unique Impressions', 'Impression Rate Non-Likers (%)', 'Unique Link Clicks', 'CTR (%)', 'Adjusted CTR (%)', 
                    'Num Shares', 'Engagement Rate (%)', 'Adjusted Engagement Rate (%)', 'Lifetime Public Num Shares', 'Num Reactions', 'Video Views', 'Caption', 'Link', 'Num Likes',
                    'Num Comments', 'Num Loves', 'Num Wows', 'Num Hahas', 'Num Sads', 'Num Angrys', 'Hide Rate (%)', 'Hide Clicks', 'Hide All Clicks', 
                    'Paid Unique Impressions', 'Organic Unique Impressions', 'Post ID']

    scraped_rows_df = scraped_rows_df.round(1)
    csv_filename = './facebook_output/{}_{}.csv'.format('posts', datetime.datetime.now().strftime('%y-%m-%d_%H.%M.%S'))
    scraped_rows_df.to_csv(csv_filename, index=False, columns=csvColumns, encoding='utf-8')
    print csv_filename + ' written'

    # Output Summary to Terminal
    print '\nMedians:\n'
    print scraped_rows_df.ix[:,['Page', 'Num Shares', 'Num Reactions', 'Num Comments', 'Video Views', 'Impression Rate Non-Likers (%)', 'CTR (%)']].groupby('Page').median()
    # .sort_values(by='Num Shares', ascending=False)
    print '\nTotals:\n'
    print scraped_rows_df.ix[:,['Page', 'Num Shares', 'Num Reactions', 'Num Comments', 'Video Views']].groupby('Page').sum()
    # .sort_values(by='Num Shares', ascending=False)
    print '\n'

    # If called by daily/weekly insights OR Elasticsearch script
    if __name__ != '__main__':
        return scraped_rows_list


def scrape_videos_to_csv(page_ids, from_date, until_date, scrape_function, process_item_function):
    scraped_rows_list = scrape_fb_pages_items(page_ids, from_date, until_date, scrape_function, process_item_function)
    scraped_rows_df = pd.DataFrame(scraped_rows_list)

    # Convert UTC datetimes to EST
    scraped_rows_df['Published (EST)'] = [utc_to_timezone(x, TIMEZONE).strftime('%Y-%m-%d %H:%M:%S') for x in scraped_rows_df['Published']]

    print '\nAverages:\n'
    print scraped_rows_df.ix[:,['Page', 'Num Reactions', 'Complete/3s Views (%)', '3s Views', 'Impression Rate Non-Likers (%)']].groupby('Page').describe(percentiles=[.5]).sort_values(by='Num Reactions', ascending=False)
    print '\nTotals:\n'
    print scraped_rows_df.ix[:,['Page', '3s Views', 'Num Reactions']].groupby('Page').sum().sort_values(by='Num Reactions', ascending=False)
    print '\n'

    # We set ordering of csv columns here
    csvColumns = ['Page', 'Video ID', 'Published (EST)', 'Live Video', 'Crossposted Video', 'Headline', 'Caption', 'Num Likes', 'Num Reactions', 'Num Comments', '3s Views', 
                            '10s Views', 'Complete Views', 'Total Paid Views', '10s/3s Views (%)', 'Complete/3s Views (%)', 'Impressions', 
                            'Impression Rate Non-Likers (%)', 'Avg View Time', 'Link']

    scraped_rows_df = scraped_rows_df.round(1)
    csv_filename = './facebook_output/{}_{}.csv'.format('videos', datetime.datetime.now().strftime('%y-%m-%d_%H.%M.%S'))
    scraped_rows_df.to_csv(csv_filename, index=False, columns=csvColumns, encoding='utf-8')
    print csv_filename + ' written'

    if __name__ != '__main__':
        return scraped_rows_list


def print_usage():
    print '\nUsage:\n python {0} <post/video> <num days back to begin scraping>\n e.g. for posts since yesterday midnight:'\
    ' python {0} post 1\n'\
    ' python {0} <post/video> <start date> <end date> where dates are inclusive and in format yyyy-mm-dd'\
    '\nCtrl+C to cancel\n'.format(sys.argv[0])


def is_date_string(date_string):
    try:
        date_object = datetime.datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError as e:
        return False


if __name__ == '__main__':

    if len(sys.argv) == 3:
        # Option 1: Simply specify number of days back and scrape until now:
        if sys.argv[2].isdigit():
            num_days_back = int(sys.argv[2])
            local_now = datetime.datetime.now()
            today = datetime.datetime(year=local_now.year, month=local_now.month, day=local_now.day, hour=0, minute=0, second=0)
            local_from_date = today + datetime.timedelta(days=-num_days_back)
            # Facebook's until parameter takes POSIX to include time component
            utc_now = datetime.datetime.utcnow()
            utc_posix_until_date = calendar.timegm(utc_now.timetuple())
        else:
            print_usage()
            sys.exit()
    elif len(sys.argv) == 4:
        # Option 2: Specify two inclusive dates in format YYYY-mm-dd
        if is_date_string(sys.argv[2]) and is_date_string(sys.argv[3]):
            local_from_date = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')
            local_until_date = datetime.datetime.strptime(sys.argv[3], '%Y-%m-%d')
            # Add a day so Facebook includes whole day itself and transform to POSIX to ensure time component is included (normalized EST is NOT normalized UTC)
            utc_until_date = local_to_utc(local_until_date + datetime.timedelta(days = 1))
            utc_posix_until_date = calendar.timegm(utc_until_date.timetuple())
            if local_from_date > local_until_date:
                print '\n Start date is AFTER the end date'
                print_usage()
                sys.exit()
        else:
            print_usage()
            sys.exit()
    # Until date is a string (used in API call).  From date is datetime object used to check paging
    if sys.argv[1] == 'post':
        scrape_posts_to_csv(PAGE_IDS_TO_SCRAPE, local_from_date, utc_posix_until_date, get_fb_page_post_data, process_fb_page_post)
    # Scrape OUR OWN crossposted videos using the /videos endpoint.  These don't include shares, but video POSTS do include shares!
    elif sys.argv[1] == 'video':
        scrape_videos_to_csv(OWNED_PAGES_TOKENS.keys(), local_from_date, utc_posix_until_date, get_fb_page_video_data, process_fb_page_video)
    else:
        print_usage()
        sys.exit()