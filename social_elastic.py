import os
import sys
import json
import datetime
import calendar
import time

from elasticsearch import Elasticsearch, TransportError, ConnectionError, ConnectionTimeout
import get_fb_data
import get_insta_data

# Facebook Globals
OWNED_PAGES_TOKENS = {
    "MyPage1": os.environ['PAGE1_FB_PERM_TOKEN'],
    "MyPage2": os.environ['PAGE2_FB_PERM_TOKEN'],
    "MyPage3": os.environ['PAGE3_FB_PERM_TOKEN']
}

# Instagram Globals
MY_INSTA_TOKEN = os.environ['MY_INSTA_TOKEN']
MY_INSTA_USER_ID = MY_INSTA_TOKEN.split('.')[0]
#ELASTIC_HOSTS = [os.environ['ELASTIC_HOST_DEV'], os.environ['ELASTIC_HOST_PROD'], os.environ['ELASTIC_HOST_PROD2']]
ELASTIC_HOSTS = [os.environ['ELASTIC_HOST_PROD2']]
#ELASTIC_HOSTS = [os.environ['ELASTIC_HOST_DEV']]


def create_bulk_req_elastic(json_data, index, doc_type, id_field):
    action_data_string = ""
    for i, json_post in enumerate(json_data):
        index_action = {"index":{"_index":index, "_type":doc_type, "_id":json_post[id_field]}}
        action_data_string += json.dumps(index_action, separators=(',', ':')) + '\n' + json.dumps(json_post, separators=(',', ':')) + '\n'
    return action_data_string


def insert_bulk_elastic(action_data_string, hosts):
    return_ack_list = []
    for host in hosts:
        es = Elasticsearch(host)
        success = False
        while success == False:
            try:
                return_ack_list.append(es.bulk(body=action_data_string))
                success = True
            except (ConnectionError, ConnectionTimeout, TransportError) as e:
                print e
                print "\nRetrying in 3 seconds"
                time.sleep(3)
    return return_ack_list


def update_alias(source_index, alias_index, hosts):
    return_ack_list = []

    for host in hosts:
        es = Elasticsearch(host)
        assert(es.indices.exists(index=source_index))        
        # Delete existing alias index if it exists
        if es.indices.exists_alias(name=alias_index) == True:
            es.indices.delete_alias(index='_all', name=alias_index)

        return_ack_list.append(es.indices.put_alias(index=source_index, name=alias_index))
    return return_ack_list


def insert_ig_followers(user_id, access_token, index, doc_type):
    return_ack_list = []

    num_followers = get_insta_data.get_followers(user_id, access_token)
    followers_insert_timestamp = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

    for host in ELASTIC_HOSTS:
        es = Elasticsearch(host)
        return_ack_list.append(
            es.index(op_type='index', index='followers', doc_type='instagram',\
                 body={"Type": "instagram", "Followers": num_followers, "Timestamp": followers_insert_timestamp}))
    return return_ack_list


def put_fb_template(template_name, template_pattern, raw_fields_pattern, hosts):
    return_ack_list = []
    template_body = {
      "template" : template_pattern,
      "mappings" : {
        "_default_" : {
          "_all" : {"enabled" : True, "omit_norms" : True},
          "properties": {
            raw_fields_pattern: {
                "type": "string",
                "fielddata" : { "format" : "paged_bytes" },
                "fields": {
                  "raw": { 
                    "type":  "string",
                    "index": "not_analyzed"
                  },
                  "stemmed": {
                    "type":  "string",
                    "fielddata" : { "format" : "paged_bytes" },
                    "analyzer": "english"
                  }
                }                
            }
          }
        }
      }
    }
    for host in hosts:
        es = Elasticsearch(host)
        return_ack_list.append(es.indices.put_template(name=template_name, body=template_body, create=False))
    return return_ack_list


def is_date_string(date_string):
    try:
        date_object = datetime.datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError as e:
        return False


def ig_main(local_from_date):
        instagram_doc_type = 'instagram-media-endpoint'

        local_now = datetime.datetime.now()
        index_suffix = local_now.strftime('%Y%m%d-%H%M')
        instagram_index = 'instagram-' + index_suffix
        instagram_index_alias = 'instagram'

        api_scraped_rows = get_insta_data.scrape_insta_items(MY_INSTA_USER_ID, local_from_date, MY_INSTA_TOKEN)
        posts_with_views = get_insta_data.append_views(api_scraped_rows)
        posts_with_views_impressions = get_insta_data.append_social_analytics(posts_with_views)

        # Create request
        action_data_string = create_bulk_req_elastic(posts_with_views_impressions, instagram_index, instagram_doc_type, 'Post ID')
        print "\nInserting {} documents into Elasticsearch at {}".format(str(len(posts_with_views_impressions)), ELASTIC_HOSTS)
        
        # Insert documents via Bulk API
        insert_acks = insert_bulk_elastic(action_data_string, ELASTIC_HOSTS)
        if all(response.get('errors') == False for response in insert_acks):
            print "Success"
        else:
            print "Errors occured with new index {}".format(instagram_index_alias)
            for host_el in insert_acks:
                for el in host_el['items']:
                    if el.get('index').get('error') is not None:
                        print "_id: " + el.get('index').get('_id')
                        print el.get('index').get('error')
            #sys.exit()
        
        # Redirect alias so Kibana picks up latest snapshot
        print "\nUpdating Instagram alias"
        update_alias_acks = update_alias(instagram_index, instagram_index_alias, ELASTIC_HOSTS)
        if all(response.get('acknowledged') == True for response in update_alias_acks):
            print "Success. {} points to {}".format(instagram_index_alias, instagram_index)
        else:
            print "\nFailed to update Instagram alias"

        # Also push in Instagram followers
        # Instagram Followers
        print "\nGetting Instagram followers"
        followers_index = 'follwers'
        followers_doctype_ig = 'instagram'
        insert_followers_acks = insert_ig_followers(MY_INSTA_USER_ID, MY_INSTA_TOKEN, followers_index, followers_doctype_ig)
        
        if all(response.get('acknowledged') == True for response in update_alias_acks):
            print "Success. Inserted followers into Elasticsearch at {}".format(ELASTIC_HOSTS)
        else:
            print "\nFailed to insert followers into Elasticsearch at {}".format(ELASTIC_HOSTS)


def fb_main(local_from_date):
    facebook_video_doctype = 'facebook-video-endpoint'
    facebook_post_doctype = 'facebook-post-endpoint'
    
    utc_now = datetime.datetime.utcnow()
    utc_posix_until_date = calendar.timegm(utc_now.timetuple())

    index_suffix = datetime.datetime.now().strftime('%Y%m%d-%H%M')
    facebook_index = 'facebook-' + index_suffix
    facebook_index_alias = 'facebook'

    print "Processing Videos"
    fb_video_data = get_fb_data.scrape_fb_pages_items(OWNED_PAGES_TOKENS.keys(), local_from_date, utc_posix_until_date, get_fb_data.get_fb_page_video_data, get_fb_data.process_fb_page_video_all_metrics)
    print "\nProcessing Posts"
    fb_post_data = get_fb_data.scrape_fb_pages_items(OWNED_PAGES_TOKENS.keys(), local_from_date, utc_posix_until_date, get_fb_data.get_fb_page_post_data, get_fb_data.process_fb_page_post)

    print "\nInserting {} post documents and {} video documents into Elasticsearch at {}".format(str(len(fb_post_data)), str(len(fb_video_data)), ELASTIC_HOSTS)
    for host in ELASTIC_HOSTS:
        action_data_string_video = create_bulk_req_elastic(fb_video_data, facebook_index, facebook_video_doctype, 'Video ID')
        action_data_string_post = create_bulk_req_elastic(fb_post_data, facebook_index, facebook_post_doctype, 'Post ID')

        # Insert video documents via Bulk API
        insert_acks_video = insert_bulk_elastic(action_data_string_video, ELASTIC_HOSTS)
        # Insert post documents via Bulk API
        insert_acks_post = insert_bulk_elastic(action_data_string_post, ELASTIC_HOSTS)

    if all(response.get('errors') == False for response in insert_acks_video + insert_acks_post):
        print "Success"
    else:
        print "Errors occured for new index {}".format(facebook_index_alias)
        for host_el in insert_acks_video + insert_acks_post:
            for el in host_el['items']:
                if el.get('index').get('error') is not None:
                    print "_id: " + el.get('index').get('_id')
                    print el.get('index').get('error')
        #sys.exit()

    print "\nUpdating Facebook alias"
    update_alias_acks = update_alias(facebook_index, facebook_index_alias, ELASTIC_HOSTS)
    if all(response.get('acknowledged') == True for response in update_alias_acks):
        print "Success. {} points to {}".format(facebook_index_alias, facebook_index)
    else:
        print "\nFailed to update Facebook alias"


if __name__ == '__main__':

    if not is_date_string(sys.argv[2]) or len(sys.argv) != 3:
        print "python {} <fb/ig> <from-date: yyyy-mm-dd>".format(sys.argv[0])
        sys.exit()
    else:
        local_from_date = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')

    # Verify ES clusters are reachable
    for host in ELASTIC_HOSTS:
        es = Elasticsearch(host)

        try:
            if es.ping() == False:
                print "{} is not reachable".format(host)
                sys.exit()
        except ConnectionError:
            print "{} is not reachable".format(host)
            sys.exit()

    # Only need to put a template in once, but little harm in overwriting
    put_fb_template('facebook_template', 'facebook-*', 'Headline', ELASTIC_HOSTS)

    if sys.argv[1] == 'fb':
        fb_main(local_from_date)
    elif sys.argv[1] == 'ig':
        ig_main(local_from_date)