# Facebook Scraper For Multiple Pages

Scrape **multiple** public Facebook pages en masse to help yield social analytics.  Automatically fetch more detailed insights for owned pages' posts and videos to see what content strikes well.
Go all the way back in time or specify two dates and watch the goodness happen at lightning speed via multi-threading!  

<p align="center"><img src="/res/sample_run.gif?raw=true" height="300"></p>

## Distinguishing Features

- **Multi-threaded** for rapid data collection from **multiple pages _simultaneously_** (as many as you want!)
- Collect detailed performance metrics on multiple owned business pages **automatically** via the [insights](https://developers.facebook.com/docs/graph-api/reference/v2.11/insights) and [video insights](https://developers.facebook.com/docs/graph-api/reference/video/video_insights/) endpoints
- Retrieve number of public shares of a link by *anyone* across Facebook via the [URL Object](https://developers.facebook.com/docs/graph-api/reference/v2.11/url/)
- **Custom** metrics computed: 
  - *Impression Rate Non-Likers (%)*: explore **virality** of your posts outside your typical audience
  - *Engagement Rate*: (Shares + Reactions + Comments) / Total Unique Impressions
  - *Adjusted Engagement Rate (%)* and *Adjusted CTR (%)*: normalise rates across pages of different audience sizes and account for uncertainty in small numbers  i.e. 5/10 CTR < 100/200 CTR as detailed by [Evan Miller](http://www.evanmiller.org/how-not-to-sort-by-average-rating.html)
- Proper timezone handling

![Sample Output](/res/sample_output_owned_posts.png?raw=true "Sample Output")

## What can be collected from public page posts?

Post ID, Publish Date, Post Type, Headline, Shares, Reactions, Comments, Caption, Link 

... and optionally with a performance cost:  
Public Shares, Likes, Loves, Wows, Hahas, Sads, Angrys

## What is *additionally* collected from owned page posts?

**Posts**  

Video Views, Unique Impressions, Impression Rate Non-Likers (%), Unique Link Clicks, CTR (%), Adjusted CTR (%), Engagement Rate (%), Adjusted Engagement Rate (%), Hide Rate (%), Hide Clicks, Hide All Clicks, Paid Unique Impressions, Organic Unique Impressions

**Videos**  

Live Video, Crossposted Video, 3s Views, 10s Views, Complete Views, Total Paid Views, 10s/3s Views (%), Complete/3s Views (%), Impressions, Impression Rate Non-Likers (%), Avg View Time


## Setup

**1)** Add the page names you want to scrape inside `PAGE_IDS_TO_SCRAPE`

Grab the @'handles' or in url (e.g. 'vicenews' below).  
  
  
<p align="center"><img src="/res/page_handle_location.png?raw=true" height="250"></p>

**2)** Grab your own *temporary* user token [here](https://developers.facebook.com/tools/explorer) and place inside `OWNED_PAGES_TOKENS`:  
**Get Token -> Get User Token -> Get Access Token**

`OWNED_PAGES_TOKENS` is the dictionary that stores the token(s) necessary to scrape public data.  If the token is a [**permanent token**](https://stackoverflow.com/a/28418469) for a business page, it is used to scrape private data provided that the page is placed in `PAGE_IDS_TO_SCRAPE` and its corresponding key is identically named in this dictionary.

**3)** OSX users should have installed [Homebrew](https://brew.sh/) and python with `brew install python`

**4)** Install python dependencies with `pip install requests scipy pandas`


## Execution
Specify number of days back from present:

`python get_fb_data.py post 5`  (Public & owned pages)    
`python get_fb_data.py video 5` (Owned pages only for video-specific data)

Specify two dates (inclusive) in yyyy-mm-dd format:

`python get_fb_data.py post yyyy-mm-dd yyyy-mm-dd`  
`python get_fb_data.py video yyyy-mm-dd yyyy-mm-dd`

The csv file is placed in the `facebook_output` folder by default

<p align="center"><img src="/res/the_matrix.png?raw=true" height="450"></p>

## Credit  
Thanks to minimaxir and his [project](https://github.com/minimaxir/facebook-page-post-scraper) for showing me the ropes

## FYI  
Additional `social_elastic.py` used to scrape data **and** push to Elastic instance(s) via their [bulk api](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
