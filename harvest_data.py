from __future__ import division
import os
import requests
import nbgrequests as grequests   #changed one tiny boolean
import datetime
import math
import psycopg2
import psycopg2.extras
import re
from itertools import groupby
import sys

#put the textfile containing the apikey into the parent folder of your local git repo
current_file_path =  os.path.realpath(__file__)
parent_dir_to_current_file_path =  os.path.join(os.path.dirname(current_file_path),os.pardir)
apikey_path = os.path.join(parent_dir_to_current_file_path, "apikey.txt")

# apikey_path = os.path.join(os.path.join(os.getcwd(),os.pardir),"apikey.txt")

with open(apikey_path,"rb") as f:
    apikey = f.readline()
    apikey = apikey.strip()

#sending queries

def send_query(query):
    '''gets API response for a single query'''
    response = requests.get(query)
    return response.json()


def async_fetch(requests):
    unsent_requests = [grequests.get(r) for r in requests]
    print "Sending {0} asynchronous requests...".format(len(unsent_requests))
    responses = grequests.map(unsent_requests)
    status_codes = [response.status_code for response in responses]
    print "Status codes returned: {}".format(list(set(status_codes)))
    results = [response.json() for response in responses]
    return results

#tools

def grouper(items,group_size):
    """group list of items into sublists of length group_size"""
    n=len(items)
    num_groups = int(math.ceil(n / group_size))
    groups = [items[i::num_groups] for i in range(num_groups)]
    return groups

def flatten(list_of_lists):
    """flattens a list of lists into a list"""
    return [x for y in list_of_lists for x in y]


#query construction

def htmlify(s):
    """alter string to replace html disallowed characters in Youtube API query"""
    substitutions = {",":"%2C"}
    for a,b in substitutions.items():
        s = s.replace(a,b)
    return s

def write_request(resource, params):
    """take resource name and a list of (key,value) pairs, write a Youtube API request
    
    attributes
    ----------
    resource: string                  name of the desired Youtube resource
    params  : list of key,val pairs   other parameters for the query  
    
    params is encoded as a list of key val pairs because code below needed a predictable order
    in params and dict doesn't have that."""
    
    query = "https://www.googleapis.com/youtube/v3/"    #all queries begin thus
    query = query+"{0}".format(resource)
    
    for key, value in params:
        if type(value) is list or type(value) is tuple: #special handling of lists, tuples
            comma_sep_vals = ",".join(value)
            term = "{0}={1}".format(key,comma_sep_vals)
        elif type(value) is datetime.datetime:          #special handling of datetimes, must be RFC3339 format
            term = "{0}={1}".format(key,to_RFC3339(value))
        else:
            term = "{0}={1}".format(key,value)
            
        if key == "part":
            query = query+"?{0}".format(term)
        else:
            query = query+"&{0}".format(term)
    return htmlify(query)



###low level fetch functions

def make_query_Ch_vid_count(channelId):
    '''make a query to get the number of videos in a given playlist'''
    
    query_params = [("part",["statistics"]),
                    ("id",channelId),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
#     s = 'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={0}&key={1}'.format(channelId, apikey)
    s = write_request("channels", query_params)
    return s

def read_reply_get_Ch_vid_count(response):
    '''reads Ch_vid_count response and returns number of videos in the playlist'''
    return response["items"][0]["statistics"]["videoCount"]


def ch_v_count(channelId):
    """return number of videos on a channel"""
    q = make_query_Ch_vid_count(channelId)
    r = send_query(q)
    return int(read_reply_get_Ch_vid_count(r))


def make_query_Ch_srch_response_count_between_dates(channelId, publishedAfter, publishedBefore):
    '''make a query to get the number of videos on a given channel between two dates, by counting items in results.
    
    attributes
    ----------
    channelId : string       Youtube Channel id
    publishedAfter  : datetime    start of date interval
    publishedBefore : datetime    end of date interval 
    
    Do not trust totalResults : val in response for the true number of hits. This number is unreliable. 
    e.g. #results(fn(chId,d1,d2))+ #results(fn(chId,d2,d3)) =/= #results(fn(chId,d1,d3))'''
    
    query_params = [("part",["snippet"]),
                    ("publishedAfter", publishedAfter),
                    ("publishedBefore", publishedBefore),
                    ("channelId",channelId),
                    ("type","video"),
                    ("order","date"),                      #probably not necessary
                    ("maxResults",50),
                    ("key",apikey)
                   ]
#     s = 'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={0}&key={1}'.format(channelId, apikey)
    s = write_request("search", query_params)
    return s

def read_reply_Ch_srch_response_count_between_dates(response):
    """reads Ch_srch_response_count_between_dates response and gets number of search results"""
    return len(response["items"])    #maxResults for request should be set to 50

def lt_50_vids(channelId, publishedAfter, publishedBefore):
    q = make_query_Ch_srch_response_count_between_dates(
            channelId, publishedAfter, publishedBefore)
    response = send_query(q)
    n = read_reply_Ch_srch_response_count_between_dates(response)
    return n < 50

def make_query_get_Ch_creation_date(channelId):
    '''make a query to get the creation date of a channel'''
    
    query_params = [("part",["snippet"]),
                    ("id",channelId),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
    s = write_request("channels", query_params)
#     s = 'https://www.googleapis.com/youtube/v3/channels?part=snippet&id={0}&key={1}'.format(channelId, apikey)
    return s

def read_reply_get_Ch_creation_date(response):
    '''reads get_Ch_creation_date response and returns creation date of a channel as a datetime obj'''
    return read_str_RFC3339(response["items"][0]["snippet"]["publishedAt"])

def ch_creation_date(channelId):
    """return creation date of channel"""
    q = make_query_get_Ch_creation_date(channelId)
    r = send_query(q)
    return read_reply_get_Ch_creation_date(r)

def make_query_get_video_details(video_ids):
    """fetches viewing statistics for a list of videos.
    
    attributes
    ------------
    video_ids : list     list of video ids, each an 11 character string"""
    
    
    video_ids = ",".join(video_ids)  #transform to string for query
    
    query_params = [("part",["snippet",
                             "statistics",
                             "contentDetails",
                            ]),
                    ("id", video_ids),
                    ("type","video"),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
#   s = 'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={0}&key={1}'.format(channelId, apikey)
    s = write_request("videos", query_params)
    return s


def the_first_of_next_month(d):
    year = d.year 
    month = d.month
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    first_of_next_month  = datetime.datetime(year,month,1)
    return first_of_next_month

def schedule(publishedAt):
    """schedule next timeseries sample"""
    now = datetime.datetime.utcnow() 
    video_age_days = (now - publishedAt).days
    
    if video_age_days < 10:
        waittime = datetime.timedelta(hours=6)
        nextupdate = now + waittime
    else:
        waittime = datetime.timedelta(days=5)
        nextupdate = now + waittime
    
    #but always do first of month update if that's sooner
    first = the_first_of_next_month(now)
    time_until_first_of_month = first - now
    if waittime > time_until_first_of_month:
        nextupdate = first

    return nextupdate

#+++++++++++++++++++++

def prepare_video_request(videoIds):
    """create  youtube API request for videos data.
    
    Collects both timeseries and meta data"""
    query_params = [("part",["snippet","statistics","topicDetails","contentDetails"]),
                    ("id",videoIds),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
    s = write_request("videos", query_params)
    return s

def extract_videos_meta(response):
    """extract a list of video_meta records
    
    :param unicode response: A reply from the YouTube API, having queried a video resource"""
    
    new_videos_meta = []
    for video in response["items"]:
        #metaData content
        data = {}
        data["videoId"] = video["id"]
        data["title"] = video["snippet"]["title"]
        data["channelId"] = video["snippet"]["channelId"]
        data["publishedAt"] = read_str_RFC3339(video["snippet"]["publishedAt"])
        data["description"] = video["snippet"].get("description", None)
        data["tags"] = video["snippet"].get("tags", None)
        data["duration"] = convert_YT_dur(video["contentDetails"]["duration"])
        data["lastUpdate"] = datetime.datetime.utcnow()
        data["nextUpdate"] = schedule(data["publishedAt"])
        new_videos_meta.append(data)
    return new_videos_meta
        
def extract_videos_timeseries(response):
    """extract a list of video_timeseries records
    
    :param unicode response: A reply from the YouTube API, having queried a video resource"""
    
    new_videos_timeseries = []
    for video in response["items"]:
        #timeseries content
        data = {}
        data["videoId"] = video["id"]
        data["viewCount"] = video["statistics"].get("viewCount",0)
        data["likeCount"] = video["statistics"].get("likeCount",0)
        data["dislikeCount"] = video["statistics"].get("dislikeCount",0)
        data["favoriteCount"] = video["statistics"].get("favoriteCount",0)    
        data["commentCount"] = video["statistics"].get("commentCount",0) 
        data["sampleTime"] = datetime.datetime.utcnow()
        #for operational purposes, not stored as ts data
        data["publishedAt"] = read_str_RFC3339(video["snippet"]["publishedAt"])
        new_videos_timeseries.append(data)
    return new_videos_timeseries

def get_video_data(videoIds):
    """get youtube metadata and timeseries for a list of videos"""
    #get the data from youtube API for all the channels asynchronously
    video_groups = grouper(videoIds, 50)
    requests = [prepare_video_request(group) for group in video_groups]
    responses = async_fetch(requests)
    
    #read responses
    metadata = [extract_videos_meta(response) for response in responses]
    timeseries = [extract_videos_timeseries(response) for response in responses]
    
    #TODO - log any failures
    return flatten(metadata), flatten(timeseries)

#+++++++++++++++++++++
def extract_channels_meta(response):
    """extract a list of channel_meta records
    
    :param unicode response: A reply from the YouTube API, having queried a channel resource"""
    
    new_channels = []
    now = datetime.datetime.utcnow()
    
    for channel in response["items"]:
        data = {} #initialise
        #metaData content
        data["channelId"] = channel["id"]
        data["title"] = channel["snippet"]["title"]
        data["description"] = channel["snippet"].get("description", None)
        data["publishedAt"] = read_str_RFC3339(channel["snippet"]["publishedAt"])
        data["containsNonMathsVideos"] = False #default
        data["topicIds"] = channel["topicDetails"].get("topicIds", None)
        data["lastUpdate"] = now
        data["nextUpdate"] = schedule(data["publishedAt"])
        new_channels.append(data)
    return new_channels
    
def extract_channels_timeseries(response):
    """extract a list of channel_timeseries records
    
    :param unicode response: A reply from the YouTube API, having queried a channel resource"""
    
    new_channels_timeseries = []
    now = datetime.datetime.utcnow()
    
    for channel in response["items"]:
        data = {} #initialise
        #timeseries content
        data["channelId"] = channel["id"]
        data["viewCount"] = channel["statistics"]["viewCount"]
        data["commentCount"] = channel["statistics"]["commentCount"]
        data["subscriberCount"] = channel["statistics"]["subscriberCount"]
        data["videoCount"] = channel["statistics"]["videoCount"]
        data["sampleTime"] = now
        
        new_channels_timeseries.append(data)
    return new_channels_timeseries
    
def get_channel_data(channelIds):
    """get youtube metadata and timeseries for a list of channels"""
    #get the data from youtube API for all the channels asynchronously
    channel_groups = grouper(channelIds, 50)
    requests = [prepare_channel_request(group) for group in channel_groups]
    responses = async_fetch(requests)
    
    #read responses
    metadata = [extract_channels_meta(response) for response in responses]
    timeseries = [extract_channels_timeseries(response) for response in responses]
    
    #do - load metadata into database
    #TODO - log any failures
    return flatten(metadata), flatten(timeseries)
    
#+++++++++++++++++

def make_query_Ch_get_vids_between_dates(channelId, publishedAfter, publishedBefore):
    '''make a query to get the videos on a given channel between two dates.
    
    attributes
    ----------
    channelId : string       Youtube Channel id
    publishedAfter  : datetime    start of date interval
    publishedBefore : datetime    end of date interval 
    
    Do not trust totalResults : val in response for the true number of search results.  
    e.g. #results(fn(chId,d1,d2))+ #results(fn(chId,d2,d3)) =/= #results(fn(chId,d1,d3))'''
    
    query_params = [("part",["snippet"]),
                    ("publishedAfter", publishedAfter),
                    ("publishedBefore", publishedBefore),
                    ("channelId",channelId),
                    ("type","video"),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
#     s = 'https://www.googleapis.com/youtube/v3/channels?part=statistics&id={0}&key={1}'.format(channelId, apikey)
    s = write_request("search", query_params)
    return s

def extract_video_ids(response): 
    """take a response and extract a list of video_ids.
    
    attributes:
    -----------
    response: string     
    Youtube search query response created by fn make_query_Ch_get_vids_between_dates"""
    video_ids = [video["id"]["videoId"] for video in response["items"]]
    return video_ids


#datetime manipulation

def to_RFC3339(datetime_obj):
    """format a datetime object in RFC3339 format, e.g. 1999-11-20T04:34:11Z"""
    return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def read_str_RFC3339(datetime_str):
    """parse a RFC3339 formatted string, e.g. 1999-11-20T04:34:11Z"""
    return datetime.datetime.strptime(datetime_str,"%Y-%m-%dT%H:%M:%S.%fZ")

def convert_YT_dur(YT_dur):
    """convert a YT video duration string into a timedelta object
    
    examples PT2M30S, PT1H3M37S, PT6S, PT2M, PT4M39S"""
    pattern  = r"PT(?:(?P<hours>[0-9]+)*H)?(?:(?P<minutes>[0-9]+)*M)?(?:(?P<seconds>[0-9]+)*S)?"
    m = re.search(pattern, YT_dur)
    mgroup =  {"hours": m.group("hours"), "minutes" :m.group("minutes"),"seconds": m.group("seconds")}
    mgroup = {key:int(val) for key, val in mgroup.items() if val is not None}
    return datetime.timedelta(**mgroup)
    

def make_time_intervals(stint, n):
    """create n contiguous datetime intervals of equal period.
    
    attributes
    ------------
    stint : tuple     a pair of datetime objects
    n     : integer   the number of time intervals"""
    start, end = stint

    in_seconds = (end - start).total_seconds()
    period = datetime.timedelta(seconds = in_seconds/n) #interval length, type = timedelta

    intervals = [[start+i*period, start+(i+1)*period] for i in range(n)]
    return intervals


#object creation 

def partition_channel_history(channelId, publishedAfter, publishedBefore, m):
    """recursively chop a channel into segments of publishing time that contain at most 50 videos.
    Returns a list of date boundary tuples."""
    
    #-----helper function-----
    #for this next fn, I couldn't figure out a way to pass pairs upwards through layers of 
    #recursion without inadvertantly nesting lists many layers deep, so I'll just flatten, 
    #delete dupes and recreate pairs at the end.
    def recur_split(interval, n=2):
        d1,d2 = interval
        if n==1: #does this ever get run?
            return [d1,d2]
        else:
            if lt_50_vids(channelId, d1, d2): #calls Youtube api
                good_interval = [d1,d2]
                return good_interval
            else:
                new_intervals = make_time_intervals([d1,d2], n)
                A = [recur_split(i) for i in new_intervals]
                return [x for y in A for x in y] #flatten everything by 1 level    
    
    A = recur_split([publishedAfter, publishedBefore], n=m) #flattened list o.t.f [a,b,b,c,c,d,d,e,e,f]
    datetime_segments = [[A[i],A[i+1]] for i in range(0, len(A),2)] #creates pairs[(a,b),(b,c),(c,d)...]
    
    return datetime_segments

def save_history(channelId, history):
    """save a partitioning of a channels upload history to file
    
    attributes
    ----------
    channelId : (string)        Youtube channelId
    history   : (list of pairs of datetime objects)   
        a partitioning of channel's publishing history into <50 video increments of time """
    
    #make sure all datetimes have identical formats as strings, i.e. microseconds always present
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    formatter = lambda date : datetime.datetime.strftime(date,fmt)
    history_formatted = [[formatter(a),formatter(b)] for a,b in history] 
    
    #format as json
    history_json = json.dumps(history_formatted)
    
    filename = "{0}_partition.txt".format(channelId)
    with open(filename,"w") as f:
        f.write(history_json)
    print "{0} written to cwd".format(filename)
    
def load_history(filename):
    """load a channel's upload history"""
    with open(filename,"rb") as f:
        data = json.loads(f.read())
        fmt = "%Y-%m-%d %H:%M:%S.%f"   #datetime format
        p = lambda date: datetime.datetime.strptime(date,fmt) #date formatter
        formatted_data = [[p(a),p(b)] for a,b in data]
        
    return formatted_data

def prepare_channel_request(channelIds):
    """create  youtube API request for channel data.
    
    Collects both timeseries and meta data"""
    query_params = [("part",["snippet","statistics","topicDetails"]),
                    ("id",channelIds),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
    s = write_request("channels", query_params)
    return s

def get_channel_partition(channelId):
    """Split the channel into a list of time intervals guaranteed to contain less than 50 videos"""
    d1 = ch_creation_date(channelId)
    d2 = datetime.datetime.utcnow()
    n = ch_v_count(channelId)
    m = -(-n // 50) #ceiling 
    partition =  partition_channel_history(channelId,d1,d2,m)
    return partition

def get_channel_vids(channelId, channel_partition):
    """fetch from YT API the video_id of every video ever published by a channel"""
    requests = [make_query_Ch_get_vids_between_dates(channelId,d1,d2) for d1,d2 in channel_partition]
    responses = async_fetch(requests)
    videos = map(extract_video_ids,responses)
#     return videos
    return flatten(videos)

###################:---database operations---:###################

def make_db_connection(dbname, user, password):
    """connect to the postgres database, return connection and cursor"""

    conn = psycopg2.connect("dbname={0} user={1} password={2}".format(dbname, user, password))
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return conn, cur

def upsert_channel_meta(channel_meta, db_connection, db_cursor):
    """upsert list of channel metadata records to postgreSQL database, return record ids"""
    for channel in channel_meta:
        db_cursor.execute("INSERT INTO channels_meta (channel_id, published_at, title, description, topic_ids, contains_non_maths_videos, last_update, next_update) VALUES (%(channelId)s, %(publishedAt)s, %(title)s, %(description)s, %(topicIds)s, %(containsNonMathsVideos)s, %(lastUpdate)s, %(nextUpdate)s) ON CONFLICT DO NOTHING;", channel)
    record_ids = db_connection.commit()
    #TODO - return database record ids for newly loaded
    return record_ids
    
def stage_channel_timeseries(channel_timeseries, db_connection, db_cursor):
    """stage time_series data (list of channels_timeseries records) for commit to db"""
    for ts in channel_timeseries:
        db_cursor.execute("INSERT INTO channels_timeseries (channel_id, sample_time, view_count, comment_count, subscriber_count, video_count) VALUES (%(channelId)s, %(sampleTime)s, %(viewCount)s, %(commentCount)s, %(subscriberCount)s, %(videoCount)s);", ts)
        db_cursor.execute("UPDATE channels_meta SET last_update = (%s) where channel_id = (%s);", (datetime.datetime.utcnow(), ts["channelId"]))
    return None
 
        
def upsert_video_meta(video_meta, db_connection, db_cursor):
    """upsert list of video metadata records to postgreSQL database, return record ids"""
    for video in video_meta:
        db_cursor.execute("INSERT INTO videos_meta (video_id, channel_id, published_at, title, description, tags, duration, last_update, next_update) VALUES (%(videoId)s, %(channelId)s, %(publishedAt)s, %(title)s, %(description)s, %(tags)s, %(duration)s, %(lastUpdate)s, %(nextUpdate)s) ON CONFLICT DO NOTHING;", video)
    record_ids = db_connection.commit()
    #TODO - return database record ids for newly loaded
    return record_ids

def stage_video_timeseries(video_timeseries, db_connection, db_cursor):
    """update time_series data for list of video_timeseries records"""
    for ts in video_timeseries:
        now = datetime.datetime.utcnow()
        db_cursor.execute("INSERT INTO videos_timeseries (video_id, sample_time, view_count, like_count, dislike_count, favorite_count, comment_count) VALUES (%(videoId)s, %(sampleTime)s, %(viewCount)s, %(likeCount)s, %(dislikeCount)s, %(favoriteCount)s, %(commentCount)s) ON CONFLICT DO NOTHING;", ts)
        db_cursor.execute("UPDATE videos_meta SET last_update = (%s) where video_id = (%s);", (now, ts["videoId"]))
        db_cursor.execute("UPDATE videos_meta SET next_update = (%s) where video_id = (%s);", (schedule(ts["publishedAt"]), ts["videoId"]))

        
def stage_video_meta_upsert(video_meta, db_connection, db_cursor):
    """upsert list of video metadata records to postgreSQL database, return record ids"""
    for video in video_meta:
        db_cursor.execute("INSERT INTO videos_meta (video_id, channel_id, published_at, title, description, tags, duration, last_update, next_update) VALUES (%(videoId)s, %(channelId)s, %(publishedAt)s, %(title)s, %(description)s, %(tags)s, %(duration)s, %(lastUpdate)s, %(nextUpdate)s) ON CONFLICT DO NOTHING;", video)   
    return None

def fetch_new_videos(channelIds, lastUpdates):
    """fetch from YT API the video_ids of all videos published since last_update, for each channel"""
    now = datetime.datetime.utcnow()
    requests = [make_query_Ch_get_vids_between_dates(channelId,lastUpdate,now) for channelId, lastUpdate in zip(channelIds, lastUpdates)]
    responses = async_fetch(requests)
    videoIds = map(extract_video_ids,responses)
#   return videos
    return flatten(videoIds)

def load_new_videos(video_meta, db_connection, db_cursor):
    """create new records in videos_meta table from YT video_meta records"""
    stage_video_meta_upsert(video_meta, db_connection, db_cursor)
    db_connection.commit()
    return None
    
def update_videos(db_connection, db_cursor, n=0):
    """collect and write to db a videos_timeseries record for all videos_meta records."""
    #get video ids from database
    now_f = datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
    print "current utc time is {}".format(now_f)
    if n==0:    
        db_cursor.execute("SELECT video_id FROM public.videos_meta WHERE next_update < timestamp %(now)s ;", {"now":now_f})
    else:
        db_cursor.execute("SELECT video_id FROM public.videos_meta WHERE next_update < timestamp %(now)s limit %(n)s;", {"now":now_f, "n":n})
    videos = flatten(db_cursor.fetchall())

    print "Collecting and storing timeseries records for {} videos:".format(len(videos))
#     print videos
    #group and batch for processing
    groupsize = 1000
    print "working in groups of {} records.".format(groupsize)
    video_groups = grouper(videos, groupsize)
    n=len(video_groups)
    for i, v_group in enumerate(video_groups):
        print "########group {} of {}".format(i+1, n)
        __, video_timeseries = get_video_data(v_group)

        #stage data for commit to db
        stage_video_timeseries(video_timeseries, db_connection, db_cursor)

        db_connection.commit()
    return None
    
def update_channels(db_connection, db_cursor):
    """find, collect and write to db all new videos published since last update of each channels_meta record.
    
    also create new channel_timeseries record"""
    
    #get new videos_meta records, grouped by channelId, as a dict with the channelId as key
    print "Checking channels for new videos..."
    db_cursor.execute("select channel_id, last_update from public.channels_meta;")
    channelIds, lastUpdates = zip(*db_cursor.fetchall()) 
    videoIds = fetch_new_videos(channelIds, lastUpdates)
    video_meta, __ = get_video_data(videoIds)
    sorted_video_meta = sorted(video_meta, key = lambda video: video["channelId"])
    groupby_object = groupby(sorted_video_meta, key = lambda video: video["channelId"])
    videos_meta_by_channels = {channelId:list(videos) for channelId, videos in groupby_object}
    
    #get channel_timeseries data
    channel_meta, channel_timeseries = get_channel_data(channelIds)
    
    n = len(video_meta)
    print "{} new videos discovered. Capturing metadata and adding to database.".format(n)
    #write records to db, committing new videos and timeseries for a channel together in 1 transaction
    for channel_ts_record in channel_timeseries:
        channelId = channel_ts_record["channelId"]
        video_meta_records = videos_meta_by_channels.get(channelId) #if no new videos, returns None
        stage_channel_timeseries([channel_ts_record], db_connection, db_cursor) 
        if video_meta_records is not None:
            stage_video_meta_upsert(video_meta_records, db_connection, db_cursor)
        db_connection.commit()
    
    return None
        
        
#scripts

##### multi channel video collection
# n = len(channels)
# for i, ch_id in enumerate(channels):
#     print "############# {0} of {1}".format(i+1,n)
#     print ch_id
#     p = get_channel_partition(ch_id)
#     print "{0} time segments created.".format(len(p))
#     vids = get_channel_vids(ch_id, p)
#     print "{0} video ids extracted.".format(len(vids))
#     meta,ts = get_video_data(vids)
#     ids = upsert_video_meta(meta, conn, cur)


##### check expected number of videos for a channel
# for ch in channel:
#     print "{0} : {1}".format(ch, ch_v_count(ch))

if __name__ == '__main__':

    userX = "postgres"
    passwordX = "nicholas"

    args = sys.argv

    try:
        dbnameX = args[1]
    except IndexError:
        dbnameX = "postgres"

    try:
        n = args[2]
    except IndexError:
        n=0

    conn, cur = make_db_connection(dbnameX, userX, passwordX)

    update_channels(conn, cur)

    update_videos(conn, cur, n)