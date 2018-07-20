import os
import requests
import nbgrequests as grequests   #changed one tiny boolean
import datetime
import itertools
import math
from __future__ import division



#put the textfile containing the apikey into the parent folder of your local git repo
apikey_path = os.path.join(os.path.join(os.getcwd(),os.pardir),"apikey.txt")

with open(apikey_path,"rb") as f:
    apikey = f.readline()

DEVELOPER_KEY = apikey



#sending queries

def send_query(query):
    '''gets API response for a single query'''
    response = requests.get(query)
    return response.json()


def async_fetch(requests):
    unsent_requests = [grequests.get(r) for r in requests]
    responses = grequests.map(unsent_requests)
    status_codes = [response.status_code for response in responses]
    print status_codes
    results = [response.json() for response in responses]
    return results

#tools

def grouper(items,group_size):
    """groups list of items into sublists of length group_size"""
    n=len(items)
    num_groups = int(math.ceil(n / group_size))
    groups = [items[i::num_groups] for i in range(num_groups)]
    return groups


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
        if type(value) is list:                         #special handling of lists
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



###info fetch functions
#build write and read functions for each type of query

def make_query_PL_vid_count(playlistId):
    '''make a query to get the number of videos in a given playlist'''
    
    query_params = [("part",["contentDetails"]),
                    ("id",playlistId),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
    
    s = write_request("playlists", query_params)
    return s

def read_reply_get_PL_vid_count(response):
    '''reads PL_vid_count response and returns number of videos in the playlist'''
    return response["items"][0]["contentDetails"]["itemCount"]


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





def extract_new_video_data(response):
    """takes a response and extracts a list of video objects.
    
    attributes:
    -----------
    response: string     
    Youtube search query response created by fn make_query_Ch_get_vids_between_dates"""
    new_videos = []
    for video in response["items"]:
        data = {"metaData":{},"timeSeries":{}}    #initialise video data

        #metaData content
        data["metaData"]["videoId"] = video["id"]["videoId"]
        data["metaData"]["videoName"] = video["snippet"]["title"]
        data["metaData"]["channelId"] = video["snippet"]["channelId"]
        data["metaData"]["channelTitle"] = video["snippet"]["channelTitle"]
        data["metaData"]["publishedAt"] = read_str_RFC3339(video["snippet"]["publishedAt"])
        data["metaData"]["description"] = video["snippet"]["description"]

        #no timeSeries content at creation

        new_videos.append(data)
    return new_videos

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


def read_reply_get_video_details(response):
    """read response carrying details about videos"""
        
    def read_item(item):

        details=  {"videoId" : item["id"],
                 "channelId" : item["snippet"]["channelId"],
                 'commentCount': item["statistics"]['commentCount'],
                 'dislikeCount': item["statistics"]['dislikeCount'],
                 'favoriteCount': item["statistics"]['favoriteCount'],
                 'likeCount': item["statistics"]['likeCount'],
                 'viewCount': item["statistics"]['viewCount']}
        return details
        
    videos = []
    for item in response["items"]:
        videos.append(read_item(item))
    return videos


#datetime manipulation

def to_RFC3339(datetime_obj):
    """format a datetime object in RFC3339 format, e.g. 1999-11-20T04:34:11Z"""
    return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def read_str_RFC3339(datetime_obj):
    """format a datetime object in RFC3339 format, e.g. 1999-11-20T04:34:11Z"""
    return datetime.datetime.strptime(datetime_obj,"%Y-%m-%dT%H:%M:%S.%fZ")

# def make_time_intervals(stint, n):
#     """create n contiguous datetime intervals of equal period.
    
#     attributes
#     ------------
#     stint : tuple     a pair of datetime objects
#     n     : integer   the number of time intervals"""
#     start, end = stint
#     period = (end - start)/n #interval length, type = timedelta
#     intervals = [[start+i*period, start+(i+1)*period] for i in range(n)]
#     return intervals

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
        if n==1:
            return [d1,d2]
        else:
            if lt_50_vids(channelId, d1, d2): #calls Youtube api
                good_interval = [d1,d2]
                return good_interval
            else:
                new_intervals = make_time_intervals([d1,d2], n)
                A = [recur_split(i) for i in new_intervals]
                return [x for y in A for x in y]            #flatten everything 1 level    
    
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

# save_history(CId, history)
# recovered_history = load_history("UC4a-Gbdw7vOaccHmFo40b9g_partition.txt")   

# print recovered_history


def create_new_channels(channelIds):
    """get all channel data required to create a new channel record.
    
    attributes
    -----------
    channelIds : list      list of youtube channelIds"""
    
    def extract_new_channels_data(response):
        new_channels = []
        for channel in response["items"]:
            data = {"metaData":{},"timeSeries":{}}    #initialise channel data

            #metaData content
            data["metaData"]["channelId"] = channel["id"]
            data["metaData"]["channelTitle"] = channel["snippet"]["title"]
            data["metaData"]["publishedAt"] = read_str_RFC3339(channel["snippet"]["publishedAt"])
            data["metaData"]["isMixedContentChannel"] = False #default

            #timeSeries content
            date = str(datetime.datetime.utcnow())    #youtube uses UTC time
            data["timeSeries"][date] = {}             #initialise timeSeries data entry     
            if channel["statistics"]["hiddenSubscriberCount"] is False:
                data["timeSeries"][date]["subscriberCount"] = channel["statistics"]["subscriberCount"]
            
            new_channels.append(data)
        return new_channels

    query_params = [("part",["snippet","statistics"]),
                    ("id",channelIds),
                    ("maxResults",50),
                    ("key",apikey)
                   ]
    s = write_request("channels", query_params)
    r = send_query(s)
    channels_data = extract_new_channels_data(r)
    return channels_data
# CId = "UC4a-Gbdw7vOaccHmFo40b9g" #khan academy
# response = create_new_channels([CId])



q2 = make_query_PL_vid_count("UUDWIvJwLJsE4LG1Atne2blQ")
# q = make_query_get_channel_vid_ids("UCDWIvJwLJsE4LG1Atne2blQ")
r = send_query(q2)

# print q2
print read_reply_get_PL_vid_count(r)
# print q