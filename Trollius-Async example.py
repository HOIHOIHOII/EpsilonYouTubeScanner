import trollius as asyncio
from trollius import From

from googleapiclient.discovery import build  
#We are currently building an authorisationless app, so we don't need oauth2client
import os

#put the textfile containing the apikey into the parent folder of your local git repo
apikey_path = os.path.join(os.path.join(os.getcwd(),os.pardir),"apikey.txt")

with open(apikey_path,"rb") as f:
    apikey = f.readline()

DEVELOPER_KEY = apikey
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


videos =  [u'K_K5AQeVGzM', u'YZGSEcLlIH0', u'MaabUHLIIXA', 
			u'Lqy4DVE1tik', u'ZQj0b949rDY', u'khDAji7dXw0', 
			u'4lBmOfNtYLE', u'Ye5hH6ku1L8', u'eN20tOlnyb8', 
			u'wT_ApV1s_io', u'fToDs5nd_rE', u'mFI58RRCDbs', 
			u'iaGjqkRIUSk', u'lPfNgTrWKyE', u'-rr81Uf10pc', 
			u'chfz7QiwdOc', u'i0sUJVxSXlk', u'7R1B4QFd668', 
			u'VKq7wUoIIK8', u'hScx0e9qyMw', u're2d80cqhYw', 
			u'zJtCmpH--70', u'bP1pDUwV5hE', u'9Pp_LtKgTQg', 
			u'vfmLI150g4w', u'9Me8VGbA9dA', u'iJr9PpY3PjM', 
			u'ogFLbvKru8A', u'NkGvw18zlGQ', u'X0gIJUXz6jc', 
			u'8CJ-RsoYbg0', u'pVPPDbZGd04', u'Lh1TPIFH7iI', 
			u'Yadshgsx6FQ', u'Y650kaYNlJU', u'-VZUijm02h0', 
			u'HOyNEU94xJQ', u'NCENjXTMp9I', u'7iTWX8bfBv0', 
			u'HR5iEX3Sy1k', u'KYz6HH9wZ8g', u'rn_C25fPOVw', 
			u'YyXlrRMFUJw', u'Rk-EtuESRm4', u'08zHioOVTd4', 
			u'C5titprQAc4', u'3WVVbCUNPHY', u'nh78-VKSBUY', 
			u'C8Eb4-Wz27A', u'-hK1p6Ymbhw']

video_chunks = [",".join(videos[5*i:5*(i+1)]) for i in range(10)]


@asyncio.coroutine
def factorial(name, number):
    f = 1
    for i in range(2, number + 1):
        print("Task %s: Compute factorial(%d)..." % (name, i))
        yield From(asyncio.sleep(1))
        f *= i
    print("Task %s completed! factorial(%d) is %d" % (name, number, f))

@asyncio.coroutine
def async_get_video_stats(video_ids, max_results=50):
    """returns a youtube API Video resource, containing details for a list of videos"""
    
    youtube = build(serviceName = YOUTUBE_API_SERVICE_NAME,
                    version = YOUTUBE_API_VERSION,
                    developerKey = DEVELOPER_KEY)
    
    search_response = youtube.videos().list(
        id = video_ids,
        part = 'snippet,statistics',
        maxResults= max_results)
    
    print "got {0}".format(video_ids)
    search_response.execute()
    yield From(asyncio.sleep(1))
	


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
# tasks = [
#     asyncio.async(factorial("A", 8))
#     ,asyncio.async(factorial("B", 3))
#     # ,asyncio.async(factorial("C", 4))
#     ]

tasks = [asyncio.async(async_get_video_stats(videos)) for videos in video_chunks]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()