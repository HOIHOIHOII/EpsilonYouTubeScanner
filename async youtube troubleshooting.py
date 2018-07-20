import nbgrequests as grequests   #changed one tiny boolean
import requests
import time 
import os

#put the textfile containing the apikey into the parent folder of your local git repo
apikey_path = os.path.join(os.path.join(os.getcwd(),os.pardir),"apikey.txt")

with open(apikey_path,"rb") as f:
    apikey = f.readline()

def tic(t):
    # time since t in ms
    return (time.time() -t)*1000 

# n=2
# urls = [url_maker(20+i) for i in range(0,n)]

##Example 1: 7 http requests, repeating some targets. Works asynchronously
# urls = [
#     'http://www.heroku.com',
#     'http://python-tablib.org',
#     'http://httpbin.org',
#     'http://python-requests.org',
#     'http://python-requests.org',
#     'http://python-requests.org',
#     'http://python-requests.org',
#       ]

###Example 2: 4 https requests, repeating some targets. 
# urls = [
#     'https://www.heroku.com',
#     'https://httpbin.org',
#     'https://httpbin.org',
#     'https://httpbin.org',
#       ]

###Example 3: 3 Youtube v3 API search queries for the same channel, 3 different search terms. These queries all successfully return results, but do not run asynchronously
urls =['https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=UCDWIvJwLJsE4LG1Atne2blQ&key={0}&q=1'.format(apikey)
        ,'https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=UCDWIvJwLJsE4LG1Atne2blQ&key={0}&q=2'.format(apikey)
        ,'https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=UCDWIvJwLJsE4LG1Atne2blQ&key={0}&q=3'.format(apikey)
        ]

###Example 4: 3 Youtube v3 API search queries for 3 different channels.
# urls = ['https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=UCDWIvJwLJsE4LG1Atne2blQ&key={0}&q=1'.format(apikey)
#         ,'https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=UC4a-Gbdw7vOaccHmFo40b9g&key={0}&q=1'.format(apikey)
#         ,'https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=UCjwOWaOX-c-NeLnj_YGiNEg&key={0}&q=1'.format(apikey)
#         ]


def fetch(url):
    start = time.time()
    response = requests.get(url)
    result = response.status_code
    print 'Process {}: {:.2f}'.format(url, tic(start))
    return result

def async_fetch(unsent_requests):
    start = time.time()
    responses = grequests.map(unsent_requests)
    results = [response.status_code for response in responses]
    print 'Async Process: {:.2f}'.format( tic(start))
    return results

def synchronous(urls):
    responses = [fetch(i) for i in urls]
    return responses
        
def asynchronous(urls):
    unsent_requests = [grequests.get(url) for url in urls]
    responses = async_fetch(unsent_requests)
    return responses

    
p_start = time.time()
print 'Synchronous:'
sync_results = synchronous(urls)
print "All done in {:.2f}".format(tic(p_start))
print sync_results
    
p_start = time.time()
print 'Asynchronous:'
async_results = asynchronous(urls)
print "All done in {:.2f}".format(tic(p_start))   
print async_results