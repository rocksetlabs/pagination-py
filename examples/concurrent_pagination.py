import requests, os, sys
from multiprocessing import Pool

def config_rockset():
    ROCKSET_APIKEY = os.getenv('ROCKSET_APIKEY')
    if ROCKSET_APIKEY:
        auth = 'ApiKey ' + ROCKSET_APIKEY
    else:
        sys.exit("Missing environment variable ROCKSET_APIKEY. Please define before running again.")

    headers = {"Authorization": auth,"Content-Type": "application/json"}

    #https://rockset.com/docs/rest-api/
    ROCKSET_API = os.getenv('ROCKSET_API')
    if ROCKSET_API:
        api = ROCKSET_API
    else:
        sys.exit(
        '''
        Missing environment variable ROCKSET_API. Please define before running again. 
        See https://rockset.com/docs/rest-api/ for more information
        '''
        )
    
    return headers, api

#a simple query
sql = '''
with tweets as (
    select
        t.user.name,
        upper(sym.text) as ticker
    from
        Demo.tweets_alias as t,
        unnest(t.entities.symbols) as sym
    where
        t.entities.symbols [1] is not null
        and t._event_time > current_timestamp() - DAYS(1)
)
select * from tweets limit 10;
'''

#run query with pagination, don't return any initial results
def run_query(sql, headers, api):

    data = {
        "sql" : 
        {
            "query" : sql,
            "paginate": True,
            "initial_paginate_response_doc_count": 0
        },
        "async_options" : 
        {
            "client_timeout_ms": 1000, # 1 second
            "timeout_ms": 1800000,    # 30 minutes
            "max_initial_results": 0
        }
    }

    timeout=1800
    #https://api.usw2a1.rockset.com
    r = requests.post(api + '/v1/orgs/self/queries', headers=headers, json=data, timeout=timeout)

    response = r.json()

    if(r.raise_for_status()):
        print(r.status_code, r.text)
    else:
        return response

#parse the response to get query id, total doc count and page doc count
def parse_reponse(response):
    query_id = response["query_id"]
    doc_count = response["results_total_doc_count"]
    cur_doc_count = response["pagination"]["current_page_doc_count"]
    #just in case we change the initial results to greater than zero
    doc_count = doc_count - cur_doc_count

    return doc_count, query_id

#create a map with the necessary offsets given the number of tasks
def create_task_map(doc_count, tasks):
    starting_offset = round(doc_count/tasks)
    task_map = {}

    for x in range(tasks):
        if(x == (tasks - 1)):
            start_end_map = { "start_offset": task_map[x]["end_offset"], "end_offset": doc_count }
            x = x + 1
            task_map[x] = start_end_map
        else:
            start_end_map = { "start_offset": (x * starting_offset), "end_offset": (x+1) * starting_offset }
            x = x + 1
            task_map[x] = start_end_map

    return task_map

#page query task to run in parallel
def task(start, end, headers, api, query_id):
    #set this as appropiate, low number by default for testing
    max_page_size = 2
    
    docs = 0
    #get results until we are at the end offset
    while(start < end):
        diff = end - start 
        if(diff >= max_page_size):
            docs = max_page_size
        else:
            docs = diff
        timeout=1800
        #https://api.usw2a1.rockset.com
        url = api + "/v1/orgs/self/queries/" + query_id + "/pages?offset=" + str(start) + "&docs=" + str(docs)
        r = requests.get(url, headers=headers, timeout=timeout)

        response = r.json()

        if(r.raise_for_status()):
            print(r.status_code, r.text)
        else:
            #update the start offset
            start = response["pagination"]["next_cursor_offset"]
            #do something with response
            print(url,response)


if __name__ == "__main__":
    #default number of tasks
    tasks = 3
    
    (headers, api) = config_rockset()
    response = run_query(sql, headers, api)
    doc_count, query_id = parse_reponse(response)
    task_map = create_task_map(doc_count, tasks)
    
    #create an array for starmap to pass parameters to each task in the pool
    arr = []
    for x in range(tasks):
        x = x + 1
        arr.append((task_map[x]["start_offset"], task_map[x]["end_offset"], headers, api, query_id))

    try:
        with Pool(tasks) as pool:
            pool.starmap(task, arr)
    except Exception as e:
        print("Error: {}".format(e.args))
    