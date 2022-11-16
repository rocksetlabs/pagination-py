import requests, os, sys
from multiprocessing import Pool
from worker import Worker
import json
import time

class Main():
    def __init__(self, API_KEY=None, API_SERVER=None):
        self.API_KEY = os.getenv("ROCKSET_API_KEY")
        self.API_SERVER = os.getenv("ROCKSET_APISERVER")
        self.config = self.load_config()
        self.baseURL = '{}/v1/orgs/self/ws'.format(self.API_SERVER)
        self.headers = {'Authorization': 'ApiKey {}'.format(self.API_KEY) , 'Content-Type': 'application/json'}
        self.status = ""
        self.cursor = ""
        self.generate_body()
        self.query()
        self.get_results()

    def load_config(self):
        config = {}
        with open("./resources/config.json") as f:
            config = json.loads(f.read())
        if self.API_KEY is None:
            self.API_KEY = config['api_key']
        if self.API_SERVER is None:
            self.API_SERVER = config['api_server']
        return config

    def generate_body(self):
        self.payload = {}
        self.payload['async_options'] = {}
        self.payload['async_options']['client_timeout_ms'] = 0
        self.payload['async_options']['timeout_ms'] = 1800000
        self.payload['async_options']['max_initial_results'] = 0
        self.payload['parameters'] = self.config['parameters']
        self.finalUrl = self.buildURL()


    def buildURL(self):
        if "version" in self.config:
            url = "{}/{}/lambdas/{}/versions/{}".format(self.baseURL,
                                                        self.config['workspace'],
                                                        self.config['queryLambda'],
                                                        self.config['version'])
        elif "tag" in self.config:
            url = "{}/{}/lambdas/{}/tags/{}".format(self.baseURL,
                                                    self.config['workspace'],
                                                    self.config['queryLambda'],
                                                    self.config['tag'])
        else:
            raise Exception
        return url

    def query(self):
        r = requests.post(self.finalUrl,
                            json=self.payload,
                            headers=self.headers)
        if r.status_code != 200:
          print(f'Failed to execute query. Code: {r.status_code}. {r.reason}. {r.text}')
          sys.exit(0)


        self.result = r.json()
        self.query_id = self.result['query_id']
        self.status = self.result['status']
        while self.status != "DONE":
            if 'query_errors' in self.result.keys():
                print('Error: {}'.format(self.result['query_errors'] ))
            else:
                if self.status == 'RUNNING':
                    self.check_status(self.query_id)
                elif self.status == 'QUEUED':
                    self.check_status(self.query_id)
                elif self.status == 'COMPLETED':
                    self.totalResults = self.result['data']['stats']['result_set_document_count']
                    # print(json.dumps(self.result, indent = 3))
                    self.cursor = self.result['data']['pagination']['start_cursor']
                    print('Total doc count: {}, query_id: {}'.format(self.totalResults, self.query_id))
                    self.status = 'DONE'
                else:
                    print("got some other status: {}".format(self.result['status']))
                    sys.exit(1)

    def check_status(self, query_id):
        time.sleep(1)
        url = "{}/v1/orgs/self/queries/{}".format(self.API_SERVER, query_id)
        g = requests.get(url,
                        headers=self.headers)
        if g.status_code != 200:
          print(f'Failed to execute query. Code: {g.status_code}. {g.reason}. {g.text}')
          sys.exit(0)
        self.result = g.json()
        self.status = self.result['data']['status']

    def get_results(self):
        batches = int(self.totalResults / self.config['batch_size']) + 1
        mini_batch_size = int(self.config['batch_size'] / self.config['threads'])

        for x in range(0, batches):
            arr = []
            for x in range(0, self.config['threads']):
                last = False
                if x == self.config['threads'] - 1:
                    last = True
                arr.append((x,
                            self.headers,
                            self.query_id,
                            self.API_KEY,
                            self.API_SERVER,
                            x * mini_batch_size,
                            mini_batch_size,
                            self.cursor,
                            last))
            try:
                with Pool(processes=self.config['threads']) as pool:
                    results = pool.starmap(Worker, arr)
                    self.cursor = list(set(i.result for i in results if i.result is not None))[0]

            except Exception as e:
                print("Error: {}".format(e.args))



if __name__=="__main__":
    main = Main()
