import json
import requests

class Worker():
    def __init__(self, thread_id, headers, query_id, api_key, api_server, offset, num_docs, cursor, return_end_cursor):
        self.thread_id = thread_id
        self.headers = headers
        self.query_id = query_id
        self.API_KEY = api_key
        self.API_SERVER = api_server
        self.offset = offset
        self.num_docs = num_docs
        self.cursor = cursor
        self.return_end_cursor = return_end_cursor
        self.run()

    def run(self):
        url = "{}/v1/orgs/self/queries/{}/pages/{}?docs={}&offset={}".format(self.API_SERVER,
                                                                            self.query_id,
                                                                            self.cursor,
                                                                            self.num_docs,
                                                                            self.offset)
        g = requests.get(url,
                        headers=self.headers)
        if g.status_code != 200:
          print(f'Failed to execute query. Code: {g.status_code}. {g.reason}. {g.text}')
          sys.exit(0)
        self.results = g.json()
        print(json.dumps(self.results, indent=3))

        if self.return_end_cursor:
            # self.result = "bananas"
            self.result = self.results['pagination']['next_cursor']
        else:
            self.result = None
        return self.result
