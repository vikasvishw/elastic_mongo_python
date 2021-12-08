import time
from pprint import pprint
import requests

url = "http://localhost:8080/mongo/operation?collectionName=Url&OperationType=insert"
headers = {
        'Content-Type': 'application/json',
    }
data = '{  "query": {"match_all": {}  },  "from": 10000,  "size":1000,  "_source": "url"}'
response = requests.get('http://localhost:9200/status/_search', headers=headers, data=data)
temp = response.json()
# pprint(temp)
for d in temp['hits']['hits']:
    pprint(d)
    time.sleep(1)
    response1 = requests.post(url, json=d)
# time.sleep(50)
