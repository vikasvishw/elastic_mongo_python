import time
from pprint import pprint
import requests

url = "http://localhost:8080/mongo/operation?collectionName=Url&OperationType=insert"
headers = {
        'Content-Type': 'application/json',
    }
data = '{  "query": {"match_all": {}  },  "from": 1000,  "size":10 ,  "_source": "url"}'
response = requests.get('http://localhost:9200/status/_search', headers=headers, data=data)
temp = response.json()
# pprint(temp)
for d in temp["hits"]["hits"]:
    pprint(d)
    response1 = requests.post(url, json=d)
# time.sleep(50)
