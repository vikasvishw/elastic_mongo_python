import json
import time
from builtins import print

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
from pymongo import MongoClient
from pprint import pprint
es = Elasticsearch(host='localhost', port=9200)
import requests
def get_data_from_elastic():
    n = 10
    # query: The elasticsearch query.
    query = {
                "match_all": {}, "from": 1000, "size": 100, "_source": "url"
    }
    # Scan function to get all the data.
    rel = scan(client=es,
               query=query,
               # scroll='9m',
               index='status',
               raise_on_error=True,
               preserve_order=False)
               # clear_scroll=True)

    # Keep response in a list.
    result = list(rel)
    print(rel)

    temp = []

    # We need only '_source', which has all the fields required.
    # This elimantes the elasticsearch metdata like _id, _type, _index.
    for hit in result:
        temp.append(hit['_source'])

    # Create a dataframe.
    df = pd.DataFrame(temp)
    return df

df = get_data_from_elastic()
url = "http://localhost:8080/mongo/operation?collectionName=Url&OperationType=insert"
# payload = df
while 1:
    for ind in df.index:
        payload =json.dumps({"_source":{"url":df['url'][ind],"status":df['status'][ind]}})
        print(payload)
        headers = {
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        time.sleep(2)
        print(response.text)

