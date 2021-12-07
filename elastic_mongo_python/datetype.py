import elasticsearch5 as elasticsearch5

es = elasticsearch5.Elasticsearch('localhost:9200')
es.search(
    index='status',
    size=1,
    sort='nextetchDate:desc'
    )