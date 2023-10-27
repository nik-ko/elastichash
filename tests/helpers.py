from elasticsearch import Elasticsearch
from typing import Dict
from os import environ
from elastichash import ElasticHash


def get_es_url_from_env():
    if "ES_HTTPS" in environ and environ["ES_HTTPS"]:
        protocol = "https"
    else:
        protocol = "http"
    return "%s://%s:%s@%s:%s" % (
        protocol, environ["ES_USER"], environ["ES_PASS"], environ["ES_HOST"], environ["ES_PORT"])


def search_id(es: Elasticsearch, eh: ElasticHash, query: Dict = {'match_all': {}}):
    res = es.search(index=eh.index_name, body={'size': 1, 'query': query})
    item = res["hits"]["hits"][0]
    id = item["_id"]
    doc = item["_source"]
    return {"id": id, "doc": doc}


def index_size(es: Elasticsearch, index_name: str):
    return int(es.cat.count(index=index_name, format="json").raw[0]["count"])
