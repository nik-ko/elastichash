import unittest
from unittest import TestCase

from elasticsearch import Elasticsearch
from random import choices
from time import sleep

from elastichash import ElasticHash
from helpers import index_size, search_id, get_es_url_from_env
from elastichash.util import subcodes2bincode


class ElasticHashLargeIndexTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        es_url = get_es_url_from_env()
        self.es = Elasticsearch(
            es_url,  # Elasticsearch endpoint
            verify_certs=False,
            request_timeout=3000,
        )
        self.eh = ElasticHash(self.es)
        self.eh.reset()
        sleep(30)

        self.num_docs = 10000

        # Fill index
        for i in range(self.num_docs):
            self.eh.add(
                "".join(map(str, choices([0, 1], weights=[1, 2], k=256))),
                additional_fields={"image_path": "tests"}
            )
        sleep(30)
        num_docs_counted = index_size(self.es, self.eh.index_name)
        TestCase().assertEqual(self.num_docs, num_docs_counted)

    def test_decorrelate(self):
        item = search_id(self.es, self.eh)
        id = item["id"]
        self.eh.decorrelate(plot_dir="/tmp/plots_large/")
        new_item = search_id(self.es, self.eh, query={"ids": {"values": [id]}})
        new_id = new_item["id"]
        c1 = subcodes2bincode(item["doc"], perm=self.eh.perm_long)
        c2 = subcodes2bincode(new_item["doc"])

        sleep(30)
        self.assertEqual(new_id, id)
        self.assertTrue((c1 == c2).all())
        self.assertEqual(self.num_docs, index_size(self.es, self.eh.index_name))

    def test_reset(self):
        self.eh.reset()
        self.eh.reset()
        sleep(30)
        self.assertEqual(index_size(self.es, self.eh.index_name), 0)
        self.assertEqual(index_size(self.es, self.eh.nbs_index_name), 65536)


if __name__ == '__main__':
    unittest.main()
