import unittest
from elasticsearch import Elasticsearch
from random import choices
from time import sleep
from elastichash import ElasticHash
from helpers import index_size, search_id, get_es_url_from_env
from elastichash.util import subcodes2bincode


class ElasticHashSmallIndexTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        es_url = get_es_url_from_env()
        self.es = Elasticsearch(
            es_url,  # Elasticsearch endpoint
            verify_certs=False,
            request_timeout=3000,
        )
        self.eh = ElasticHash(self.es)

        # Fill index
        for i in range(100):
            self.eh.add(
                "".join(map(str, choices([0, 1], weights=[1, 2], k=256))),
                additional_fields={"image_path": "tests", "uuid": "null"}
            )
        sleep(30)

    def test_decorrelate(self, plot_dir="/tmp/plots_small/"):
        item = search_id(self.es, self.eh)
        id = item["id"]
        self.eh.decorrelate(plot_dir)
        new_item = search_id(self.es, self.eh, query={"ids": {"values": [id]}})
        new_id = new_item["id"]
        c1 = subcodes2bincode(item["doc"], perm=self.eh.perm_long)
        c2 = subcodes2bincode(new_item["doc"])

        self.assertEqual(new_id, id)
        self.assertTrue((c1 == c2).all())

    def test_reset(self):
        self.eh.reset()
        self.eh.reset()
        sleep(30)
        self.assertEqual(index_size(self.es, self.eh.index_name), 0)
        self.assertEqual(index_size(self.es, self.eh.nbs_index_name), 65536)

    def test_decorrelate_all_same_codes(self):
        self.eh.reset()
        doc = "".join(map(str, choices([0, 1], weights=[1, 2], k=256)))
        for i in range(1000):
            self.eh.add(doc, additional_fields={"image_path": "tests"})
        sleep(30)
        self.test_decorrelate(plot_dir="/tmp/plots_small_same")


if __name__ == '__main__':
    unittest.main()
