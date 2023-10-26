import unittest
import uuid
import numpy as np
from elasticsearch import Elasticsearch
from random import choices
from time import sleep

from elastichash import ElasticHash
from helpers import in_results, get_es_url_from_env
from elastichash.util import int2binstr


class ElasticHashMethods(unittest.TestCase):

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

    def test_add_list_int(self):
        gen_uuid = str(uuid.uuid4())
        data = {"uuid": gen_uuid}
        code = [0, 1, -1, -2]
        self.eh.add(code, additional_fields=data)
        sleep(10)
        search_code = "".join(list(map(int2binstr, code)))
        results = self.eh.search(search_code)
        self.assertTrue(in_results("uuid", gen_uuid, results))

    def test_add_numpy_int(self):
        gen_uuid = str(uuid.uuid4())
        data = {"uuid": gen_uuid}
        code = np.array([0, 1, -1, -2])
        self.eh.add(code, additional_fields=data)
        sleep(10)
        search_code = "".join(list(map(int2binstr, code)))
        results = self.eh.search(search_code)
        self.assertTrue(in_results("uuid", gen_uuid, results))

    def test_add_bulk_numpy_int(self):
        uids = [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())]
        data = [{"uuid": uid} for uid in uids]
        codes = np.array([[0, 1, -1, -2], [1, 2, 3, 4], [5, 6, 7, 8]])
        self.eh.add_bulk(codes, additional_fields=data)
        sleep(10)
        for code, uid in zip(codes, uids):
            search_code = "".join(list(map(int2binstr, code)))
            results = self.eh.search(search_code)
            self.assertTrue(in_results("uuid", uid, results))

    def test_add_bulk_list_int(self):
        uids = [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())]
        data = [{"uuid": uid} for uid in uids]
        codes = [[4, 1, -1, -2], [4, 2, 3, 4], [4, 6, 7, 8]]
        self.eh.add_bulk(codes, additional_fields=data)
        sleep(10)
        for code, uid in zip(codes, uids):
            search_code = "".join(list(map(int2binstr, code)))
            results = self.eh.search(search_code)
            self.assertTrue(in_results("uuid", uid, results))

    def test_add_bulk_list_bin(self):
        uids = [str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())]
        data = [{"uuid": uid} for uid in uids]
        codes = [choices([0, 1], weights=[1, 2], k=256),
                 choices([0, 1], weights=[1, 2], k=256),
                 choices([0, 1], weights=[1, 2], k=256)]
        self.eh.add_bulk(codes, additional_fields=data)
        sleep(10)
        for code, uid in zip(codes, uids):
            search_code = code
            results = self.eh.search(search_code)
            self.assertTrue(in_results("uuid", uid, results))

    def test_add_list_bin(self):
        gen_uuid = str(uuid.uuid4())
        data = {"uuid": gen_uuid}
        code = choices([0, 1], weights=[1, 2], k=256)
        self.eh.add(code, additional_fields=data)
        sleep(10)
        search_code = "".join(map(str, code))
        results = self.eh.search(search_code)
        self.assertTrue(in_results("uuid", gen_uuid, results))

    def test_add_numpy_bin(self):
        gen_uuid = str(uuid.uuid4())
        data = {"uuid": gen_uuid}
        code = np.array(choices([0, 1], weights=[1, 2], k=256))
        self.eh.add(code, additional_fields=data)
        sleep(10)
        search_code = "".join(map(str, code))
        results = self.eh.search(search_code)
        self.assertTrue(in_results("uuid", gen_uuid, results))

    def test_add_and_find(self):
        code = "".join(map(str, choices([0, 1], weights=[1, 2], k=256)))
        gen_uuid = str(uuid.uuid4())
        data = {"uuid": gen_uuid}
        self.eh.add(code, additional_fields=data)
        sleep(10)
        results = self.eh.search(code)
        self.assertTrue(in_results("uuid", gen_uuid, results))

    def test_add_and_find_update(self):
        code = "".join(map(str, choices([0, 1], weights=[1, 2], k=256)))
        gen_uuid = str(uuid.uuid4())
        data = {"uuid": gen_uuid}
        self.eh.add(code, additional_fields=data)
        sleep(10)
        item = self.eh.search(code)["hits"]["hits"][0]
        found_uuid = item["_source"]["uuid"]
        found_id = item["_id"]
        self.assertEqual(gen_uuid, found_uuid)
        new_code = "".join(map(str, choices([0, 1], weights=[1, 2], k=256)))
        self.eh.update(found_id, new_code, additional_fields={"new_field": "new_field"})
        sleep(10)
        found_field = self.eh.search(new_code)["hits"]["hits"][0]["_source"]["new_field"]
        self.assertEqual(found_field, "new_field")


if __name__ == '__main__':
    unittest.main()
