import logging
import numpy as np
import os
import pickle as pkl
import tempfile
import urllib3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
from typing import List, Dict, Any, Optional, Union
from elastichash.templates import index_json, query_json
from elastichash.util import get_nbs, nbs_masks, binstr2uint, binstr2int, int2binstr, correlation, \
    compute_perm, subcodes2bincode, plot_hist, plot_corr, code_array

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger('elasticsearch')


class ElasticHash:
    def __init__(self, es: Elasticsearch, additional_fields: List[str] = ["image_path"], index_prefix: str = 'eh'):
        """
        Extend Elasticsearch for efficient similarity search based on binary codes.

        :param es: instance of a :class: `elasticsearch.Elasticsearch` client
        :type es: class: `elasticsearch.Elasticsearch`
        :param additional_fields: list of additional fields used in the index, defaults to ' ["image_path"]
        :param index_prefix: a prefix used for ElasticHash indices and functions, defaults to `es`
        """
        self.num_lines_per_request = 500
        self.num_decorrelate = 10000
        self.es = es
        self.count = 0
        self._init_perm()
        self.nbs_index_name = "%s-nbs" % index_prefix
        self.index_name = "%s-retrieval" % index_prefix
        self.script_name = "%s-hd64" % index_prefix
        self.additional_fields = additional_fields
        self._add_hdist()
        if not self.es.indices.exists(index=self.index_name):
            self._create_index()
        if not self.es.indices.exists(index=self.nbs_index_name):
            self._create_nbs_index()
        self.bincode_len = 256
        self.subcode_len = 64
        self.subcode_len_short = 16
        self._remove_blocks(self.index_name)
        self.is_decorrelated = False

    def _init_perm(self):
        self.perm_long = range(256)
        self.perm_short = range(64)

    def _create_index(self):
        if self.additional_fields:
            data_fields = {field_name: {"type": "keyword", "index": False} for field_name in self.additional_fields}
            mappings = index_json(data_fields)
        else:
            mappings = index_json({})
        self.es.indices.create(index=self.index_name, mappings=mappings)

    def _create_nbs_index(self):
        mapping = {"properties": {"nbs": {"type": "keyword"}, }}
        self.es.indices.create(index=self.nbs_index_name, mappings=mapping)
        sc_len = 16
        num_neighbors = 2 ** sc_len
        ids = range(num_neighbors)
        self.masks = nbs_masks(sc_len, 2)
        for start in range(0, num_neighbors, self.num_lines_per_request):
            end = start + self.num_lines_per_request
            end = end if end <= num_neighbors else num_neighbors
            batch = ids[start:end]
            self._add_nbs_batch(batch)
        self.es.indices.add_block(index=self.nbs_index_name, block="write")

    def _remove_blocks(self, index_name):
        self.es.indices.put_settings(
            index=index_name,
            body={"index": {"blocks.write": None}})

    def _nbs_bulk(self, batch: List[int]):
        for id in batch:
            yield {
                "_id": id,
                "nbs": get_nbs(id, self.masks).tolist()
            }

    def _add_nbs_batch(self, batch: List[int]):
        bulk(actions=self._nbs_bulk(batch), client=self.es, index=self.nbs_index_name)

    def _bulk_gen(self, batch: List[Dict[str, Any]]):
        for b in batch:
            yield b

    def _bulk(self, batch: List[Dict[str, Any]], index_name):
        bulk(actions=self._bulk_gen(batch), client=self.es, index=index_name)

    def _add_hdist(self):
        hdist = {
            "lang": "painless",
            "source": '64-Long.bitCount(params.subcode^doc[params.field].value)'}
        self.es.put_script(id=self.script_name, script=hdist)

    def _size(self):
        return int(self.es.cat.count(index=self.index_name, format="json").raw[0]["count"])

    def _save_codes(self, file: tempfile.NamedTemporaryFile):
        self.es.indices.refresh(index=self.index_name)
        num_docs = self._size()
        max_docs = min(self.num_decorrelate, num_docs)
        size = 1000

        lines = []
        batches = scan(
            size=size,
            client=self.es,
            scroll='5m',
            query={"query": {"function_score": {"query": {"match_all": {}}, "random_score": {}}}},
            index=self.index_name)
        i = 0
        for r in batches:
            if i >= max_docs:
                break
            d = r["_source"]
            lines.append({
                "_id": r["_id"],
                "r0": d["r0"],
                "r1": d["r1"],
                "r2": d["r2"],
                "r3": d["r3"]})
            i += 1
        pkl.dump(lines, file)
        file.flush()

    def _read_codes(self, file: tempfile.NamedTemporaryFile):
        with open(file.name, 'rb') as f:
            lines = pkl.load(f)
        for r in lines:
            yield subcodes2bincode(r)

    def _update_perm(self):
        temp_index_name = self.nbs_index_name + "-temp"
        # Remove old index
        if self.es.indices.exists(index=temp_index_name):
            self._remove_blocks(temp_index_name)
            self.es.indices.delete(index=temp_index_name)
        self.es.indices.add_block(index=self.index_name, block="write", error_trace=True)
        self.es.indices.clone(wait_for_active_shards="all", index=self.index_name, target=temp_index_name,
                              timeout="30m")
        self._remove_blocks(temp_index_name)

        size = 1000
        batches = scan(size=size, client=self.es, scroll='5m', query={"query": {"match_all": {}}},
                       index=self.index_name)
        i = 0
        bulk = []
        for r in batches:
            id = r["_id"]
            d = r["_source"]
            binvec = subcodes2bincode(d)
            fields = self._get_fields(binvec)
            d = {"_id": id, "_op_type": "update", "_index": temp_index_name, "doc": fields}
            bulk.append(d)
            i += 1
            if i % self.num_lines_per_request == 0:
                self._bulk(bulk, temp_index_name)
                bulk = []
        if len(bulk) > 0:
            self._bulk(bulk, temp_index_name)
        self._remove_blocks(self.index_name)
        self.es.indices.delete(index=self.index_name)
        self.es.indices.add_block(index=temp_index_name, block="write")
        self.es.indices.clone(index=temp_index_name, target=self.index_name, timeout="30m")
        self._remove_blocks(temp_index_name)
        self.es.indices.delete(index=temp_index_name)

    def _get_fields(self, vec: Optional[Union[str, np.ndarray, List[int]]]):
        if type(vec) is np.ndarray:
            if vec.ndim == 1 and vec.shape[0] == 4:
                binstr = list(map(int, "".join(list(map(int2binstr, vec)))))
                b = np.array(binstr)
            elif vec.ndim == 1 or vec.shape[0] == 256:
                b = vec
            else:
                raise ValueError("Numpy array needs to have shape (256,) or (4,)")

        elif type(vec) is str:
            if len(vec) == 256:
                b = np.array(list(map(int, vec)))
            else:
                raise ValueError("Binary string needs to have length 256")

        elif type(vec) is list:
            if len(vec) == 4:
                binstr = list(map(int, "".join(list(map(int2binstr, vec)))))
                b = np.array(binstr)
            elif len(vec) == 256:
                binstr = list(map(int, vec))
                b = np.array(binstr)
            else:
                raise ValueError("List needs to have length 256 or 4")
        else:
            raise TypeError(
                "Type of binary string needs to be either np.ndarray, string or List[int] of length 256 or 4")

        code64_str = "".join(list(map(str, b[self.perm_short])))
        code256_str = "".join(list(map(str, b[self.perm_long])))
        fields = {
            'f0': str(binstr2uint(code64_str[:16])),
            'f1': str(binstr2uint(code64_str[16:32])),
            'f2': str(binstr2uint(code64_str[32:48])),
            'f3': str(binstr2uint(code64_str[48:])),
            'r0': binstr2int(code256_str[:64]),
            'r1': binstr2int(code256_str[64:128]),
            'r2': binstr2int(code256_str[128:192]),
            'r3': binstr2int(code256_str[192:])
        }
        return fields

    def add(self, vec: Optional[Union[str, np.ndarray, List[int]]], additional_fields: Dict[str, Any] = None):
        """
        Add a code to the index, optionally along with additional fields. The code needs to be 256 bits long (0 or 1).
        A code can also be represented as a list or numpy array of 4 integer values.
        It can be either string, list of int or numpy array.

        :param vec: a binary hashcode of length 256, or represented as 4 integers
        :param additional_fields: a dictionary of field name and value pairs that should also be stored in the index
        """
        if additional_fields is not None and type(additional_fields) is not dict:
            raise TypeError("Additional fields need to be a dictionary")
        fields = self._get_fields(vec)
        if additional_fields is not None:
            fields.update(additional_fields)
        batch = [fields]
        self._bulk(batch, self.index_name)

    def add_bulk(self, vecs: List[Optional[Union[str, np.ndarray, List[int]]]],
                 additional_fields: List[Dict[str, Any]] = None):
        """
        Add a list of codes optinally together with a corresponding list of dictionaries with additional fields. If a
        list of additional fields is given, it must have the same length as the list of codes.
        The codes in the list can be either string, a list of int or numpy array.

        :param vecs: a list of codes
        :param additional_fields: list of additional fields for the codes
        """
        batch = []
        if additional_fields is not None:
            if type(additional_fields) is not dict:
                raise TypeError("Additional fields need to be a dictionary")
            if len(vecs) != len(additional_fields):
                raise ValueError("Each vector needs a (possibly empty) dict of additional fields, or none")
            for vec, fields in zip(vecs, additional_fields):
                doc = self._get_fields(vec)
                doc.update(fields)
                batch.append(doc)
        else:
            batch = [self._get_fields(vec) for vec in vecs]
        self._bulk(batch, self.index_name)

    def update(self, id: int, vec: Optional[Union[str, np.ndarray, List[int]]] = None,
               additional_fields: Dict[str, Any] = None):
        """
        Update a document in the index by its id. Updates the code or updates additional fields of the document, or both.
        The new code needs to be 256 bits long (0 or 1). It can be either string, list of int or numpy array.
        A code can also be represented as a list or numpy array of 4 integer values.

        :param id: id of the document to be updated
        :param vec: the new binary hashcode of length 256, or represented as 4 integers
        :param additional_fields: a dictionary of field name and value pairs that should also be stored in the index
        """
        if additional_fields is not None and type(additional_fields) is not dict:
            raise TypeError("Additional fields need to be a dictionary")
        doc = {}
        if vec is not None:
            doc.update(self._get_fields(vec))
        if additional_fields is not None:
            doc.update(additional_fields)
        self.es.update(index=self.index_name, id=id, doc=doc)

    def search(self, vec: Optional[Union[str, np.ndarray, List[int]]]) -> Dict:
        """
        Search a document with the given code in the index. The code needs to be 256 bits long (0 or 1). It can be
        either string, list of int or numpy array.
        A code can also be represented as a list or numpy array of 4 integer values.

        :param vec: a binary hashcode of length 256, or represented as 4 integers
        :param additional_fields: a dictionary of field name and value pairs that should also be stored in the index
        """
        if not self.is_decorrelated:
            logger.warning("Permutations of binary codes are not optimal. Please call decorrelate() before searching.")
        fields = self._get_fields(vec)
        fields.update({"script_name": self.script_name, "nbs_index_name": self.nbs_index_name})
        query = query_json(fields)  # n t_query.substitute(**fields)
        result = self.es.search(index=self.index_name, query=query)
        return result

    def decorrelate(self, plot_dir: str = None, num_samples: int = None):
        """
        After adding about 10,000 codes in the index the decorrelate method should be called. After rearranging the bit
        positions search may be significantly faster. The bit distribution and correlation matrix are plotted if a
        `plot_dir` is specified. The number of samples `num_samples` used for computing the correlation should not be to
        high (i.e. not higher than 10,000 as correlation computation is carried out in memory. Based on the correlation
        a better permutation for the bits is computed. The permutation is applied only for all documents in the  index,
        or in case of interruption, none. This is achieved by using a temporary copy of the retrieval index.
        This step is also needed to find increase performance on the short codes as these are the most discriminative
        ones of the long codes. More details can be found in https://arxiv.org/abs/2305.04710

        :param plot_dir: directory for correlation and bit distribution plots
        :type plot_dir: str
        :param num_samples: number of samples to use for computing the correlation matrix
        :type num_samples: int
        :return: True if decorrelation was successful, False otherwise
        """
        if num_samples is not None:
            self.num_decorrelate = num_samples
        tmp = tempfile.NamedTemporaryFile()
        self.es.indices.add_block(index=self.index_name, block="write")
        self._save_codes(tmp)
        self._remove_blocks(self.index_name)
        codes = list(self._read_codes(tmp))

        logger.info("Starting decorrelation...")

        codes = code_array(codes)
        corr = correlation(codes)
        perm = compute_perm(corr)

        if len(perm) == 0:
            self._init_perm()
            logger.warning("Could not compute correlation. Bit-permutation stays the same as before.")
            return False

        self.perm_long = perm

        if plot_dir:
            os.makedirs(plot_dir, exist_ok=True)
            plot_hist(codes, os.path.join(plot_dir, "counts.png"))
            plot_corr(corr, os.path.join(plot_dir, "corr.png"))

        num_subcodes = int(self.bincode_len / self.subcode_len)
        self.perm_short = perm[0:self.subcode_len_short] + \
                          perm[num_subcodes:num_subcodes + self.subcode_len_short] + \
                          perm[2 * num_subcodes:2 * num_subcodes + self.subcode_len_short] + \
                          perm[3 * num_subcodes:3 * num_subcodes + self.subcode_len_short]

        self._update_perm()
        self.is_decorrelated = True

        logger.info("All codes decorrelated.")
        return True

    def reset(self):
        """
        Delete and recreate all indices. This will also delete all documents in the retrieval index.
        """
        if self.es.indices.exists(index=self.index_name):
            self._remove_blocks(self.index_name)
            self.es.indices.delete(index=[self.index_name])
        if self.es.indices.exists(index=self.nbs_index_name):
            self._remove_blocks(self.nbs_index_name)
            self.es.indices.delete(index=[self.nbs_index_name])
        self._add_hdist()
        self._create_nbs_index()
        self.es.indices.add_block(index=self.nbs_index_name, block="write")
        self._create_index()
