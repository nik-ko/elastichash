import numpy as np
import unittest

from elastichash.util import subcodes2bincode


class ElasticHashUtilsTest(unittest.TestCase):

    def test_subcode_zeros(self):
        r = {"r0": 0, "r1": 0, "r2": 0, "r3": 0}
        c1 = subcodes2bincode(r)
        c2 = np.zeros(shape=c1.shape, dtype=np.uint8)
        self.assertTrue((c1 == c2).any())
