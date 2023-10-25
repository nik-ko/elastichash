import networkx as nx
import numpy as np
import seaborn as sns
from bitstring import BitArray
from itertools import combinations
from matplotlib import pyplot as plt
from networkx.algorithms.community.kernighan_lin import kernighan_lin_bisection as kl
from numpy.ma import masked_invalid, corrcoef
from typing import List, Optional, Union, Dict


def binstr2int(s: str) -> int:
    """
    Convert binary string to signed integer

    :param s: binary string
    :return: signed int
    """
    b = BitArray(bin=s)
    return b.int


def int2binstr(s: Optional[Union[int, str]], length: int = 64) -> str:
    """
    Convert an integer to a binary string

    :param s: a number
    :type s: str or int
    :param length: length of binary code
    :type length: int
    :return: binary string representation
    :rtype: str
    """
    if type(s) is str:
        s = int(s)
    b = BitArray(int=s, length=length)
    return b.bin


def binstr2uint(s: str) -> int:
    """
    Convert binary string to unsigned integer

    :param s: binary string
    :return: unsigned int representation of string
    """
    b = BitArray(bin=s)
    return b.uint


def nbs_masks(binstr_len: int = 16, d: int = 2) -> List[np.uint16]:
    """
    Generate mask for binary strings of length `l` and maximum Hamming distance `d`

    :param binstr_len: length of binary string
    :param d: Hamming distance
    :return: list of masks as binary strings
    """
    combs = []
    ids = range(binstr_len)
    for r in range(1, d + 1):
        combs += list(combinations(ids, r))
    masks = np.zeros((len(combs), binstr_len), dtype=np.int64)
    for i, c in enumerate(combs):
        masks[i, c] = 1
    masks_str = [(np.uint16)(binstr2uint("0" * binstr_len))] + [(np.uint16)(binstr2uint("".join(m))) for m in
                                                                masks.astype(str)]
    return masks_str


def get_nbs(q: int, masks: List[np.uint16]) -> np.ndarray[int]:
    """
    Compute all possible neighbors by applying masks to query

    :param q: binary query string
    :param masks: list of binary strings
    :return: list of neighbors as binary strings
    """
    return np.bitwise_xor(q, masks, dtype=int)


def subcodes2bincode(item: Dict[str, int], perm: List[int] = None) -> np.ndarray[np.int8]:
    """
    Converts subcode strings of length 64 to binary string and concatenates them to a binary string of length 256.
    Optionally a permutation can be applied to this string.

    :param item: Dict with fields r0, r1, r2, r3 containing subcodes as int
    :param perm: List of 256 indices for permuting the binary code
    :return: Binary code as numpy array
    """
    code = int2binstr(item["r0"]) + int2binstr(item["r1"]) + int2binstr(item["r2"]) + int2binstr(item["r3"])
    code = np.array([np.int8(x) for x in code])
    if perm is not None:
        code = code[perm]
    return code


def kl_partition(g: nx.Graph, partition_len: int):
    """
    Recursively applies KL algorithm to partition a graph into two partitions until partition does not contain more than
    `partition_len` elements-

    :param g: a list of weighted edges
    :param partition_len: number of edges in partition
    :return: list of partitions
    """

    def partition(g: nx.Graph, sc_weights: List[float]):
        if len(g) <= partition_len:
            sc_weights += [g.size(weight='weight')]
            return list(g.nodes)
        else:
            a_nodes, b_nodes = kl(g, partition=None, weight='weight', seed=42, max_iter=256)
            a = nx.subgraph_view(g, filter_node=lambda x: x in a_nodes)
            b = nx.subgraph_view(g, filter_node=lambda x: x in b_nodes)
            return partition(a, sc_weights) + partition(b, sc_weights)

    sc_weights = []
    p = list(map(int, partition(g, sc_weights)))
    return p, sc_weights


def code_array(codes: List[np.ndarray[np.int8]]):
    num_codes = len(codes)
    code_len = codes[0].shape[0]
    code_arr = np.empty((num_codes, code_len), dtype=np.int8)
    for i, code in enumerate(codes[:min(num_codes, len(codes))]):
        code_arr[i, :] = code
    return code_arr


def correlation(codes: np.ndarray[np.int8]):
    """
    Compute bitwise correlations for a set of binary codes.

    :param codes:
    :return:
    """
    codes = codes.T
    corr = abs(corrcoef(masked_invalid(codes)))
    return corr


def weighted_edges(corr: np.ndarray):
    """
    Extract positive weighted edges from a correlation matrix and return a list of edges (x, y, weight)

    :param corr: correlation matrix
    :type corr: np.ndarray[np.float]
    :return: List[Tuple]
    :rtype:
    """
    tri = np.tril(corr, -1)
    y, x = np.where(tri > 0)
    new_tri = 1 - tri
    out = [[x_i, y_i, new_tri[y_i][x_i]] for x_i, y_i in zip(x, y)]
    return out


def compute_perm(corr: np.ndarray, sc_len: int = 16):
    """
    Computes a better permutation of bits for a binary code based on a correlation matrix by applying the Kernighan–Lin
    algorithm. The bits are decorrelated between partitions (subcodes) of length `sc_len`.

    :param corr: correlation matrix
    :type corr: np.ndarray[np.float]
    :param sc_len: length of subcodes
    :type sc_len: int
    :return: a better bit permutation
    :rtype: List[int]
    """
    g = nx.Graph()
    g.add_weighted_edges_from(weighted_edges(corr))
    p, _ = kl_partition(g, sc_len)
    return p


def plot_corr(corr: np.ndarray, output_path="./corr.png"):
    plt.figure()
    plt.box(False)
    plt.rcParams["figure.figsize"] = (40, 40)
    sns.heatmap(corr)
    plt.plot()
    plt.savefig(output_path, bbox_inches='tight', pad_inches=0)


def plot_hist(codes: np.ndarray[np.int8], output_path="./counts.png"):
    plt.figure()
    plt.box(False)
    plt.rcParams["figure.figsize"] = (30, 15)
    counts1 = np.count_nonzero(codes > 0, axis=0)
    counts0 = np.count_nonzero(codes <= 0, axis=0)
    ind = np.arange(codes.shape[1])  # the x locations for the groups
    p1 = plt.bar(ind, counts1, width=1.0)
    p2 = plt.bar(ind, counts0, bottom=counts1, width=1.0)
    plt.legend((p1[0], p2[0]), ('1\'s', '0\'s'))
    plt.plot()
    plt.savefig(output_path, bbox_inches='tight', pad_inches=0)
