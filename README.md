[![build](https://github.com/nik-ko/elastichash/actions/workflows/test.yml/badge.svg)](https://github.com/nik-ko/elastichash/actions/workflows/test.yml) 
[![doc](https://github.com/nik-ko/elastichash/actions/workflows/documentation.yml/badge.svg)](https://github.com/nik-ko/elastichash/actions/workflows/documentation.yml)
[![PyPI version](https://img.shields.io/pypi/v/elastichash.svg)](https://pypi.python.org/pypi/elastichash)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# ElasticHash

## Introduction

ElasticHash implements efficient similarity search by using a two-stage method for efficiently searching binary hash 
codes using Elasticsearch. 
In the first stage, a coarse search based on short hash codes is performed using multi-index hashing and ES terms lookup 
of neighboring hash codes. In the second stage, the list of results is re-ranked by computing the Hamming distance on 
long hash codes.

The only requirement ist that binary codes to be indexed need to be 256 bits long as currently only 256 bit codes are 
supported.

For a whole image similarity search system, including model training and model serving, 
see https://github.com/umr-ds/ElasticHash.

## Install

`pip install elastichash`

## Usage

- Create an Elastisearch client to use it with ElasticHash
  ```
  es = Elasticsearch(elasticsearch_endpoint)
  eh = ElasticHash(es)
  ```
- New items can be added by calling `add(code)` where `code` can be a list, string or numpy array together with
  additional fields
  ```
  eh.add(code, additional_fields={"image_path": "/path/to/an/image"})
  ```
- After adding a suffiently large amount of codes (e.g. 10,000), `decorrelate()` needs to be called to rearrange the
  binary hashcode permutations
- To search documents by their hash code use `search(code)` 
