Quickstart
=======================================

ElasticHash implements efficient similarity search by using a two-stage method for efficiently searching binary hash
codes using Elasticsearch.
In the first stage, a coarse search based on short hash codes is performed using multi-index hashing and ES terms lookup
of neighboring hash codes. In the second stage, the list of results is re-ranked by computing the Hamming distance on
long hash codes.

For a whole image similarity search system, including model training and model serving,
see https://github.com/umr-ds/ElasticHash.

.. important::

    Currently only 256 bit codes are supported


Install python package

.. code-block:: bash

   pip install elastichash

Create an Elastisearch client to use it with ElasticHash. ::

  es = Elasticsearch(elasticsearch_endpoint)
  eh = ElasticHash(es)

New items can be added by calling :func:`add` where `code` can be a list, string or numpy array together with additional fields ::

  eh.add(code, additional_fields={"image_path": "/path/to/an/image"})

After adding a suffiently large amount of codes (e.g. 10,000), :func:`decorrelate` needs to be called to rearrange the binary hashcode permutations

Search documents by their hash code use :func:`search`. By string: ::

    search('0010100101010010010100100100101000101001010100100101001001001010010101001001010010010100100101001010100100101001001010010001001001010010101001001010010010100100010101001001010010010100100101001010100100101001001010010001001001010010101001001010010010100100')

Or by list of integer: ::

    search(['0','0','1','0','1','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','0','1','0','1','0','0','0','1','0','1','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','0','1','0','1','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','0','1','0','0','1','0','0','1','0','1','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','0','1','0','0','1','0','0','1','0','1','0','0','1','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0','1','0','1','0','0','1','0','0'])

Or use a list or `numpy.ndarray` of four (long) int values as query::

    search([1,-1,1000,-1000])
    search(numpy.array([1, -1, 1000, -1000])