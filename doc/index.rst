.. melastic documentation master file, created by
   sphinx-quickstart on Sat Sep 24 14:06:36 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to melastic's documentation!
====================================

Melastic (my elastic) is a library for performing bulk operations on an `ElasticSearch <http://elastic.io>`_ node.

First, specify some :ref:`configuration parameters <config_label>` for connecting to your node::

    import melastic
    config = melastic.Config(
        "http://yournode.com", {"Authorization": "Basic your_auth_token"},
        "your_index", "your_document_type"
    )

You can then perform batch operations (:py:func:`melastic.bulk_create`, :py:func:`melastic.bulk_update`, :py:func:`melastic.bulk_index`, and :py:func:`melastic.bulk_delete`) on documents.

You can also :py:class:`melastic.Scroll` through existing documents::

    with Scroll(config, query) as scroll:
        for page in scroll:
            for item in page:
                do_stuff(item)

Contents:
---------

.. toctree::
   melastic
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

