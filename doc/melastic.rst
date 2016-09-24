melastic package
================

Module Contents
---------------

.. automodule:: melastic
    :show-inheritance:

.. _config_label:

.. autoclass:: Config

    .. py:attribute:: http_endpoint

        The URL to your ElasticSearch node.

    .. py:attribute:: http_headers

        A dictionary of headers to send to the node with every request.
        Provide your Authorization parameters here.

    .. py:attribute:: index

        The index to work with.

    .. py:attribute:: doctype

        The document type to work with.


.. autofunction:: bulk_create

    Expects each document to be a dictionary of the form::

        {
            "_source": {
                "foo": "bar",
            }
        }

    :param config: The configuration to use when connecting to the node.
    :type config: A :ref:`Config <config_label>` namedtuple.
    :param docs: The documents to include in the batch.
    :type docs: A list of dictionaries.


.. autofunction:: bulk_update

    Expects each document to be a dictionary of the form::

        {
            "_id": 123,
            "_source": {
                "foo": "bar",
            }
        }

    :param config: The configuration to use when connecting to the node.
    :type config: A :ref:`Config <config_label>` namedtuple.
    :param docs: The documents to include in the batch.
    :type docs: A list of dictionaries.


.. autofunction:: bulk_index

    Expects each document to be a dictionary of the form::

        {
            "_id": 123,
            "_source": {
                "foo": "bar",
            }
        }

    :param config: The configuration to use when connecting to the node.
    :type config: A :ref:`Config <config_label>` namedtuple.
    :param docs: The documents to include in the batch.
    :type docs: A list of dictionaries.


.. autofunction:: bulk_delete

    Expects each document to be a dictionary of the form::

        {
            "_id": 123,
        }

    :param config: The configuration to use when connecting to the node.
    :type config: A :ref:`Config <config_label>` namedtuple.
    :param docs: The documents to include in the batch.
    :type docs: A list of dictionaries.


.. autoclass:: Scroll

    .. automethod:: __init__

        :param config: The configuration to use when connecting to the node.
        :type config: A :ref:`Config <config_label>` namedtuple.
        :param query: The query to scroll through.
        :type query: A dictionary.
        :param lifetime: How long to keep this scroll alive in between paging.
        :type lifetime: unicode.


.. autoclass:: Batch

    .. automethod:: __init__

        :param config: The configuration to use when connecting to the node.
        :type config: A :ref:`Config <config_label>` namedtuple.
        :param docs: The documents to include in the batch.
        :type docs: A list of dictionaries.

.. autoclass:: BulkCreate

    .. automethod:: push

.. autoclass:: BulkUpdate

.. autoclass:: BulkIndex

.. autoclass:: BulkDelete
