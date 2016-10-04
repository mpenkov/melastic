"""Defines classes and methods for working with ElasticSearch."""
import json
import logging
import collections
import math

import requests
import six.moves.http_client as httplib

LOGGER = logging.getLogger(__name__)


Config = collections.namedtuple(
    "Config", ["http_endpoint", "http_headers", "index", "doctype"]
)
"""Stores the parameters for connecting to an ElasticSearch node."""


class HttpException(Exception):
    """Raised when the library receives an unexpected HTTP status code."""

    def __init__(self, code, body):
        super(HttpException, self).__init__("received HTTP %d" % code)
        self.code = code
        self.body = body


def bulk_create(config, docs):
    """Create documents in bulk."""
    return BulkCreate(config, docs).push()


def bulk_update(config, docs):
    """Update documents in bulk."""
    return BulkUpdate(config, docs).push()


def bulk_index(config, docs):
    """Index documents in bulk."""
    return BulkIndex(config, docs).push()


def bulk_delete(config, docs):
    """Delete documents in bulk."""
    return BulkDelete(config, docs).push()

ACTIONS = ["create", "update", "index", "delete"]


class Batch(object):
    """Represents an abstract batch operation."""

    def __init__(self, config, docs, action):
        if len(docs) == 0:
            raise ValueError("empty batch")
        if action not in ACTIONS:
            raise ValueError("bad action, %r not in %r" % (action, ACTIONS))
        self.config = config
        self.action = action
        self.docs = docs

    def check_batch(self):
        """Raises KeyError if any document of the batch is incompletely
        specified."""
        raise NotImplementedError("abstract method")

    def serialize(self):
        """Returns a string that could be sent directly to the bulk API."""
        raise NotImplementedError("abstract method")

    def push(self):
        """Sends the batch to the bulk API and processes results."""
        raise NotImplementedError("abstract method")

    def process_response(self, response_body):
        reply = json.loads(response_body)
        if reply["errors"]:
            LOGGER.warning("push: there were some errors")

        assert len(reply["items"]) == len(self.docs)

        for i, remote in enumerate(reply["items"]):
            #
            # TODO: if status != 200, then will there be an _id?
            #
            self.docs[i]["status"] = remote[self.action]["status"]
            self.docs[i]["_id"] = remote[self.action]["_id"]


class BulkCreate(Batch):
    """Creates new documents."""

    def __init__(self, config, docs):
        Batch.__init__(self, config, docs, "create")

    def check_batch(self):
        for doc in self.batch:
            doc["_source"]

    def serialize(self):
        lines = []
        for doc in self.docs:
            #
            # Contrary to what the documentation seems to say, we *must*
            # include action prior to every item.
            #
            action = {
                "create": {
                    "_index": self.config.index, "_type": self.config.doctype
                }
            }
            lines.append(json.dumps(action))
            lines.append(json.dumps(doc["_source"]))
        #
        # N.B. ElasticSearch requires a final newline.
        #
        return "\n".join(lines) + "\n"

    def push(self):
        """Returns the created documents as a list of dictionaries.
        Each returned document will have an _id and status."""
        data = self.serialize()

        r = requests.post(
            self.config.http_endpoint + "/_bulk",
            headers=self.config.http_headers, data=data
        )
        LOGGER.debug("push: r.status_code: %s", r.status_code)
        LOGGER.debug(r.text)

        if r.status_code != httplib.OK:
            raise HttpException(r.status_code, r.text)

        self.process_response(r.text)

        return self.docs


class BulkUpdate(Batch):
    """Updates documents by replacing the values of existing fields or adding
    new ones.  Does not delete existing fields."""

    def __init__(self, config, docs, action="update"):
        Batch.__init__(self, config, docs, action)

    def check_batch(self):
        for doc in self.batch:
            doc["_source"]
            doc["_id"]

    def serialize(self):
        #
        # http://stackoverflow.com/questions/28434111/requesterror-while-updating-the-index-in-elasticsearch
        # N.B. ElasticSearch requires a final newline.
        #
        lines = []
        for doc in self.docs:
            action = {
                "update": {
                    "_index": self.config.index, "_type": self.config.doctype,
                    "_id": doc["_id"]
                }
            }
            lines.append(json.dumps(action))
            lines.append(json.dumps({"doc": doc["_source"]}))
        return "\n".join(lines) + "\n"

    def push(self):
        """Returns the documents.  Each document will contain its update
        status."""
        data = self.serialize()

        r = requests.post(
            self.config.http_endpoint + "/_bulk",
            headers=self.config.http_headers, data=data
        )
        LOGGER.debug("push: r.status_code: %s", r.status_code)
        LOGGER.debug(r.text)

        if r.status_code != httplib.OK:
            raise HttpException(r.status_code, r.text)

        self.process_response(r.text)

        return self.docs


class BulkIndex(BulkUpdate):
    """An index is similar to an update, but it replaces the entire document as
    opposed to updating parts of it. The benefit of index is that it allows
    fields to be deleted or renamed."""

    def __init__(self, config, docs, action="index"):
        Batch.__init__(self, config, docs, action)

    def serialize(self):
        lines = []
        for doc in self.docs:
            action = {
                "index": {
                    "_index": self.config.index, "_type": self.config.doctype,
                    "_id": doc["_id"]
                }
            }
            lines.append(json.dumps(action))
            lines.append(json.dumps(doc["_source"]))
        return "\n".join(lines) + "\n"


class BulkDelete(Batch):
    """Deletes the documents specified in the batch."""
    def __init__(self, config, docs):
        Batch.__init__(self, config, docs, "delete")

    def check_batch(self):
        for doc in self.batch:
            doc["_id"]

    def serialize(self):
        lines = []
        for doc in self.docs:
            action = {
                "delete": {
                    "_index": self.config.index, "_type": self.config.doctype,
                    "_id": doc["_id"]
                }
            }
            lines.append(json.dumps(action))
        return "\n".join(lines) + "\n"

    def push(self):
        data = self.serialize()

        r = requests.post(
            self.config.http_endpoint + "/_bulk",
            headers=self.config.http_headers, data=data
        )
        LOGGER.debug("push: r.status_code: %s", r.status_code)
        LOGGER.debug(r.text)

        if r.status_code != httplib.OK:
            raise HttpException(r.status_code, r.text)

        self.process_response(r.text)

        return self.docs


class Scroll(object):
    """Represents a scrollable query on the ElasticSearch node."""

    def __init__(self, config, query, lifetime="1m"):
        #
        # Specify the scroll size inside the query
        #
        self.config = config
        self.query = query
        self.lifetime = lifetime

        self.scroll_id = None
        self.total_hits = None
        self.num_pages = None
        self.current_page_num = None

    def __enter__(self):
        self.__open()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.__close()

    def __open(self):
        assert self.scroll_id is None, "scroll is already open"

        LOGGER.debug("Scroll.__open: query: %r", json.dumps(self.query))
        r = requests.get(
            self.config.http_endpoint + "/{:s}/{:s}/_search".format(
                self.config.index, self.config.doctype
            ),
            params={"scroll": self.lifetime},
            headers=self.config.http_headers, data=json.dumps(self.query)
        )
        LOGGER.debug(r.status_code)
        LOGGER.debug(r.text)

        if r.status_code != httplib.OK:
            raise HttpException(r.status_code, r.text)

        r = json.loads(r.text)

        self.scroll_id = r["_scroll_id"]
        self.total_hits = r["hits"]["total"]
        self.first_page = r["hits"]["hits"]
        self.next_page_num = 1

        if self.total_hits:
            self.num_pages = int(
                math.ceil(self.total_hits / len(self.first_page))
            )
        else:
            self.num_pages = 0

    def __close(self):
        assert self.scroll_id, "scroll is not open"
        requests.delete(
            self.config.http_endpoint + "/_search/scroll",
            headers=self.config.http_headers,
            params={"scroll_id": self.scroll_id}
        )

    def __iter__(self):
        return self

    def next(self):
        #
        # For Python 2.7 compatibility
        #
        return self.__next__()

    def __next__(self):
        LOGGER.debug("Scroll.__next__: called scroll_id: %r", self.scroll_id)
        if self.scroll_id is None:
            self.__open()

        assert self.num_pages

        if self.first_page:
            result = self.first_page
            self.first_page = None
            return result
        elif self.next_page_num >= self.num_pages:
            raise StopIteration

        self.next_page_num += 1

        r = requests.get(
            self.config.http_endpoint + "/_search/scroll",
            headers=self.config.http_headers,
            params={"scroll": self.lifetime, "scroll_id": self.scroll_id}
        )
        LOGGER.debug(r.status_code)
        LOGGER.debug(r.text)

        if r.status_code != httplib.OK:
            raise HttpException(r.status_code, r.text)
        return json.loads(r.text)["hits"]["hits"]

    def __len__(self):
        """Return the number of pages in this scroll."""
        return self.num_pages

    def __repr__(self):
        return "Scroll(%r, %r, %r, %r, %r, %r)" % (
            self.query, self.lifetime, self.scroll_id, self.total_hits,
            self.num_pages, self.current_page_num
        )
