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


class Batch(object):

    def __init__(self, config, docs):
        self.config = config
        self.docs = docs

    def serialize(self):
        raise NotImplementedError("abstract method")

    def push(self):
        raise NotImplementedError("abstract method")


class BulkCreate(Batch):

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
            lines.append(json.dumps(doc["src"]))
        #
        # N.B. ElasticSearch requires a final newline.
        #
        return "\n".join(lines) + "\n"

    def push(self):
        """Returns a dictionary that keys URLs to their Elastic IDs."""
        if not self.docs:
            return self.docs

        data = self.serialize()

        r = requests.post(
            self.config.http_endpoint + "/_bulk",
            headers=self.config.http_headers, data=data
        )
        LOGGER.debug("push: r.status_code: %s", r.status_code)
        LOGGER.debug(r.text)
        assert r.status_code == httplib.OK

        reply = json.loads(r.text)
        if reply["errors"]:
            LOGGER.warning("push: there were some errors")

        assert len(reply["items"]) == len(self.docs)

        for i, remote in enumerate(reply["items"]):
            self.docs[i]["_id"] = remote["create"]["_id"]
            self.docs[i]["status"] = remote["create"]["status"]

        return self.docs


class BulkUpdate(Batch):

    def __init__(self, *args, **kwargs):
        self.action = kwargs.pop("action", "update")
        super(BulkUpdate, self).__init__(*args, **kwargs)

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
            lines.append(json.dumps({"doc": doc["src"]}))
        return "\n".join(lines) + "\n"

    def push(self):
        if not self.docs:
            return self.docs

        data = self.serialize()

        r = requests.post(
            self.config.http_endpoint + "/_bulk",
            headers=self.config.http_headers, data=data
        )
        LOGGER.debug("push: r.status_code: %s", r.status_code)
        LOGGER.debug(r.text)
        assert r.status_code == httplib.OK

        reply = json.loads(r.text)
        if reply["errors"]:
            LOGGER.warning("push: there were some errors")

        for i, remote in enumerate(reply["items"]):
            self.docs[i]["status"] = remote[self.action]["status"]

        return self.docs


class BulkIndex(BulkUpdate):
    """An index is similar to an update, but it replaces the entire document as
    opposed to updating parts of it. The benefit of index is that it allows
    fields to be deleted or renamed."""

    def __init__(self, *args, **kwargs):
        kwargs["action"] = "index"
        super(BulkIndex, self).__init__(*args, **kwargs)

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
            lines.append(json.dumps(doc["src"]))
        return "\n".join(lines) + "\n"


class BulkDelete(Batch):

    def serialize(self):
        lines = []
        for doc in self.docs:
            action = {
                "delete": {
                    "_index": self.index, "_type": self.doctype,
                    "_id": doc["_id"]
                }
            }
            lines.append(json.dumps(action))
        return "\n".join(lines) + "\n"

    def push(self):
        if not self.docs:
            return

        data = self.serialize()

        r = requests.post(
            self.config.http_endpoint + "/_bulk", headers=self.http_headers,
            data=data
        )
        LOGGER.debug("push: r.status_code: %s", r.status_code)
        LOGGER.debug(r.text)
        assert r.status_code == httplib.OK


Scroll = collections.namedtuple(
    "Scroll", ["scroll_id", "total_hits", "total_pages", "first_page"]
)


class Scroll(object):

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
        LOGGER.debug(r.text)
        assert r.status_code == httplib.OK
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

    def __next__(self):
        if self.scroll_id is None:
            self.__open()

        assert self.num_pages

        if self.first_page:
            result = self.first_page
            self.first_page = None
            return result
        elif self.next_page_num >= self.num_pages:
            raise StopIteration

        r = requests.get(
            self.config.http_endpoint + "/_search/scroll",
            headers=self.config.http_headers,
            params={"scroll": self.lifetime, "scroll_id": self.scroll_id}
        )
        assert r.status_code == httplib.OK
        return json.loads(r.text)["hits"]["hits"]
