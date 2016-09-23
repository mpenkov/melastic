import unittest
import json

import mock
import requests  # NOQA we need this for mocking
import six.moves.http_client as httplib

from . import melastic


DUMMY_CONFIG = melastic.Config("http://foo.bar", {}, "foo", "bar")


class BulkCreateTest(unittest.TestCase):

    @mock.patch("requests.post")
    def test(self, mock_post):
        docs = [{"src": {"text": "dummy text"}}]
        create = melastic.BulkCreate(DUMMY_CONFIG, docs)
        self.assertEquals(create.docs, docs)

        jsonlines = create.serialize().split("\n")
        self.assertEquals(len(jsonlines), 3)
        self.assertEquals(
            json.loads(jsonlines[0]),
            {"create": {"_type": "bar", "_index": "foo"}}
        )
        self.assertEquals(
            json.loads(jsonlines[1]), {"text": "dummy text"}
        )
        self.assertEquals(jsonlines[2], "")

        mock_post.return_value = mock.MagicMock()
        mock_post.return_value.status_code = httplib.OK
        mock_post.return_value.text = """
{
    "took": 1, "errors": 0,
    "items": [
        {
            "create": {
                "status": "OK", "_index": "foo", "_type": "bar",
                "_id": "new_elastic_id", "_version": 1
            }
        }
    ]
}
"""
        create.push()
        #
        # http://stackoverflow.com/questions/3829742/assert-that-a-method-was-called-in-a-python-unit-test
        #
        self.assertTrue(mock_post.called)

        self.assertEquals(docs[0]["_id"], "new_elastic_id")
        self.assertEquals(docs[0]["status"], "OK")


class BulkUpdateTest(unittest.TestCase):

    @mock.patch("requests.post")
    def test(self, mock_post):
        docs = [{"_id": "abc", "src": {"text": "dummy text"}}]
        update = melastic.BulkUpdate(DUMMY_CONFIG, docs)
        self.assertEquals(update.docs, docs)

        jsonlines = update.serialize().split("\n")
        self.assertEquals(len(jsonlines), 3)
        self.assertEquals(
            json.loads(jsonlines[0]),
            {"update": {"_type": "bar", "_id": "abc", "_index": "foo"}}
        )
        self.assertEquals(
            json.loads(jsonlines[1]),
            {"doc": {"text": "dummy text"}}
        )

        mock_post.return_value = mock.MagicMock()
        mock_post.return_value.status_code = httplib.OK
        mock_post.return_value.text = """
{
    "took": 1, "errors": 0,
    "items": [
        {
            "update": {
                "status": "OK", "_index": "foo", "_type": "bar",
                "_id": "abc", "_version": 1
            }
        }
    ]
}
"""
        update.push()
        #
        # http://stackoverflow.com/questions/3829742/assert-that-a-method-was-called-in-a-python-unit-test
        #
        self.assertTrue(mock_post.called)
        self.assertEquals(docs[0]["status"], "OK")


class BulkIndexTest(unittest.TestCase):

    @mock.patch("requests.post")
    def test(self, mock_post):
        docs = [{"_id": "abc", "src": {"text": "dummy text"}}]
        index = melastic.BulkIndex(DUMMY_CONFIG, docs)
        self.assertEquals(index.docs, docs)

        jsonlines = index.serialize().split("\n")
        self.assertEquals(len(jsonlines), 3)
        self.assertEquals(
            json.loads(jsonlines[0]),
            {"index": {"_type": "bar", "_id": "abc", "_index": "foo"}}
        )
        self.assertEquals(jsonlines[1], """{"text": "dummy text"}""")
        self.assertEquals(jsonlines[2], "")

        mock_post.return_value = mock.MagicMock()
        mock_post.return_value.status_code = httplib.OK
        mock_post.return_value.text = """
{
    "took": 1, "errors": 0,
    "items": [
        {
            "index": {
                "status": "OK", "_index": "foo", "_type": "bar",
                "_id": "abc", "_version": 1
            }
        }
    ]
}
"""
        index.push()
        #
        # http://stackoverflow.com/questions/3829742/assert-that-a-method-was-called-in-a-python-unit-test
        #
        self.assertTrue(mock_post.called)
        self.assertEquals(docs[0]["status"], "OK")

DUMMY_QUERY = {"query": {"match_all": {}}}


class ScrollTest(unittest.TestCase):

    @mock.patch("requests.get")
    @mock.patch("requests.delete")
    def test_object(self, mock_delete, mock_get):
        scroll = melastic.Scroll(DUMMY_CONFIG, DUMMY_QUERY)

        self.assertIsNone(scroll.scroll_id)
        with self.assertRaises(AssertionError):
            scroll._Scroll__close()

        mock_get.return_value = mock.MagicMock()
        mock_get.return_value.status_code = httplib.OK
        mock_get.return_value.text = """
{
  "_scroll_id": "dummy_scroll_id",
  "took": 1,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  },
  "hits": {
    "total": 10,
    "max_score": 1,
    "hits": [
      {
        "_index": "foo",
        "_type": "bar",
        "_id": "abc",
        "_score": 1,
        "_source": {
          "text": "hello world!"
        }
      }
    ]
  }
}
"""
        scroll._Scroll__open()

        self.assertEquals(scroll.num_pages, 10)
        self.assertEquals(scroll.scroll_id, "dummy_scroll_id")
        self.assertEquals(len(scroll.first_page), 1)
        self.assertEquals(scroll.first_page[0]["_id"], "abc")

        first_page = next(scroll)
        self.assertEquals(len(first_page), 1)
        self.assertEquals(first_page[0]["_id"], "abc")

        mock_get.return_value.text = """
{
  "_scroll_id": "dummy_scroll_id2",
  "took": 1,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  },
  "hits": {
    "total": 10,
    "max_score": 1,
    "hits": [
      {
        "_index": "foo",
        "_type": "bar",
        "_id": "def",
        "_score": 1,
        "_source": {
          "text": "hey man!"
        }
      }
    ]
  }
}
"""
        second_page = next(scroll)
        self.assertIsNone(scroll.first_page)
        self.assertEquals(len(second_page), 1)
        self.assertEquals(second_page[0]["_id"], "def")

        scroll._Scroll__close()

    @mock.patch("requests.get")
    @mock.patch("requests.delete")
    def test_context_manager(self, mock_delete, mock_get):
        mock_get.return_value = mock.MagicMock()
        mock_get.return_value.status_code = httplib.OK
        mock_get.return_value.text = """{
  "_scroll_id": "dummy_scroll_id",
  "took": 1,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  },
  "hits": {
    "total": 1,
    "max_score": 1,
    "hits": [
      {
        "_index": "foo",
        "_type": "bar",
        "_id": "abc",
        "_score": 1,
        "_source": {
          "text": "hello world!"
        }
      }
    ]
  }
}"""
        with melastic.Scroll(DUMMY_CONFIG, DUMMY_QUERY) as scroll:
            pages = list(scroll)
            self.assertEquals(len(pages), 1)