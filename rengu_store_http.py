# -*- coding: utf-8 -*-
from io import BytesIO, SEEK_SET, SEEK_END
from urllib.parse import urlencode
from json import loads, dumps
from uuid import UUID

import requests
from splitstream import splitfile

from rengu.store import RenguStore

ITER_SIZE = 65536

# From https://gist.github.com/obskyr/b9d4b4223e7eaf4eedcd9defabb34f13
class ResponseStream(object):
    def __init__(self, request_iterator):
        self._bytes = BytesIO()
        self._iterator = request_iterator

    def _load_all(self):
        self._bytes.seek(0, SEEK_END)
        for chunk in self._iterator:
            self._bytes.write(chunk)

    def _load_until(self, goal_position):
        current_position = self._bytes.seek(0, SEEK_END)
        while current_position < goal_position:
            try:
                current_position = self._bytes.write(next(self._iterator))
            except StopIteration:
                break

    def tell(self):
        return self._bytes.tell()

    def read(self, size=None):
        left_off_at = self._bytes.tell()
        if size is None:
            self._load_all()
        else:
            goal_position = left_off_at + size
            self._load_until(goal_position)

        self._bytes.seek(left_off_at)
        return self._bytes.read(size)

    def seek(self, position, whence=SEEK_SET):
        if whence == SEEK_END:
            self._load_all()
        else:
            self._bytes.seek(position, whence)


class RenguStoreHttp(RenguStore):
    """Rengu store over HTTP"""

    def __init__(self, name: str, extra: list[str]):
        self.uri = name
        self.extra = extra

        self.cache = {}

    def __repr__(self):
        return f"RenguStoreHttp( {self.uri} )"

    class ResultSet:
        def __init__(self, parent, uri, args):

            self.parent = parent
            self.args = args
            self.uri = uri

        def __iter__(self):
            headers = {"Accept": "application/json", "Accept-Encoding": "gzip, deflate"}
            r = requests.get(self.uri, {"q": self.args}, stream=True, headers=headers)
            self.stream = splitfile(
                ResponseStream(r.iter_content(ITER_SIZE)), format="json"
            )

            return self

        def __next__(self):
            x = loads(next(self.stream))
            ID = UUID(x.get("ID"))
            self.parent.cache[ID] = x
            return ID

        def __repr__(self):

            return dumps(self.args)

    def __len__(self):
        return 100000

    def get(self, ID: UUID) -> dict:
        return self.cache.get(ID)

    def query(
        self,
        args: list[str],
        start: int = 0,
        count: int = -1,
        default_operator: str = "&",
        result: "RenguStoreHttp.ResultSet" = None,
        with_data: bool = True,
    ):

        # headers = {"Accept": "application/json", "Accept-Encoding": "gzip, deflate"}
        # with requests.get(self.uri, {"q": args}, stream=True, headers=headers) as r:
        #    stream = ResponseStream(r.iter_content(ITER_SIZE))
        #    yield from (loads(j) for j in splitfile(stream, format="json"))

        return self.ResultSet(self, self.uri, args)

    def save(self, obj):
        headers = {"Accept": "application/json", "Accept-Encoding": "gzip, deflate"}
        r = requests.post(self.uri, json=obj, headers=headers)
        return r.json().get("ID")

    def delete_not_implemented(self, ID):
        headers = {"Accept": "application/json", "Accept-Encoding": "gzip, deflate"}
        r = requests.delete(self.uri + f"/{ID}", headers=headers)
        return r.ok
