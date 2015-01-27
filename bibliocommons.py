from nose.tools import set_trace

from datetime import (
    datetime,
    timedelta,
)
import os
import json
import urlparse

from core.model import (
    DataSource,
    Representation,
)

class BibliocommonsBase(object):

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def _parse_time(self, t):
        return datetime.strptime(t, self.TIME_FORMAT)

    def parse_times_in_place(self, data):
        for i in ('updated', 'created'):
            if i in data:
                data[i] = self._parse_time(data[i])


class BibliocommonsAPI(BibliocommonsBase):

    LIST_MAX_AGE = timedelta(days=1)
    TITLE_MAX_AGE = timedelta(days=30)

    BASE_URL = "https://api.bibliocommons.com/v1"
    LIST_OF_USER_LISTS_URL = "users/{user_id}/lists"
    LIST_URL = "lists/{list_id}" 
    TITLE_URL = "titles/{title_id}" 

    def __init__(self, _db, api_key=None, do_get=None):
        self._db = _db
        self.api_key = api_key or os.environ['BIBLIOCOMMONS_API_KEY']
        self.source = DataSource.lookup(self._db, DataSource.BIBLIOCOMMONS)
        self.do_get = do_get or Representation.http_get_no_timeout

    def request(self, path, max_age=LIST_MAX_AGE, identifier=None,
                do_get=None):
        if not path.startswith("/"):
            path = "/" + path
        if not path.startswith(self.BASE_URL):
            url = self.BASE_URL + path
        joiner = '?'
        if '?' in url:
            joiner = '&'
        url += joiner + "api_key=" + self.api_key

        representation, cached = Representation.get(
            self._db, url, data_source=self.source, identifier=identifier,
            do_get=self.do_get, max_age=max_age, pause_before=1, debug=True)
        content = json.loads(representation.content)
        return content

    def list_pages_for_user(self, user_id, max_age=LIST_MAX_AGE):
        url = self.LIST_OF_USER_LISTS_URL.format(user_id=user_id)
        first_page = self.request(url, max_age)
        yield first_page
        if first_page['pages'] > 1:
            max_page = first_page['pages']
            for page_num in range(2, max_page+1):
                page_arg = "?page=%d" % page_num
                page_url = url + page_arg
                next_page = self.request(page_url, max_age)
                yield next_page

    def list_data_for_user(self, user_id, max_age=LIST_MAX_AGE):
        for page in self.list_pages_for_user(user_id, max_age):
            for list_data in page['lists']:                
                self.parse_times_in_place(list_data)
                yield list_data

    def get_list(self, list_id):
        url = self.LIST_URL.format(list_id=list_id)
        return self._make_list(self, self.request(url))

    def get_title(self, title_id):
        url = self.TITLE_URL.format(title_id=title_id)
        data = self.request(url, max_age=self.TITLE_MAX_AGE)
        return self._make_title(self, data)

    def _make_title(self, data):
        if not 'title' in data:
            return None
        return BibliocommonsTitle(data['title'])

    def _make_list(self, data):
        return BibliocommonsList(data)

class BibliocommonsList(BibliocommonsBase):

    def __init__(self, json_data):
        self.items = []
        list_data = json_data['list']
        self.parse_times_in_place(list_data)
        for i in (
                'id', 'name', 'description', 'list_type', 'language',
                'created', 'updated'
        ):
            setattr(self, i, list_data.get(i, None))

        self.items = []
        for li_data in list_data.get('list_items', []):
            item = BibliocommonsListItem(li_data)
            self.items.append(item)

    def __iter__(self):
        return self.items.__iter__()

class BibliocommonsListItem(BibliocommonsBase):

    TITLE_TYPE = "title"

    def __init__(self, item_data):
        self.annotation = item_data.get('annotation')
        self.type = item_data.get('list_item_type')
        if self.type == self.TITLE_TYPE and 'title' in item_data:
            self.item = BibliocommonsTitle(item_data['title'])
        else:
            self.item = item_data

class BibliocommonsTitle(BibliocommonsBase):

    def __init__(self, data):
        self.data = data

    def __getitem__(self, k):
        return self.data.get(k, None)

    def to_edition(self, _db):
        # Create or locate a Simplified edition for this Bibliocommons
        # title.
        pass
