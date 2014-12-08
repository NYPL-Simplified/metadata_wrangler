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

class BibliocommonsAPI(object):

    BASE_URL = "https://api.bibliocommons.com/v1"
    LIST_OF_USER_LISTS_URL = "users/{user_id}/lists"
    LIST_URL = "lists/{list_id}" 
    TITLE_URL = "titles/{title_id}" 

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, _db, api_key=None, do_get=None):
        self._db = _db
        self.api_key = api_key or os.environ['BIBLIOCOMMONS_API_KEY']
        self.source = DataSource.lookup(self._db, DataSource.BIBLIOCOMMONS)
        self.do_get = do_get or Representation.http_get_no_timeout

    def _parse_time(self, t):
        return datetime.strptime(t, self.TIME_FORMAT)

    def request(self, path, max_age=None, identifier=None, do_get=None):
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
            do_get=self.do_get, max_age=max_age, debug=True)
        content = json.loads(representation.content)
        return content

    def list_pages_for_user(self, user_id, max_age=None):
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

    def list_data_for_user(self, user_id, max_age=None):
        for page in self.list_pages_for_user(user_id, max_age):
            for list_data in page['lists']:                
                for i in ('updated', 'created'):
                    if i in list_data:
                        list_data[i] = self._parse_time(list_data[i])
                yield list_data

    def get_list(self, list_id, last_modified=None):
        url = self.LIST_URL.format(list_id=list_id)
        return self.request(url, None)

    def get_title(self, title_id, last_modified=None):
        url = self.TITLE_URL.format(title_id=title_id)
        return self.request(url, None)
