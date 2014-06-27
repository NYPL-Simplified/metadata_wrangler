class OpenSearchDocument(object):
    """Generates OpenSearch documents."""

    TEMPLATE = """
 <?xml version="1.0" encoding="UTF-8"?>
 <OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">
   <ShortName>%(name)s</ShortName>
   <Description>%(description)s</Description>
   <Tags>%(tags)s</Tags>
   <Url type="application/atom+xml;profile=opds-catalog" template="%(url_template)s"/>
 </OpenSearchDescription>"""

    @classmethod
    def search_info(cls, languages, lane):

        d = dict()
        tags = languages.split(",")
        if lane is not None:
            tags.append(lane.lower().replace(" ", "-"))
            name = "%s books (Language=%s)" % (lane, languages)
            description = "Search for %s" % lane
        else:
            name = "Books (Language=%s)" % languages
            description = "Search for books"
        d['description'] = description
        d['name'] = name
        d['tags'] = " ".join(tags)
        return d

    @classmethod
    def for_lane(cls, languages, lane, base_url):
        info = cls.search_info(languages, lane)
        info['url_template'] = base_url + "?q={searchTerms}"

        return cls.TEMPLATE % info
