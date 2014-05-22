class XMLParser(object):

      """Helper functions to process XML data."""

      def text_of_optional_subtag(self, tag, name):
            tag = tag.xpath(name)
            if tag:
                  return tag[0].text
            return None
      
      def text_of_subtag(self, tag, name):
            return tag.xpath(name)[0].text

      def int_of_subtag(self, tag, name):
            return int(self.text_of_subtag(tag, name))

      def process_all(self, xml, xpath, namespaces={}, handler=None):
            if not handler:
                  handler = self.process_one
            if isinstance(xml, str):
                  root = etree.fromstring(xml)
            else:
                  root = xml
            for i in root.xpath(xpath, namespaces=namespaces):
                  data = handler(i, namespaces)
                  if data:
                        yield data

      def process_one(self, tag, namespaces):
            return None
