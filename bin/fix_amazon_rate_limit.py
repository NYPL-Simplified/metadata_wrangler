#!/usr/bin/env python
from bs4 import BeautifulSoup
import sys
import os
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))
import urlparse
from core.model import (
    production_session,
    Representation,
)
from amazon import AmazonAPI
from bs4 import BeautifulSoup
bad_url = sys.argv[1]

_db = production_session()

print "So the problematic URL is %s..." % bad_url
rep, cached = Representation.get(_db, bad_url, max_age=0)
if AmazonAPI.RATE_LIMIT_TEXT not in rep.content:
    print "Looks fine to me:"
    print rep.content
    sys.exit()

soup = BeautifulSoup(rep.content)
form = soup.find('form', action='/errors/validateCaptcha')
fields = {}
for i in form.find_all('input', type='hidden'):
    fields[i['name']] = i['value']

captcha_field_name = form.find('input', type='text')['name']

print "CAPTCHA URL is:"
for img in form.find_all('img'):
    print img['src']
print "Enter CAPTCHA value from URL:"
value = sys.stdin.readline().strip()
fields[captcha_field_name] = value

field_data = [k + "=" + v for k, v in fields.items()]
url = urlparse.urljoin("http://www.amazon.com", form['action']) + "?" + "&".join(field_data)
print "Okay, trying %s" % url
referer = dict(Referer=bad_url)
rep, cached = Representation.get(_db, url, extra_request_headers=referer, 
                                 max_age=0)
print rep.content
