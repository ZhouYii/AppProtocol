import httplib2
from datetime import datetime
import simplejson
import base64


# Prepare test data/create the json
img = open("test.jpg", "rb")
byte_stream = img.read()
to_string = base64.b64encode(byte_stream)

json_obj = {'test-key' : 'test-data',
            'img_raw': to_string}

URL = 'http://localhost:8000/form'

jsondata = simplejson.dumps(json_obj)

h = httplib2.Http()
resp, content = h.request(URL,
                          'POST',
                          jsondata,
                          headers={'Content-Type': 'application/json'})
