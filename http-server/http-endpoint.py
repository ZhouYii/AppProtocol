#server logic
from pprint import pprint
from twisted.application.internet import TCPServer
from twisted.application.service import Application
from twisted.web.resource import Resource
from twisted.web.server import Site

#business logic
import json
import base64

PORT = 8000

class FormPage(Resource):
    def render_GET(self, request):
        return ''

    def render_POST(self, request):
        contents = request.content.read()
        json_dict = json.loads(contents)
        print json_dict.keys()

        for key, value in json_dict.items() :
            decoded = base64.b64decode(value)
            img = open("image.jpg", "wb")
            img.write(decoded)
            img.close()
            print "first 50 chars"
            print value[:50]

        '''
        json_dict = json.loads(newdata)
        # Parse json dict
        if json_dict.has_key("img_raw") :
            img = open("img.jpg", "wb")
            img.write(base64.b64decode(json_dict["img_raw"]))
            img.close()
        '''
        return '<html><body>success-striing</body></html>'

root = Resource()
root.putChild("form", FormPage())
application = Application("Http Enpoint for App")
TCPServer(PORT, Site(root)).setServiceParent(application)
