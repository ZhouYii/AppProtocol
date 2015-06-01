from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor

from db.database import * 
from tools.util import *
from routing import perform_routing

import json

class IphoneChat(Protocol):
    def __init__(self) :
        self.db_handle = init_session() 

    def connectionMade(self):
        self.factory.clients.append(self)
        print "clients are ", self.factory.clients
 
    def connectionLost(self, reason):
        self.factory.clients.remove(self)

    def message(self, message):
	print "Response: " + str(message)
        self.transport.write(str(message) + '\n')

    def message_phone_number_exists(self) :
        #TODO return a different message from invalid input to have
        # different actions on the client side
        self.message("-1")

    def dataReceived(self, data):
      #try :
      print "Recieved from client : " + str(data)
      perform_routing(self, self.db_handle, data)
      #except Exception :
      #  self.message("Incorrect Input Error")

factory = Factory()
factory.protocol = IphoneChat
factory.clients = []
reactor.listenTCP(80, factory)
print "server started"
handle = init_session()
reactor.run()
