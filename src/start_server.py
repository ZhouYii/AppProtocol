from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor
from db.database import * 
#from src.serveractions import register, login
from tools.util import *

class IphoneChat(Protocol):
    def __init__(self) :
        self.db_handle = init_session() 

    def connectionMade(self):
        self.factory.clients.append(self)
        print "clients are ", self.factory.clients
 
    def connectionLost(self, reason):
        self.factory.clients.remove(self)

    def message(self, message):
        self.transport.write(message + '\n')

    def message_phone_number_exists(self) :
        #TODO return a different message from invalid input to have
        # different actions on the client side
        self.message("-1")

    def dataReceived(self, data):
        a = data.split(':')
        print a
        if len(a) > 1:
            command = a[0]
            content = a[1]
 
            msg = ""
            if command == "iam":
                self.name = content
                msg = self.name + " has joined"

            elif command == "log" :
                phone_num, password = split_login_string(content)

                # validate input from phone 
                if protoc_validate_login(phone_num, password) == False  :
                    # invalid input
                    self.message("0")
                    return

                phone_num = sanitize_phone_number(phone_num)
    
                success = login_authenticate(self.db_handle, phone_num, password)
                if success :
                    self.message("1")
                else :
                    self.message("0")

	    elif command == "reg" :
                phone_num, password = split_login_string(content)
                print content
                print phone_num
                print password

                # validate input from phone 
                if protoc_validate_login(phone_num, password) == False  :
                    # invalid input
                    print "error 2"
                    self.message("0")
                    return

                phone_num = sanitize_phone_number(phone_num)
                if check_phonenumber_taken(self.db_handle, phone_num) == True :
                    print "error 1"
                    self.message_phone_number_exists()
                    return

                insert_user_into_database(self.db_handle, phone_num, password)
		self.message("1")
 
            elif command == "msg":
                msg = self.name + ": " + content
                print msg
            '''
            for c in self.factory.clients:
                c.message(msg)
            '''
 
factory = Factory()
factory.protocol = IphoneChat
factory.clients = []
reactor.listenTCP(80, factory)
print "server started"
handle = init_session()
reactor.run()

