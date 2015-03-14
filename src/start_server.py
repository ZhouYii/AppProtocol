from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor
from db.database import * 
#from src.serveractions import register, login
from tools.util import *
import newsfeed.newsfeed as nf
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
        self.transport.write(message + '\n')

    def message_phone_number_exists(self) :
        #TODO return a different message from invalid input to have
        # different actions on the client side
        self.message("-1")

    def dataReceived(self, data):
        opcode, message = split_opcode(data)

        if opcode == "iam":
            self.name = message
            msg = self.name + " has joined"

        elif opcode == "newstatus" :
            '''
                Assumes message is json-formatted with the following fields :
                id : phone number identifying the user who posted the status
                body : text body
                photos (optional) : sequence of bytes representing a photo
            '''
            content = json.load(message)
            phone_num = content["id"]
            text = content["body"]
            if content.has_key("photo") :
                photo = content["photo"]
            else :
                photo = 0
            new_status_update(self.db_handle, user_id, content, photo)

        elif opcode == "pollnews" :
            # pollnews:userid (phone number)
            phonenum = int(msg)
            json = get_user_timeline(self.db_handle, phonenum)
            self.message(json)

        '''
        elif opcode == "newstatus" :
            userid, content = first_split(message, "#")
            # delegate to newsfeed lib.
            nf.new_status_update(self.db_handle, user_id, content)
        '''

        elif opcode == "addfriend" :
            id1,id2 = [long(x) for x in data.split('#')]

        elif opcode == "img" :
            print type(message)
            key, bytestring = split_opcode(message)
            print message
            # todo delete 
            output = open("recv.jpg", "wb")
            output.write(bytestring)

        elif opcode == "log" :
            phone_num, password = split_login_string(message)

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

        elif opcode == "reg" :
            phone_num, password = split_login_string(message)

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

        elif opcode == "msg":
            msg = self.name + ": " + message
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
