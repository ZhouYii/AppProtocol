from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor
from db.database import * 
#from src.serveractions import register, login
from tools.util import *
import newsfeed.newsfeed as nf
import json

def perform_routing(server_handle, db_handle, data) :
    opcode, message = split_opcode(data)
    print "opcode " + str(opcode) + " msg: " + str(message)
    if opcode == "iam":
        server_handle.name = message
        msg = server_handle.name + " has joined"

    elif opcode == "newstatus" :
        '''
            Assumes message is json-formatted with the following fields :
            id : phone number identifying the user who posted the status
            body : text body
            photos (optional) : sequence of bytes representing a photo
        '''
        print len(message)
        import json
        content = json.loads(message)
        print "content" + str(content)

        phone_num = int(content["id"])
        text = content["body"]
        if content.has_key("photo") :
            photo = content["photo"]
            out = open("bytes.jpg", "wb")
            out.write(photo)
            out.flush()
            out.close()
        else :
            photo = 0

        print phone_num
        print text
        print nf.new_status_update(db_handle, phone_num, text, photo)

    elif opcode == "pollnews" :
        # pollnews:userid (phone number)
        phonenum = int(message)
        json = nf.get_user_timeline(db_handle, phonenum)
        print "sent poll" + str(json)
        server_handle.message(json)

        '''
    elif opcode == "newstatus" :
        userid, content = first_split(message, "#")
        # delegate to newsfeed lib.
        nf.new_status_update(self.db_handle, user_id, content)
        '''

    elif opcode == "addfriend" :
        id1,id2 = [int(x) for x in message.split('#')]
        # 0 for success 1 for fail
        success = add_friend(db_handle, id1, id2)
        server_handle.message(str(success))

    elif opcode == "getfriends" :
        id = int(message)
        friends_list = getfriends(id)
        


    elif opcode == "putprofimg" :
        '''
            op:phonenumber#<bytes of img>
        '''
        key, bytestring = first_split(message, "#")
        phonenum = int(key)
        bytestring = base64.b64decode(bytestring)
        # Write to database.
        
    elif opcode == "getprofimg" :
        '''
            op:phonenumber
        '''
        phonenum = int(message)
        # todo delete 

    elif opcode == "log" :
        phone_num, password = split_login_string(message)
        print phone_num
        print password

        # validate input from phone 
        if protoc_validate_login(phone_num, password) == False  :
            # invalid input
            print "zero1"
            server_handle.message("0")
            return

        phone_num = sanitize_phone_number(phone_num)

        success = login_authenticate(db_handle, phone_num, password)
        if success :
            server_handle.message("1")
        else :
            print "zero2"
            server_handle.message("0")

    elif opcode == "reg" :
        phone_num, password = split_login_string(message)

        # validate input from phone 
        if protoc_validate_login(phone_num, password) == False  :
            # invalid input
            print "error 2"
            server_handle.message("0")
            return

        phone_num = sanitize_phone_number(phone_num)
        if check_phonenumber_taken(db_handle, phone_num) == True :
            print "error 1"
            server_handle.message_phone_number_exists()
            return

        insert_user_into_database(db_handle, phone_num, password)
        server_handle.message("1")

    elif opcode == "msg":
        msg = server_handle.name + ": " + message
    '''
    for c in self.factory.clients:
        c.message(msg)
    '''
