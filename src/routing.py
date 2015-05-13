from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor
from db.database import *
from tools.util import split_opcode, event_print_helper

import uuid

import events.events as ev

def perform_routing(server_handle, db_handle, data) :
    import json
    print "data: " + data
    opcode, message = split_opcode(data)
    print "opcode " + str(opcode) + " msg: " + str(message)

    if opcode == "reg" :
        # INPUT : gender phone-num nickname password-hash
        #phone_num, password = split_login_string(message)
        dat = json.loads(str(message))
        if dat.has_key("gender") and dat.has_key("phone_num") \
            and dat.has_key("nick") and dat.has_key("pass_hash") :
                phone_num = dat["phone_num"]
                password = dat["pass_hash"]
                if dat["gender"] == "M" :
                    gender = True
                else :
                    gender = False
                nick = dat["nick"]

                #phone_num = sanitize_phone_number(phone_num)
                '''
                if check_phonenumber_taken(db_handle, phone_num) == True :
                    server_handle.message(str(0))
                    return
                '''
                insert_user_into_database(db_handle,
                                          phone_num,
                                          gender,
                                          nick,
                                          password)
                server_handle.message("1")
        else :
            server_handle.message("0")

    elif opcode == "log" :
        dat = json.loads(str(message))
        if dat.has_key("phone") and dat.has_key("pass") :
            phone_num = dat["phone"]
            password = dat["pass"]

            # phone_num = sanitize_phone_number(phone_num) frontend strip out non-numeric?
            success = login_authenticate(db_handle, phone_num, password)
            if success :
                server_handle.message("1")
            else :
                server_handle.message("0")
        else :
            server_handle.message("0")

    elif opcode == "profile" :
        dat = json.loads(str(message))
        if dat.has_key("phone") :
            phone_num = int(dat["phone"])

            if dat.has_key("intro") :
                user_profile_update_intro(db_handle, phone_num, dat["intro"])

            if dat.has_key("email") :
                user_profile_update_email(db_handle, phone_num, dat["email"])

            if dat.has_key("location") :
                user_profile_update_location(db_handle, phone_num, dat["location"])

            if dat.has_key("nick") :
                user_profile_update_nick(db_handle, phone_num, dat["nick"])

            if dat.has_key("pass") :
                user_profile_update_password(db_handle, phone_num, dat["pass"])

            server_handle.message("1")
        else :
            server_handle.message("0")

    elif opcode == "addfriend" :
        dat = json.loads(str(message))
        if not (dat.has_key("src_user") and dat.has_key("dst_user")) :
            return 1
        id1 = dat["src_user"]
        id2 = dat["dst_user"]
        if dat.has_key("msg") :
            message = dat["msg"]
        else :
            message = "I want to be your friend ;)"

        # 0 for success 1 for fail
        success = add_friend_request(db_handle, id1, id2, message)

    elif opcode == "getfriendrequests" :
        # rey with list of nickname, phone_num, message
        user_id = int(message)
        ret_msg = dict()
        ret_msg["requests"] = []
        request_list = get_pending_friend_request(handle, user_id)
        for phone_num, msg, nickname in request_list :
            req = dict()
            req["phone"] = phone_num
            req["nick"] = nickname
            req["msg"] = msg
            ret_msg["requests"].append(req)
        json_msg = json.dumps(ret_msg, separators=(',',':'))
        server_handle.message(json_msg)

    elif opcode == "acceptfriend" :
        id1, id2 = message.split("#")
        db_accept_friend_request(handle, int(id1), int(id2))
        server_handle.message(1)

    elif opcode == "getfriends" :
        id = int(message)
        print id
        friends_set = db_core_get_subscribers(handle, id)
        print "friends: " + str(friends_set)
        d = dict()
        d["friends"] = list(friends_set)
        response = json.dumps(d, separators=(',',':'))
        server_handle.message(response)

    elif opcode == "eventreject" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") and dat.has_key("event_id") :
            ev.event_reject(handle, dat["user_id"], dat["event_id"])
            server_handle.message(str(1))

    elif opcode == "eventaccept" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") and dat.has_key("event_id") :
            ev.event_accept(handle, dat["user_id"], dat["event_id"])
            server_handle.message(str(1))

    elif opcode == "eventinvite" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") and dat.has_key("event_id") :
            ev.event_invite(handle, dat["user_id"], dat["event_id"])
            server_handle.message(str(1))

    elif opcode == "newevent" :
        dat = json.loads(str(message))
        # Check if json fields are present :
        # host_id, location, time, title
        if dat.has_key("host_id") and dat.has_key("location") and \
            dat.has_key("title") and dat.has_key("time") and \
            dat.has_key("event_id") and dat.has_key("public") :
                host = dat["host_id"]
                location = dat["location"]
                title = dat["title"]
                time = int(dat["time"])
                event_uuid = uuid.UUID(dat["event_id"])
                public_visible = dat["public"]

                if dat.has_key("invite_list") :
                    invite_list = dat["invite_list"]
                    event_id = ev.create_event(db_handle, host, location, 
                                           title, time, event_uuid, public_visible,
                                           invite_list)
                else :
                    event_id = ev.create_event(db_handle, host, location, 
                                            title, time, event_uuid, public_visible)
                server_handle.message(str(event_id))

    elif opcode == "pollinvited" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") :
            user_id = dat["user_id"]
            if dat.has_key("start_offset") and dat.has_key("amount") :
                print dat["amount"]
                print dat["start_offset"]
                events = ev.poll_invited_events(handle, user_id,
                                        dat["start_offset"], 
                                        dat["amount"])
            else :
                events =  ev.poll_invited_events(handle, user_id)
            print events
            server_handle.message(event_print_helper(db_handle, events))

    elif opcode == "pollaccepted" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") :
            user_id = dat["user_id"]
            if dat.has_key("start_offset") and dat.has_key("amount") :
                events = ev.poll_accepted_events(handle, user_id,
                                        dat["start_offset"], 
                                        dat["amount"])
            else :
                events = ev.poll_accepted_events(handle, user_id)
            server_handle.message(event_print_helper(db_handle, events))

    elif opcode == "newstatus" :
        '''
            Assumes message is json-formatted with the following fields :
            id : phone number identifying the user who posted the status
            body : text body
            photos (optional) : sequence of bytes representing a photo
        '''
        import json
        content = json.loads(str(message))

        phone_num = int(content["id"])
        text = content["body"]
        if content.has_key("photo") :
            photo = content["photo"]
        else :
            photo = 0

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

    elif opcode == "msg":
        msg = server_handle.name + ": " + message
    '''
    for c in self.factory.clients:
        c.message(msg)
    '''
if __name__ == "__main__" :
    class ServerStub:
        def message(self, string):
            print("server says:" + str(string))

    def TimestampMillisec64():
        return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)

    from db.database import * 
    import uuid as uuid_
    import json as json_
    handle = init_session()
    server = ServerStub()

    # create new event json message
    '''
    msg = dict()
    msg["time"] = TimestampMillisec64()
    msg["location"] = "loc"
    msg["title"] = "title"
    msg["host_id"] = 6505758649
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "newevent:"+json_msg)
    msg = dict()
    msg["user_id"] = 6505758648
    msg["offset"] = 0
    msg["amount"] = 20
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "pollinvited:" + json_msg)
    '''

    id1 = 12341234
    id2 = 68958695

    msg = dict()
    msg["gender"] = "M"
    msg["phone_num"] = id1
    msg["nick"] = "ZhouYi2"
    msg["pass_hash"] = "password"
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "reg:"+json_msg)

    msg = dict()
    msg["gender"] = "M"
    msg["phone_num"] = id2
    msg["nick"] = "ZhouYi2"
    msg["pass_hash"] = "password"
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "reg:"+json_msg)

    msg = dict()
    msg["phone"] = id1
    msg["pass"] = "password"
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "log:"+json_msg)

    '''
    msg = "getfriends:6505758649"
    print msg
    perform_routing(server, handle, msg)
    '''

    '''
    # profile update
    msg = dict()
    msg["phone"] = 6505758649
    msg["intro"] = "password"
    msg["email"] = "myemail"
    msg["location"] = "mylocation"
    msg["nick"] = "mynickname"
    msg["pass"] = "mypassword"
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "profile:"+json_msg)
    '''
    

    '''
    # test events
    event_id = str(uuid.uuid1())
    msg=dict()
    msg["event_id"] = event_id
    msg["location"] = "test event location4"
    msg["host_id"] = 123456789
    msg["title"] = "my event title4"
    msg["time"] = TimestampMillisec64()
    msg["invite_list"] = [6505758649]
    msg["public"] = False
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "newevent:"+json_msg)
    
    msg=dict()
    msg["event_id"] = event_id
    msg["user_id"] = 6505758649
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "eventaccept:"+json_msg)
    '''

    msg = dict()
    msg["user_id"] = 6505758650
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollinvited:"+json_msg)

    msg = dict()
    msg["user_id"] = 6505758650
    msg["start_offset"] = 10
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollinvited:"+json_msg)

    msg = dict()
    msg["user_id"] = 6505758649
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollaccepted:"+json_msg)

    '''
    # testing friends
    msg = dict()
    msg["src_user"] = id1
    msg["dst_user"] = id2
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "addfriend:"+json_msg)
    perform_routing(server, handle, "getfriendrequests:"+str(id2))
    perform_routing(server, handle, "acceptfriend:"+str(id2)+"#"+str(id1))
    perform_routing(server, handle, "getfriends:"+str(id2))
    '''
