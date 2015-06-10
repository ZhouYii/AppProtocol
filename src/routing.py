from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor
from db.database import *
from tools.util import split_opcode, event_print_helper, public_event_print_helper

import uuid

import events.events as ev

def perform_routing(server_handle, db_handle, data) :
    import json
    opcode, message = split_opcode(data)
    print "logging encoding start"
    print "opcode " + str(opcode) + " msg: " + str(message)

    if opcode == "reg" :
        # INPUT : gender phone-num nickname password-hash
        #phone_num, password = split_login_string(message)
        dat = json.loads(str(message))
        if dat.has_key("gender") and dat.has_key("phone_num") \
            and dat.has_key("nick") and dat.has_key("pass_hash") \
	    and dat.has_key("parseID") :
                phone_num = dat["phone_num"]
                parse_id = dat["parseID"]
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
                                          password,
                                          parse_id)
                server_handle.message("1")
        else :
            server_handle.message("0")


    elif opcode == "log" :
        dat = json.loads(str(message))
        print dat
        if dat.has_key("phone") and dat.has_key("pass") :
            phone_num = int(dat["phone"])
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
        id1 = int(dat["src_user"])
        id2 = int(dat["dst_user"])
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
        request_list = get_pending_friend_request(db_handle, user_id)
        for phone_num, msg, nickname in request_list :
            req = dict()
            req["phone"] = phone_num
            req["nick"] = nickname
            req["msg"] = msg
            ret_msg["requests"].append(req)
        json_msg = json.dumps(ret_msg, separators=(',',':'))
        server_handle.message(json_msg)

    elif opcode == "seekuser" :
        # rey with list of nickname, phone_num, message
        user_id = int(message)
        ret_msg = get_user_information(db_handle, user_id)
        json_msg = json.dumps(ret_msg, separators=(',',':'))
        server_handle.message(json_msg)

    elif opcode == "acceptfriend" :
        id1, id2 = message.split("#")
        db_accept_friend_request(db_handle, int(id1), int(id2))
        db_accept_friend_request(db_handle, int(id2), int(id1))
        server_handle.message(1)

    elif opcode == "getfriends" :
        id = int(message)
        friends_set = db_core_get_subscribers(db_handle, id)
        d = dict()
        d["friends"] = list(friends_set)
        response = json.dumps(d, separators=(',',':'))
        server_handle.message(response)

    elif opcode == "eventreject" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") and dat.has_key("event_id") :
            ev.event_reject(db_handle, dat["user_id"], dat["event_id"])
            server_handle.message(str(1))

    elif opcode == "eventaccept" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") and dat.has_key("event_id") :
            ev.event_accept(db_handle, dat["user_id"], dat["event_id"])
            server_handle.message(str(1))

    elif opcode == "eventinvite" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") and dat.has_key("event_id") :
            ev.event_invite(db_handle, dat["user_id"], dat["event_id"])
            server_handle.message(str(1))

    elif opcode == "newPublicEvent" :
        dat = json.loads(str(message))
        # Check if json fields are present :
        # host_id, location, start_time, end_time, title
        if dat.has_key("host_id") and dat.has_key("location") and \
            dat.has_key("title") and dat.has_key("start_time") and \
            dat.has_key("description") and \
            dat.has_key("event_id") and dat.has_key("end_time") :
                host = int(dat["host_id"])
                location = dat["location"]
                title = dat["title"]
                start_time = int(dat["start_time"])
                end_time = int(dat["end_time"])
                description = dat["description"]
                event_uuid = uuid.UUID(dat["event_id"])

                event_id = ev.create_event_public(db_handle, host, location,
                                        title, start_time, end_time,
                                        event_uuid, description)
                server_handle.message(str(event_id))

    elif opcode == "newevent" :
        dat = json.loads(str(message))
        # Check if json fields are present :
        # host_id, location, time, title
        if dat.has_key("host_id") and dat.has_key("location") and \
            dat.has_key("title") and dat.has_key("time") and \
            dat.has_key("description") and \
            dat.has_key("event_id") and dat.has_key("public") :
                host = int(dat["host_id"])
                location = dat["location"]
                title = dat["title"]
                time = int(dat["time"])
                description = dat["description"]
                event_uuid = uuid.UUID(dat["event_id"])
                public_visible = bool(dat["public"])

                if dat.has_key("invite_list") :
                    invite_list = dat["invite_list"]
                    invite_list = [int(invited_user) for invited_user in invite_list]
                    event_id = ev.create_event(db_handle, host, location, 
                                           title, time, event_uuid, public_visible, 
                                           description, invite_list)
                else :
                    event_id = ev.create_event(db_handle, host, location,
                                            title, time, event_uuid,
                                            public_visible, description)
                server_handle.message(str(event_id))

    elif opcode == "pollnewsfeed" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") :
            user_id = int(dat["user_id"])
            if dat.has_key("start_offset") and dat.has_key("amount") :
                events = ev.poll_newsfeed_events(db_handle, user_id,
                                            int(dat["start_offset"]), 
                                            int(dat["amount"]))
            else :
                events =  ev.poll_newsfeed_events(db_handle, user_id)
            #new_friends = \
            #        get_unseen_friend_accept_notification(handle, user_id)
            #new_invite_accept = get_unseen_event_invite_notification(db_handle, user_id)
            response = public_event_print_helper(db_handle, events)
            server_handle.message(response)

    elif opcode == "pollinvited" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") :
            user_id = int(dat["user_id"])
            if dat.has_key("start_offset") and dat.has_key("amount") :
                events = ev.poll_invited_events(db_handle, user_id,
                                        int(dat["start_offset"]), 
                                        int(dat["amount"]))
            else :
                events =  ev.poll_invited_events(db_handle, user_id)
            response = public_event_print_helper(db_handle, events)
            server_handle.message(response)

    elif opcode == "friendAcceptNotification" :
        user_id = int(message)
        new_friends = get_unseen_friend_accept_notification(db_handle, user_id)
        d = dict()
        d["notifications"] = new_friends
        json_msg = json.dumps(d,  separators=(',',':'))
        server_handle.message(json_msg)

    elif opcode == "eventAcceptNotification" :
        user_id = int(message)
        new_invite_accept = get_unseen_event_invite_notification(db_handle, user_id)
        d = dict()
        d["notifications"] = new_invite_accept
        json_msg = json.dumps(d,  separators=(',',':'))
        server_handle.message(json_msg)

    elif opcode == "pollaccepted" :
        dat = json.loads(str(message))
        if dat.has_key("user_id") :
            user_id = int(dat["user_id"])
            if dat.has_key("start_offset") and dat.has_key("amount") :
                events = ev.poll_accepted_events(db_handle, user_id,
                                        int(dat["start_offset"]), 
                                        int(dat["amount"]))
            else :
                events = ev.poll_accepted_events(db_handle, user_id)

            response = public_event_print_helper(db_handle, events)
            server_handle.message(response)

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

    id1 = 11111111
    id2 = 22222222

    print "****Verify Registration Opcodes"

    msg = dict()
    msg["gender"] = "M"
    msg["phone_num"] = id1
    msg["nick"] = "ZhouYi2"
    msg["pass_hash"] = "password"
    msg["parseID"] = "mytestparseID"
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "reg:"+json_msg)

    msg = dict()
    msg["gender"] = "M"
    msg["phone_num"] = id2
    msg["nick"] = "ZhouYi2"
    msg["pass_hash"] = "password"
    msg["parseID"] = "mytestparseID"
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "reg:"+json_msg)

    '''
    msg = dict()
    msg["phone"] = id1
    msg["pass"] = "password"
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "log:"+json_msg)
    '''

    print "****"
    print '**** Verify opcodes for login'
    login_name = "11111111"
    login_password = "password"
    msg = dict()
    msg["phone"] = login_name
    msg["pass"] = login_password
    json_msg = json_.dumps(msg, separators=(',',':'))
    try : 
      perform_routing(server, handle, "log:" + json_msg)
    except Exception :
      print "Error Occurred"

    '''
    msg = "getfriends:6505758649"
    print msg
    perform_routing(server, handle, msg)
    '''

    print "****"
    print "**** Profile Update"
    # profile update
    msg = dict()
    msg["phone"] = 6505758649
    msg["intro"] = "password"
    msg["email"] = "myemail"
    msg["location"] = "mylocation-updated"
    msg["nick"] = "mynickname"
    msg["pass"] = "mypassword"
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "profile:"+json_msg)


    print "****"
    print "**** Add Friend Reqeust"
    msg = dict()
    msg["src_user"] = 6505758649
    msg["dst_user"] = id1
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "addfriend:"+json_msg)

    print "****"
    print "**** Get Friend Reqeust"
    perform_routing(server, handle, "getfriendrequests:" + str(id1))

    print "****"
    print "**** Accept Friend Reqeust"
    perform_routing(server, handle, "acceptfriend:" + str(6505758649) + "#" + str(id1))

    print "****"
    print "**** Get Friends"
    perform_routing(server, handle, "getfriends:" + str(6505758649))

    print "****"
    print "**** Poll New Friends"
    msg = dict()
    msg["user_id"] = 6505758649
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "pollnewsfeed:"+json_msg)

    print "****"
    print "**** No More New Friends"
    msg = dict()
    msg["user_id"] = 6505758649
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "pollnewsfeed:"+json_msg)

    # test events
    print "****"
    print "**** Invite Event"
    event_id = str(uuid.uuid1())
    msg=dict()
    msg["event_id"] = event_id
    msg["location"] = "my test event location 2"
    msg["host_id"] = id1
    msg["description"] = "my test event description"
    msg["title"] = "my test event title invited"
    msg["time"] = TimestampMillisec64()
    msg["invite_list"] = [id2]
    msg["public"] = False
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "newevent:"+json_msg)

    print "****"
    print "**** Accepted Event"
    # Comment this block to test event creation. Uncommend to test event accepted was successful
    event_id = str(uuid.uuid1())
    msg=dict()
    msg["event_id"] = event_id
    msg["location"] = "my test event location 1"
    msg["host_id"] = id1
    msg["description"] = "my test event description"
    msg["title"] = "my test event title accepted"
    msg["time"] = TimestampMillisec64()
    msg["invite_list"] = [id2]
    msg["public"] = False
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "newevent:"+json_msg)

    print "****"
    print "**** Polling Invited Events"
    msg = dict()
    msg["user_id"] = id2
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "pollinvited:"+json_msg)

    print "****"
    print "**** Send Accept Commond"
    msg=dict()
    msg["event_id"] = event_id
    msg["user_id"] = id2
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "eventaccept:"+json_msg)


    print "****"
    print "**** Polling Accepted Events"
    msg = dict()
    msg["user_id"] = id2
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "pollaccepted:"+json_msg)

    print "****"
    print "**** Polling Accepted Events Notification"
    msg = dict()
    msg["user_id"] = id1
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "pollaccepted:"+json_msg)

    print "****"
    print "**** Create Newsfeed (friend-of-friend) Event"
    print "**** 6505758649 is now a friend and can see the event"
    event_id = str(uuid.uuid1())
    msg=dict()
    msg["event_id"] = event_id
    msg["location"] = "newsfesafsdfed_event"
    msg["host_id"] = id1
    msg["description"] = "my newsfeed event"
    msg["title"] = "newfeed event"
    msg["start_time"] = TimestampMillisec64()
    msg["end_time"] = TimestampMillisec64() + 10000
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "newPublicEvent:"+json_msg)

    print "****"
    print "**** Poll Newsfeed For 6505758640"
    msg = dict()
    msg["user_id"] = 6505758649
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollnewsfeed:"+json_msg)

    print "****"
    print "**** SeekUser test"
    perform_routing(server, handle, "seekuser:"+str(id1))

    print "****"
    print "**** Event Notification test"
    perform_routing(server, handle, "eventAcceptNotification:"+str(id1))
    perform_routing(server, handle, "eventAcceptNotification:"+str(id2))

    print "****"
    print "**** Friend Notification test"
    perform_routing(server, handle, "friendAcceptNotification:"+str(id1))
    perform_routing(server, handle, "friendAcceptNotification:"+str(id2))
    perform_routing(server, handle, "friendAcceptNotification:"+str(6505758649))

    '''
    msg = dict()
    msg["user_id"] = id1
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollnewsfeed:"+json_msg)

    msg = dict()
    msg["user_id"] = id1
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollnewsfeed:"+json_msg)
    '''

    '''
    msg = dict()
    msg["user_id"] = id2
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollaccepted:"+json_msg)
    '''

    # testing friends
    '''
    msg = dict()
    msg["src_user"] = id1
    msg["dst_user"] = id2
    msg["msg"] = "omg my message"
    json_msg = json_.dumps(msg, separators=(',',':'))
    perform_routing(server, handle, "addfriend:"+json_msg)
    perform_routing(server, handle, "getfriendrequests:"+str(id2))
    perform_routing(server, handle, "acceptfriend:"+str(id2)+"#"+str(id1))
    perform_routing(server, handle, "getfriends:"+str(id2))

    msg = dict()
    msg["user_id"] = id1
    msg["start_offset"] = 0
    msg["amount"] = 10
    json_msg = json_.dumps(msg, separators=(',',':'))
    print "json msg: " + str(json_msg)
    perform_routing(server, handle, "pollinvited:"+json_msg)
    print get_unseen_friend_accept_notification(handle, id1)
    '''
