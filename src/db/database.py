from cassandra.cluster import Cluster
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import datetime, uuid

'''
    Database interfaces for functionality
''' 

def init_session(keyspace="social") :
    ''' 
    Start a connection into cassandra database
    '''
    cluster = Cluster(["ec2-54-69-204-42.us-west-2.compute.amazonaws.com"])
    handle = cluster.connect("social")
    return handle

def get_pending_friend_request(handle, userid) :
    prepared = handle.prepare("""
        SELECT * from pendingfriend WHERE pending_user = ?;
        """)
    rows = handle.execute(prepared, [userid])
    potential_friends = list(rows)

    # reverse for chronological order
    return list(reversed([list(r)[1:4] for r in potential_friends]))

'''
    requesting_user - user who issued the friend request
    accepting_user - user who recieved the friend request
'''
def db_accept_friend_request(handle, accepting_user, requesting_user) :
    prepared = handle.prepare("""
         SELECT * from pendingfriend WHERE pending_user = ? AND requesting_user = ?;
        """)
    rows = handle.execute(prepared, [accepting_user, requesting_user])

    # check if request exists
    if len(rows) > 0 :
        # remove request
        prepared = handle.prepare("""
            DELETE FROM pendingfriend WHERE pending_user = ? AND requesting_user = ?;
            """)
        handle.execute(prepared, [accepting_user, requesting_user])

        # add friend
        prepared = handle.prepare("""
            INSERT INTO friend (userid1, userid2)
            VALUES (?, ?)
            """)
        handle.execute(prepared, [requesting_user, accepting_user])
        handle.execute(prepared, [accepting_user, requesting_user])
        return 1
    return 0

def db_second_deg_friends(handle, userid) :
    first_deg = db_get_friends(handle, userid)
    second_deg = set(first_deg)
    for user in first_deg :
        new_friends = set(db_get_friends(handle, user))
        second_deg = second_deg.union(new_friends)
    second_deg.discard(userid)
    return second_deg

def db_get_friends(handle, userid) :
    query = "SELECT * FROM friend WHERE userid1 = ?;"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared, [userid])
    initial_list = set([list(r)[1] for r in rows])#.difference(set([userid]))
    return initial_list

def db_core_get_subscribers(handle, userid) :
    query = "SELECT * FROM friend WHERE userid1 = ?;"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared, [userid])
    print rows
    initial_list = set([list(r)[1] for r in rows])#.difference(set([userid]))
    return initial_list

# EVENTS
def insert_event_into_database(handle, event_id, title, 
                                loc, begin_time, creator_id, is_public) :
    prepared = handle.prepare("""
        INSERT INTO events (event_id, title, location, begin_time, attending_userids, public)
        VALUES (?, ?, ?, ?, ?, ?)
        """)
    '''
    print "insert event"
    print [int(creator_id)]
    print type([int(creator_id)])
    '''
    handle.execute(prepared, [event_id, title, loc, begin_time, [int(creator_id)], is_public])

def event_add_attendee(handle, event_id, user_id) :
    prepared = handle.prepare("""UPDATE events SET attending_userids = attending_userids + ? WHERE event_id = ?""")
    handle.execute(prepared, [set([user_id]), uuid.UUID(event_id)])

def add_new_visible_event_to_user(handle, user_id, event_id) :
    get_event_details(handle, event_id)
    retrieved_id, attendees, start_time, location, title = get_event_details(handle, event_id)

    print "aaa"
    print user_id
    print get_event_details(handle, event_id)

    prepared = handle.prepare("""
        INSERT INTO visible_events (user_id, event_id, start_time, location, title)
        VALUES (?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [user_id, event_id, start_time, location, title])

# Get the row of information from the database.
def get_event_details(handle, event_id) :
    prepared = """
        SELECT * FROM social.events WHERE event_id = """ + str(event_id) + ";"
    rows = handle.execute(prepared)
    if len(rows) == 0 :
        return None
    event = rows[0]
    ## ID, Attendees, begin time, location, title
    return (event[0], event[1], event[2], event[3], event[5])


# wrapper to provide seneible interface

def reject_event_invitation(handle, user_id, event_id) :
    prepared = handle.prepare("""
        DELETE FROM visible_events  
        WHERE event_id = """ + str(event_id) + """ AND user_id = """ + str(user_id) + ";")
    handle.execute(prepared)

def event_get_attendees(handle, event_id) :
    prepared = handle.prepare("""
        select attending_userids from events where event_id = ?;
        """)
    rows = handle.execute(prepared, [event_id])
    if len(rows) == 0 :
        return []
    else :
        return list(rows[0][0])

# extracts the details of the event referred to by event_id and passes to continutation
def accept_event_invitation(handle, user_id, event_id) :
    retrieved_id, attendees, start_time, location, title = get_event_details(handle, event_id)
    if str(retrieved_id) != str(event_id) :
        print "str" + str(event_id)
        print "str" + str(retrieved_id)
        return

    prepared = handle.prepare("""
        INSERT INTO social.accepted_events (user_id, event_id, location, start_time,  title)
        VALUES (?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [user_id, uuid.UUID(event_id), location ,start_time, title])
    
    # Definitely do not use same code path as event rejection for future-proofing
    prepared = handle.prepare("""
        DELETE FROM visible_events
        WHERE event_id = ? AND user_id = ?;""")
    handle.execute(prepared, [uuid.UUID(event_id), user_id])
    print prepared

    event_add_attendee(handle, event_id, user_id)

def get_user_events_invited(handle, userid) :
    query = "SELECT * FROM visible_events WHERE user_id = ?;"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared, [userid])
    # userid, eventid, location, start-time, title
    print "db layer"
    print rows
    return [(r[0], r[1], r[2], r[3],r[4]) for r in rows]

def get_user_events_accepted(handle, userid) :
    query = "SELECT * FROM accepted_events WHERE user_id = ?;"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared, [userid])
    #print "accepted rows"
    # build event tuple
    # userid, eventid, location, start-time, title
    print "db layer"
    print rows
    [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

'''
Add a new user
phone_number -> user id phone number
gender -> true if male, false is female
nickname -> string, for nickname
password -> a hash
'''
def insert_user_into_database(handle, phone_number, gender, nickname, password) :
    '''
    Commit a user/password pair into the database
    ''' 
    prepared = handle.prepare("""
        INSERT INTO social.user (phone_number, is_male, nickname, password)
        VALUES (?, ?, ?, ?)
        """)
    handle.execute(prepared, [int(phone_number), bool(gender), str(nickname), str(password)])
    print phone_number
    print gender
    print nickname
    print password

def check_phonenumber_taken(handle, phone_number) :
    '''
    Check if the phone number is already registered in the database
    True -> Existing registration exists.
    '''
    prepared = handle.prepare("""
        SELECT * FROM user WHERE 
        phone_number = ?
        """)
    row = handle.execute(prepared, [phone_number])
    if len(row) > 0 :
        print "Phone number taken"
        return True
    return False

def login_authenticate(handle, phone_number, password) :
    '''
    Check if phone_number and password pair are valid
    True -> allow login (implies k-v pair existst in cassandra)
    '''
    prepared = handle.prepare("""
        SELECT * FROM user WHERE 
        phone_number = ? AND password = ?
        """)

    row = handle.execute(prepared, [phone_number, password])
    if len(row) == 1 :
        return True
    return False

def db_get_user_nickname(handle, userid) :
    prepared = handle.prepare( """
        select nickname from user where phone_number = ?;
     """)
    row = handle.execute(prepared, [userid])
    print row
    if len(row) == 0 :
        return None
    return str(list(row)[0][0])

def add_friend_request(handle, requesting_user, desired_friend, message) :
    '''
        Should this be directed or undirected?

        This function should not check for referential integrity inthe database because the assumption is the user pair are valid users. Otherwise, the client should not be able to view the userid2 profile. userid1 is known to exist because we have authenticated login
    '''
    nick = db_get_user_nickname(handle, requesting_user)
    print "nick" + str(nick)

    # this should not happen. backend should get sanitary inputs
    if nick == None :
        return 1

    prepared = handle.prepare("""
        INSERT INTO pendingfriend (pending_user, requesting_user, message, requesting_nick)
        VALUES (?, ?, ?, ?)
        """)
    handle.execute(prepared, [desired_friend, requesting_user, message, nick])
    return 1

def db_newsfeed_new_post(handle, time, userid, body, photo=None) :
    if photo == None :
        photo = 0
    prepared = handle.prepare("""
        INSERT INTO newsfeed_postsv2 (post_id, author_id, body, photo) 
        VALUES (?, ?, ?, ?)
        """)
    return handle.execute(prepared, [time, userid, body, photo])


def db_newsfeed_timeline_insert(handle, owner_id, post_uuid, author_id, body, photo=None) :
    '''
        Ownerid is the user id of the account which owns the timeline
    '''
    if photo == None :
        photo = 0
    prepared = handle.prepare("""
        INSERT INTO newsfeed_timelinev2 (user_id, post_id, author, body, photo) 
        VALUES (?, ?, ?, ?, ?)
        """)
    result = handle.execute(prepared, [owner_id, post_uuid, author_id, body, photo])
    return result

def db_newsfeed_get_user_newsfeed(handle, user_id) :
    query = "SELECT * FROM  newsfeed_timelinev2 WHERE user_id= "
    query += str(user_id)
    query += " ORDER BY post_id DESC;"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared)

    # To standard python collection + truncate userid
    return [ list(r)[1:] for r in rows]

def db_insert_image(handle, key, bytestring) :
    query = "INSERT INTO images (id_key, bytes) VALUES (?, ?)"
    prepared = handle.prepare(query)
    handle.execute(prepared, [key, bytestring])

def user_profile_update_intro(handle, phone_num, intro) :
    query = "UPDATE social.user SET introduction = ? WHERE phone_number = ?"
    prepared = handle.prepare(query)
    handle.execute(prepared, [intro, phone_num])

def user_profile_update_location(handle, phone_num, location) :
    query = "UPDATE social.user SET location= ? WHERE phone_number = ?"
    prepared = handle.prepare(query)
    handle.execute(prepared, [location, phone_num])

def user_profile_update_email(handle, phone_num, email) :
    query = "UPDATE social.user SET email = ? WHERE phone_number = ?"
    prepared = handle.prepare(query)
    handle.execute(prepared, [email, phone_num])

def user_profile_update_nick(handle, phone_num, nick) :
    query = "UPDATE social.user SET nickname = ? WHERE phone_number = ?"
    prepared = handle.prepare(query)
    handle.execute(prepared, [nick, phone_num])

def user_profile_update_password(handle, phone_num, password) :
    query = "UPDATE social.user SET password = ? WHERE phone_number = ?"
    prepared = handle.prepare(query)
    handle.execute(prepared, [password, phone_num])

def TimestampMillisec64():
    return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)

if __name__ == "__main__" :
    from uuid import uuid4
    handle = init_session()
    #import newsfeed.newsfeed as nf
    #print db_newsfeed_get_user_newsfeed(handle, 6505758649)
    print str(TimestampMillisec64())
    uuid_str = uuid.uuid1()
    time_start = TimestampMillisec64()

    print event_get_attendees(handle, uuid.UUID("96a3db0f-f8e8-11e4-9664-b8e85632007e"))

    '''
    db_get_user_nickname(handle, 123456789)

    add_friend_request(handle, 6505758649, 123456789, "my frnd")
    add_friend_request(handle, 6505758650, 123456789, "hello")
    print get_pending_friend_request(handle, 123456789)
    db_accept_friend_request(handle, 123456789, 6505758650)
    '''

    ''''
    insert_event_into_database(handle, uuid_str,
                               "titlenew", "location", 
                               time_start, 6505758649)
    
    accept_event_invitation(handle, 6505758649, uuid_str)
    friends = db_core_get_subscribers(handle, 6505758649)
    print friends
    for friend_id in friends : 
        add_new_visible_event_to_user(handle, friend_id, uuid_str)
    accept_event_invitation(handle, 6505758649, uuid_str)

    print("invited")
    reject_event_invitation(handle, 6505758648, uuid_str)
    print len(get_user_events_invited(handle, 6505758648))
    print len(get_user_events_invited(handle, 6505758649))

    print( "accepted")
    print len(get_user_events_accepted(handle, 6505758648))

    print get_event_details(handle, uuid_str)
    '''
