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
    cluster = Cluster(["ec2-52-69-23-190.ap-northeast-1.compute.amazonaws.com"])
    #cluster = Cluster()
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
    print rows
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
        create_friend_accept_notification(handle, accepting_user, requesting_user)
        return 1
    return 0

def add_accept_event_notification(handle, host_id, 
                                      invited_user,
                                      event_title,
                                      seen = False) :
    prepared = handle.prepare("""
        INSERT INTO accepted_event_invitation
            (host_userid, attending_userid, event_title, seen)
        VALUES (?, ?, ?, ?)""")
    handle.execute(prepared, [host_id, invited_user, event_title,  seen])

def create_friend_accept_notification(handle, accepting_user, 
                                      request_creation_user,
                                      seen = False) :
    prepared = handle.prepare("""
        INSERT INTO accepted_friend_requests
            (request_creation_user, accepting_user, seen)
        VALUES (?, ?, ?)""")
    handle.execute(prepared, [request_creation_user, accepting_user, seen])

def get_unseen_event_invite_notification(handle, event_host) :
    prepared = handle.prepare("""
        select attending_userid, event_title, host_userid from accepted_event_invitation 
        where seen=False and host_userid = ?;""")
    rows = handle.execute(prepared, [event_host])
    notifications = [(list(r)[0], list(r)[1]) for r in rows]
    triples = [(list(r)[0], list(r)[1], list(r)[2]) for r in rows]

    # Mark as seen
    for attending_id, title, host_id in triples :
        add_accept_event_notification(handle, host_id, attending_id, title, seen=True)
    return notifications


def get_unseen_friend_accept_notification(handle, request_creation_user) :
    prepared = handle.prepare("""
        select accepting_user from accepted_friend_requests 
        where seen=False and request_creation_user = ?;""")
    rows = handle.execute(prepared, [request_creation_user])
    new_friends = [list(r)[0] for r in rows]

    # Mark as seen
    for friend_id in new_friends :
        create_friend_accept_notification(handle, friend_id, 
                                          request_creation_user, True)
    return new_friends

# Get the friends which are first and second degree away
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
    initial_list = set([list(r)[1] for r in rows])#.difference(set([userid]))
    return initial_list

# EVENTS

# Creates a public event which displays on friend-of-friend newsfeed
# Current policy is to directly update newsfeeds
def insert_event_into_database_public(handle, event_id, title, 
                                loc, begin_time, end_time, creator_id, desc) :
    prepared = handle.prepare("""
        INSERT INTO social.invitation_events (event_id, title, location, \
            begin_time, attending_userids, public, description)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, \
            [event_id, title, loc, begin_time, [creator_id], is_public, desc])

def insert_event_into_database(handle, event_id, title, 
                                loc, begin_time, creator_id, is_public, desc) :
    prepared = handle.prepare("""
        INSERT INTO social.invitation_events (event_id, title, location, begin_time, \
            attending_userids, public, description, host_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, \
            [event_id, title, loc, begin_time, [creator_id], is_public, desc, creator_id])

def event_add_attendee(handle, event_id, user_id) :
    prepared = handle.prepare("""UPDATE social.invitation_events \
        SET attending_userids = attending_userids + ? WHERE event_id = ?""")
    handle.execute(prepared, [set([user_id]), uuid.UUID(event_id)])

def add_newsfeed_event_to_user(handle, user_id, host_id, location, title,
        start_time, end_time, event_id, desc) :
    prepared = handle.prepare("""
        INSERT INTO social.newsfeed (user_id, event_id, begin_time, description,
        end_time, host_id, location, title)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [user_id, event_id, start_time, desc, end_time, \
            host_id,location, title])

def add_new_visible_event_to_user(handle, user_id, event_id, desc) :
    retrieved_id, attendees, start_time, desc, location, is_public, title, host_id = \
            get_event_details(handle, event_id)

    prepared = handle.prepare("""
        INSERT INTO visible_events (user_id, event_id, start_time, location, title, description)
        VALUES (?, ?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [user_id, event_id, start_time, location, title, desc])

# Get the row of information from the database.
def get_event_details(handle, event_id) :
    prepared = """
        SELECT event_id, attending_userids, begin_time, description, location, public, title, host_id \
        FROM social.invitation_events WHERE event_id = """ + str(event_id) + ";"
    rows = handle.execute(prepared)
    if len(rows) == 0 :
        return None
    event = rows[0]
    ## ID, Attendees, begin time, location, title
    # UUID, Attendees, TIME, DESC, LOC, PUBLIC, TITLE
    return (event[0], event[1], event[2], event[3], event[4], event[5], event[6], event[7])


# wrapper to provide seneible interface

def reject_event_invitation(handle, user_id, event_id) :
    prepared = handle.prepare("""
        DELETE FROM visible_events  
        WHERE event_id = """ + str(event_id) + """ AND user_id = """ + str(user_id) + ";")
    handle.execute(prepared)

def event_get_attendees(handle, event_id) :
    prepared = handle.prepare("""
        select attending_userids from social.invitation_events where event_id = ?;
        """)
    rows = handle.execute(prepared, [event_id])
    if len(rows) == 0 :
        return []
    else :
        return list(rows[0][0])

# extracts the details of the event referred to by event_id and passes to continutation
def accept_event_invitation(handle, user_id, event_id, desc) :
    retrieved_id, attendees, start_time, desc, location, is_public, title, host_id = \
            get_event_details(handle, event_id)
    if str(retrieved_id) != str(event_id) :
        return

    prepared = handle.prepare("""
        INSERT INTO social.accepted_events (user_id, event_id, location, start_time,  title, description)
        VALUES (?, ?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [user_id, uuid.UUID(event_id), location ,start_time, title, desc])
    
    # Definitely do not use same code path as event rejection for future-proofing
    prepared = handle.prepare("""
        DELETE FROM visible_events
        WHERE event_id = ? AND user_id = ?;""")
    handle.execute(prepared, [uuid.UUID(event_id), user_id])

    event_add_attendee(handle, event_id, user_id)

def get_user_newsfeed(handle, userid) :
    query = """SELECT event_id, begin_time, description, end_time, 
        host_id, location, title
    FROM social.newsfeed WHERE user_id = ?;"""
    prepared = handle.prepare(query)
    rows = handle.execute(prepared, [userid])
    # userid, eventid, location, start-time, title
    d_list = []
    for event_id, begin_time, description, end_time, host_id, \
            location, title in rows :
        d = dict()
        d["event_id"] = str(event_id)
        d["begin_time"] = begin_time
        d["description"] = description
        d["end_time"] = end_time
        d["host_id"] = host_id
        d["location"] = location
        d["title"] = title
        d_list.append(d)
    return d_list

def get_user_events_invited(handle, userid) :
    query = """SELECT user_id, event_id, description, location, start_time, title 
    FROM visible_events WHERE user_id = ?;"""
    prepared = handle.prepare(query)
    rows = handle.execute(prepared, [userid])
    # userid, eventid, location, start-time, title
    return [(r[0], str(r[1]), r[2], r[3], r[4], r[5]) for r in rows]

def get_user_events_accepted(handle, userid) :
    query = """SELECT user_id, event_id, description, location, start_time, title 
    FROM accepted_events WHERE user_id = ?;"""
    prepared = handle.prepare(query)
    rows = handle.execute(prepared, [userid])
    # build event tuple
    # userid, eventid, location, start-time, title
    return [(r[0], str(r[1]), r[2], r[3], r[4], r[5]) for r in rows]

'''
Add a new user
phone_number -> user id phone number
gender -> true if male, false is female
nickname -> string, for nickname
password -> a hash
'''
def insert_user_into_database(handle, phone_number, gender, nickname, password, parseID) :
    '''
    Commit a user/password pair into the database
    ''' 
    prepared = handle.prepare("""
        INSERT INTO social.user (phone_number, is_male, nickname, password, profileParseID)
        VALUES (?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [int(phone_number), bool(gender), nickname, str(password), str(parseID)])

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
    if len(row) == 0 :
        return None
    return str(list(row)[0][0])

def add_friend_request(handle, requesting_user, desired_friend, message) :
    '''
        Should this be directed or undirected?

        This function should not check for referential integrity inthe database because the assumption is the user pair are valid users. Otherwise, the client should not be able to view the userid2 profile. userid1 is known to exist because we have authenticated login
    '''
    nick = db_get_user_nickname(handle, requesting_user)

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

def get_user_information(handle, phone_num) :
    prepared = """
        SELECT email, introduction, is_male, location, nickname, profileParseID \
        FROM social.user WHERE phone_number  = """ + str(phone_num) + ";"
    rows = handle.execute(prepared)
    if len(rows) == 0 :
        return dict()
    event = rows[0]
    ## ID, Attendees, begin time, location, title
    # UUID, Attendees, TIME, DESC, LOC, PUBLIC, TITLE
    ret = dict()
    ret["email"] = event[0]
    ret["introduction"] = event[1]
    ret["is_male"] = event[2]
    ret["location"] = event[3]
    ret["nickname"] = event[4]
    ret["parseID"] = event[5]
    return ret

def TimestampMillisec64():
    return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)

if __name__ == "__main__" :
    from uuid import uuid4
    handle = init_session()
    #import newsfeed.newsfeed as nf
    uuid_str = uuid.uuid1()
    time_start = TimestampMillisec64()

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
