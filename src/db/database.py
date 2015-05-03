from cassandra.cluster import Cluster
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from tools.util import unix_time_millis
import datetime, uuid

'''
    Database interfaces for functionality
''' 

def init_session(keyspace="social") :
    ''' 
    Start a connection into cassandra database
    '''
    cluster = Cluster()
    handle = cluster.connect("social")
    return handle

def db_core_get_subscribers(handle, userid) :
    query = "SELECT * FROM friend WHERE userid1=" + str(userid) + ";"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared)
    initial_list = set([list(r)[1] for r in rows])#.difference(set([userid]))
    #initial_list.add(userid)
    query = "SELECT * FROM blacklist WHERE userid1=" + str(userid) + ";"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared)
    blacklisted = set([list(r)[1] for r in rows])

    return initial_list - blacklisted

# EVENTS
def insert_event_into_database(handle, event_id, title, 
                                loc, begin_time, creator_id) :
    prepared = handle.prepare("""
        INSERT INTO events (event_id, title, location, begin_time, attending_userids)
        VALUES (?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [event_id, title, loc, begin_time, [creator_id]])

def event_add_attendee(handle, event_id, user_id) :
    query = "UPDATE events SET attending_userids = attending_userids + {" + str(user_id) + "} WHERE event_id = " + str(event_id) + ';'
    prepared = handle.prepare(query)
    handle.execute(prepared)

def add_new_visible_event_to_user(handle, user_id, event_id) :
    retrieved_id, attendees, start_time, location, title = get_event_details(handle, event_id)

    prepared = handle.prepare("""
        INSERT INTO visible_events (user_id, event_id, start_time, location, title)
        VALUES (?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [user_id, event_id, start_time, location, title])

# Get the row of information from the database.
def get_event_details(handle, event_id) :
    prepared = """
        SELECT * FROM EVENTS WHERE event_id = """ + str(event_id) + ";"
    rows = handle.execute(prepared)
    if len(rows) == 0 :
        return None
    event = rows[0]
    ## ID, Attendees, begin time, location, title
    return (event[0], event[1], event[2], event[3], event[4])


# wrapper to provide seneible interface

def reject_event_invitation(handle, user_id, event_id) :
    prepared = handle.prepare("""
        DELETE FROM visible_events  
        WHERE event_id = """ + str(event_id) + """ AND user_id = """ + str(user_id) + ";")
    handle.execute(prepared)

# extracts the details of the event referred to by event_id and passes to continutation
def accept_event_invitation(handle, user_id, event_id) :
    retrieved_id, attendees, start_time, location, title = get_event_details(handle, event_id)
    if retrieved_id != event_id :
        return

    prepared = handle.prepare("""
        INSERT INTO accepted_events (user_id, event_id, start_time, location, title)
        VALUES (?, ?, ?, ?, ?)
        """)
    handle.execute(prepared, [user_id, event_id, start_time,location, title])
    
    # Definitely do not use same code path as event rejection for future-proofing
    prepared = handle.prepare("""
        DELETE FROM visible_events  
        WHERE event_id = """ + str(event_id) + """ AND user_id = """ + str(user_id) + ";")
    handle.execute(prepared)

    event_add_attendee(handle, event_id, user_id)

def get_user_events_invited(handle, userid) :
    query = "SELECT * FROM visible_events WHERE user_id=" + str(userid) + ";"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared)
    # userid, eventid, location, start-time, title
    return [(r[0], r[1], r[2], r[3],r[4]) for r in rows]

def get_user_events_accepted(handle, userid) :
    query = "SELECT * FROM accepted_events WHERE user_id=" + str(userid) + ";"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared)
    #print "accepted rows"
    # build event tuple
    # userid, eventid, location, start-time, title
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]
    
#converted
# NEW USER
def insert_user_into_database(handle, phone_number, password) :
    '''
    Commit a user/password pair into the database
    ''' 
    prepared = handle.prepare("""
        INSERT INTO user (phone_number, password)
        VALUES (?, ?)
        """)
    handle.execute(prepared, [phone_number, password])
    

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

def add_friend(handle, userid1, userid2) :
    '''
        Should this be directed or undirected?

        This function should not check for referential integrity inthe database because the assumption is the user pair are valid users. Otherwise, the client should not be able to view the userid2 profile. userid1 is known to exist because we have authenticated login
    '''
    prepared = handle.prepare("""
        INSERT INTO friend (userid1, userid2)
        VALUES (?, ?)
        """)
    handle.execute(prepared, [userid1, userid2])
    return 0

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
