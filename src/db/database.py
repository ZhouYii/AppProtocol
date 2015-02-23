from cassandra.cluster import Cluster
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from tools.util import unix_time_millis
import datetime

def init_session(keyspace="social") :
    ''' 
    Start a connection into cassandra database
    '''
    cluster = Cluster()
    handle = cluster.connect("social")
    return handle


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

def db_status_insert(time, userid, body) :
    prepared = handle.prepare("""
        INSERT INTO newsfeed_posts (post_id, author_id, body) 
        VALUES (?, ?, ?)
        """)
    handle.execute(prepared, [time, userid, body])

def db_timeline_insert(owner_id, post_uuid, author_id, body) :
    '''
        Ownerid is the user id of the account which owns the timeline
    '''
    prepared = handle.prepare("""
        INSERT INTO newsfeed_timeline (user_id, post_id, author, body) 
        VALUES (?, ?, ?, ?)
        """)
    handle.execute(prepared, [owner_id, post_uuid, author_id, body])

def db_get_newsfeed(user_id) :
    query = "SELECT * FROM  newsfeed_timeline WHERE user_id= "
    query += str(user_id)
    query += " ORDER BY post_id DESC;"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared)

    # To standard python collection + truncate userid
    return [ list(r)[1:] for r in rows]




if __name__ == "__main__" :
    from uuid import uuid4
    handle = init_session()
    '''
    insert_user_into_database(handle, 6505758648, "password")
    insert_user_into_database(handle, 6505758649, "password")
    add_friend(handle, 6505758649, 6505758648)
    '''
    '''
    time = unix_time_millis(datetime.datetime.now())
    print time
    db_status_insert(time, 6505758649, "new hello")
    db_timeline_insert(6505758649, time, 6505758649, "new hello")
    '''
    print db_get_newsfeed(6505758649)
