from cassandra.cluster import Cluster
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from tools.util import unix_time_millis
import datetime

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
    initial_list = set([list(r)[1] for r in rows])
    initial_list.add(userid)

    query = "SELECT * FROM blacklist WHERE userid1=" + str(userid) + ";"
    prepared = handle.prepare(query)
    rows = handle.execute(prepared)
    blacklisted = set([list(r)[1] for r in rows])

    return initial_list - blacklisted
    

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

def db_newsfeed_new_post(handle, time, userid, body, photo=None) :
    if photo == None :
        photo = 0
    prepared = handle.prepare("""
        INSERT INTO newsfeed_postsv2 (post_id, author_id, body, photo) 
        VALUES (?, ?, ?, ?)
        """)
    handle.execute(prepared, [time, userid, body, photo])


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
    handle.execute(prepared, [owner_id, post_uuid, author_id, body, photo])

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

if __name__ == "__main__" :
    from uuid import uuid4
    handle = init_session()
    import newsfeed.newsfeed as nf

    insert_user_into_database(handle, 2174180160, "abc")
    '''
    insert_user_into_database(handle, 6505758649, "password")
    add_friend(handle, 6505758649, 6505758648)
    '''
    '''
    time = unix_time_millis(datetime.datetime.now())
    print time
    db_status_insert(time, 6505758649, "new hello")
    db_timeline_insert(6505758649, time, 6505758649, "new hello")
    '''
    '''
    #print db_newsfeed_get_user_newsfeed(6505758649)
    nf.new_status_update(handle, 6505758649, "full ver2")
    print db_newsfeed_get_user_newsfeed(handle, 123456)
    '''
    '''
    #image uuid
    img_id = uuid4()
    jpeg = open("img.jpg", "rb")
    jpeg_bytes = jpeg.read()
    #out = open("tmp/out1.jpg", "wb")
    #out.write(jpeg_bytes)
    #nf.new_status_update(handle, 6505758649, "new  body1", jpeg_bytes)
    #nf.new_status_update(handle, 6505758649, "new body2")
    x = db_newsfeed_get_user_newsfeed(handle, 6505758649)
    img_index = 0
    for entry in x :
        if nf.cql_photo(entry) == None :
            print entry
        else :
            out = open("tmp/out"+str(img_index)+".jpg", "wb")
            out.write(nf.cql_photo(entry))
            out.close()
            img_index += 1
    '''
    import json
    import base64
    json_string = nf.get_user_timeline(handle, 6505758649)
    test_jpg = None
    json_obj = json.loads(json_string)
    for i in json_obj["items"] :
        post_obj = json.loads(i)
        if post_obj.has_key("photo") :
            test_jpg = post_obj["photo"]
            break
    out = open("tmp/jsondecode.jpg", "wb")
    out.write(base64.b64decode(test_jpg))
    out.close()
