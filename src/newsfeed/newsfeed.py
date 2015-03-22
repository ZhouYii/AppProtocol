from uuid import uuid4
import json
import base64
import datetime
from os import sys, path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import db.database as db
from tools.util import unix_time_millis, to_json


def new_status_update(handle, userid, body, photo=None) :
    '''
        User is assumed to be logged in, which implies existence of userid in database
    '''
    # generate post id and insert to database
    time = unix_time_millis(datetime.datetime.now())
    db.db_newsfeed_new_post(handle, time, userid, body, photo)
    
    # get subscribers and update timelines. This can be async+deferred
    to_update = db.db_core_get_subscribers(handle, userid)
    for dst_id in to_update :
        db.db_newsfeed_timeline_insert(handle, dst_id, time, userid, body, photo)

def get_user_timeline(handle,userid) :
    '''
    fields : photo (base64 encoded string, originally byteseq), 
             author
             post_id
             body
    '''
    posts = db.db_newsfeed_get_user_newsfeed(handle, userid)
    json_obj = dict()
    #json_obj["items"] = map(cql_to_json_single, posts)
    '''
        Avoid using to_json because we do not want to do double encoding
    '''
    json_items =  map(cql_to_json_single, posts)
    json_string = '{ "items" : ['
    for item in json_items :
        json_string += item
    json_string += json_items[len(json_items) - 1]
    json_string += ']}'
    return json_string

# CQL parsing interfaces
def cql_photo(post) :
    if '0' == post[3] :
        return None
    return post[3]

def cql_textbody(post) :
    return post[2]

def cql_author(post) :
    return post[1]

def cql_postid(post) :
    return post[0]

def cql_to_json_single(post) :
    d = dict()
    d["author"] = cql_author(post)
    d["postid"] = cql_postid(post)
    d["body"] = cql_textbody(post)
    if cql_photo(post) != None :
        #d["photo"] = "jpeg"
        d["photo"] = base64.encodestring(cql_photo(post))
    return to_json(d)

