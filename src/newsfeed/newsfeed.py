from uuid import uuid4
import datetime
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import db.database as db
from tools.util import unix_time_millis


def new_status_update(handle, userid, body) :
    '''
        User is assumed to be logged in, which implies existence of userid in database
    '''
    # generate post id and insert to database
    time = unix_time_millis(datetime.datetime.now())
    db.db_newsfeed_new_post(handle, time, userid, body)
    
    # get subscribers and update timelines. This can be async+deferred
    to_update = db.db_core_get_subscribers(handle, userid)
    for dst_id in to_update :
        db.db_newsfeed_timeline_insert(handle, dst_id, time, userid, body)

