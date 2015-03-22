from uuid import uuid4
import json
import base64
import datetime
from os import sys, path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import db.database as db
from tools.util import unix_time_millis, to_json

def create_new_album(handle, master_id) :
    '''
        Consistency invariant for caching : each album has one owner with 
        master copy which is always in sync with server. Slave copies mirror
        the server (transitive mirror for master).
    '''
    uuid = db.db_allocate_new_album(handle, master_id)
    '''
    Postprocess
    '''
    return uuid

def create_profile_album(handle, master_id) :
    uuid = db.db_allocate_new_album(handle, master_id)
    db.set_profile_album(handle, master_id, uuid)
    db.set_insert_profile_picture(handle, uuid)
    return uuid
