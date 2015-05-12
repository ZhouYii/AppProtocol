from uuid import uuid4
import json
import base64
import datetime
from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
import db.database as db
from tools.util import unix_time_millis, to_json
import uuid

def create_event(handle, host_id, location, title, time,
                 event_id, is_public, invite_list=[]) :

    # is-public matters?

    db.insert_event_into_database(handle, event_id, title, location, time, host_id, is_public)

    for user_id in invite_list :
        db.add_new_visible_event_to_user(handle, user_id, event_id)

    if is_public :
        second_degree = db.db_second_deg_friends(handle, host_id)
        second_degree = second_degree - set(invite_list)
        for user_id in second_degree :
            db.add_new_visible_event_to_user(handle, user_id, event_id)
    return event_id

def event_invite(handle, invitee, event_id) :
    db.add_new_visible_event_to_user(handle, invitee, event_id)

def event_reject(handle, invitee, event_id) :
    db.reject_event_invitation(handle, invitee, event_id)

def event_accept(handle, invited_user, event_id) :
    db.accept_event_invitation(handle, invited_user, event_id)

    # notify event creator

def poll_invited_events(handle, user_id, start_offset=0, amount=10) :
    users_events = db.get_user_events_invited(handle, user_id)
    return users_events[start_offset:start_offset+amount]

def poll_accepted_events(handle, user_id, start_offset=0, amount=10) :
    users_events = db.get_user_events_accepted(handle, user_id)
    return users_events[start_offset:start_offset+amount]
