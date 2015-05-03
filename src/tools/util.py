import json
import datetime
from apns import APNs, Frame, Payload
import time

#
## Monkey Patch for SSL
## Changes PROTOCOL_SSLv23 to PROTOCOL_TLSv1
#
import ssl
from functools import wraps
def sslwrap(func):
    @wraps(func)
    def bar(*args, **kw):
        kw['ssl_version'] = ssl.PROTOCOL_TLSv1
        return func(*args, **kw)
    return bar

ssl.wrap_socket = sslwrap(ssl.wrap_socket)
#
##
#

def split_login_string(string) :
    split_index = string.find(';')
    if split_index == -1 :
        # Corrupted/undefined app client-side behavior
        return ("", "")
    return (string[:split_index], string[split_index + 1:])

def sanitize_phone_number(phone_string) :
    numbers = [x for x in phone_string if x.isdigit()]
    return int("".join(numbers))

def protoc_validate_login(phone_num_str, password_str) :
    '''
        Test various conditions for undefined input behaviors on the login screen.
        The phone number string must be a long as the minimun length of 
        phone number.
    '''
    if len(password_str) == 0 :
        print "Input Invalid"
        return False
    return True

def first_split(string, separator) :
    '''
        Split the string into two substrings, at the first occurrence of the separator character
    '''
    split_idx = string.find(separator)
    opcode = string[:split_idx]
    if split_idx == len(string) - 1 :
        return opcode, ""
    return opcode, string[split_idx+1:]

def split_opcode(string, separator=":") :
    '''
        Splits the string into opcode-message pair. Assumes the separator exists in the string, otherwise the client-side message sending process fucked up.
    '''
    split_idx = string.find(separator)
    opcode = string[:split_idx]
    if split_idx == len(string) - 1 :
        return opcode, ""
    return opcode, string[split_idx+1:]

def first_split(string, separator) :
    return split_opcode(string, separator)

'''
Time Functions take input datetime.datetime.now()
'''
def unix_time(dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return delta.total_seconds()

def unix_time_millis(dt):
    return long(unix_time(dt) * 1000.0)

def to_json(dictionary) :
    return json.dumps(dictionary , separators=(',',':'))


def send_push_notification(message,
                           sound_type = "default",
                           cert_file_path = '/home/ubuntu/pemfiles/cert.pem', 
                           key_file_path = '/home/ubuntu/pemfiles/comb.pem') :

    apns = APNs(use_sandbox=True, 
                cert_file = cert_file_path)
                #key_file=key_file_path)

    # Dummy hex
    token_hex = '7ffdd1583899067942754f9afe2a575aa64f5ab3147834b9250a837f538f3097'
    payload = Payload(alert=message, sound=sound_type, badge=1)
    apns.gateway_server.send_notification(token_hex, payload)
    frame = Frame()
    identifier = 1
    expiry = time.time()+3600
    priority = 10
    frame.add_item(token_hex, payload, identifier, expiry, priority)
    apns.gateway_server.send_notification_multiple(frame)

# for polling event list, a list of event tuples are retrieved frm the database
# # userid, eventid, location, start-time, title
# Creates the json string for server to send
def event_print_helper(event_tuples) :
    def event_to_dict(event_tuple) :
        user_id, event_id, loc, time, title = event_tuple
        d = dict()
        d["event_id"] = str(event_id)
        d["location"] = str(loc)
        d["time"] = tuple([time.year, time.month, time.day, time.hour, time.minute])
        d["title"] = str(title)
        return d
        
    def event_to_string(event_tuple) :
        user_id, event_id, loc, time, title = event_tuple
        s = ""
        s += str(user_id) + "##"
        s += str(event_id) + "##"
        s += str(loc) + "##"
        s += str(time) + "##"
        s += str(title)
        return s

    #event_strings = [ event_to_string(t) for t in event_tuples]
    event_dicts = [event_to_dict(t) for t in event_tuples]
    d = dict()
    d["events"] = event_dicts
    return json.dumps(d, separators=(',',':'))

if __name__ == '__main__' :
    send_push_notification("successful push")
