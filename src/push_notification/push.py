import time
from apns import APNs, Frame, Payload

#apns = APNs(use_sandbox=True, cert_file='SocialAppCert.pem', key_file='SocialAppKey.pem')
def send_push_notification(msg="Hello", snd="default", badge_type=1, use_sandbox=True) :
    apns = APNs(use_sandbox=True, cert_file='push2.pem', key_file='SocialAppKey2.pem')
    # Send a notification
    token_hex = '88697bafe4aec5365cbd228d433c42bcd792a42104fc2982186a2bf8e0b1cced'
    payload = Payload(alert=msg, sound=snd, badge=badge_type)
    apns.gateway_server.send_notification(token_hex, payload)

# Send multiple notifications in a single transmission
'''
frame = Frame()
identifier = 1
expiry = time.time()+3600
priority = 10
frame.add_item('88697bafe4aec5365cbd228d433c42bcd792a42104fc2982186a2bf8e0b1cced', payload, identifier, expiry, priority)
apns.gateway_server.send_notification_multiple(frame)
'''
