import time
from apns import APNs, Frame, Payload

#apns = APNs(use_sandbox=True, cert_file='SocialAppCert.pem', key_file='SocialAppKey.pem')

apns = APNs(use_sandbox=True, cert_file='push2.pem', key_file='SocialAppKey2.pem')

# Send a notification
token_hex = '88697bafe4aec5365cbd228d433c42bcd792a42104fc2982186a2bf8e0b1cced'
payload = Payload(alert="Hello World!", sound="default", badge=1)
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
