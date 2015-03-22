from routing import perform_routing
from db.database import *
import json

DB_HANDLE = init_session()

# New status
jsond = dict()
jsond["id"] = 6505758649
jsond["body"] = "new_status_contents"
json_str = json.dumps(jsond)
request_string = "newstatus:" + json_str
perform_routing(None, DB_HANDLE, request_string)
