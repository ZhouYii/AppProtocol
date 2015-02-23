from uuid import uuid4

def new_status_update(userid, body) :
    '''
        User is assumed to be logged in, which implies existence of userid in database
    '''
    post_id = uuid4()
    db_status_update(post_id, userid, body)
    # update timeline

