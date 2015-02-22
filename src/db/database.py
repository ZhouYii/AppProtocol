from cassandra.cluster import Cluster

def init_session(keyspace="social") :
    ''' 
    Start a connection into cassandra database
    '''
    cluster = Cluster()
    handle = cluster.connect("social")
    return handle


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



if __name__ == "__main__" :
    handle = init_session()
    insert_user_into_database(handle, 6505758648, "password")
    insert_user_into_database(handle, 6505758649, "password")
    add_friend(handle, 6505758649, 6505758648)
    
