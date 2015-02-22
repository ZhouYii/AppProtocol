from constants import *

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
    if len(phone_num_str) == 0 or len(password_str) == 0 :
        print "Input Invalid"
        return False
    return True

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
