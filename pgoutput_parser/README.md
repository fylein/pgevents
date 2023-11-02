# PostgreSQL Logical Replication Parser Module

This README provides a detailed overview of the PostgreSQL Logical Replication Parser Module. It includes classes to handle and parse INSERT, UPDATE, and DELETE messages from PostgreSQL logical replication streams.

## Table of Contents
1. [BaseMessage Class](#basemessage-class)
2. [DeleteMessage Class](#deletemessage-class)
3. [InsertMessage Class](#insertmessage-class)
4. [UpdateMessage Class](#updatemessage-class)

---

## BaseMessage Class

'''python
# Initialize BaseMessage (usually, you won't need to do this directly)
base_msg = BaseMessage(message=your_raw_message, cursor=your_cursor)

# Sample usage to read an int32 value
value = base_msg.read_int32()
'''

---

## DeleteMessage Class

'''python
# Initialize and decode DeleteMessage
delete_msg = DeleteMessage(message=your_raw_message, cursor=your_cursor)
parsed_message = delete_msg.decode_delete_message()

# Sample usage
if parsed_message['message_type'] == 'D':
    print("This is a delete message.")
'''

---

## InsertMessage Class

'''python
# Initialize and decode InsertMessage
insert_msg = InsertMessage(message=your_raw_message, cursor=your_cursor)
parsed_message = insert_msg.decode_insert_message()

# Sample usage
if parsed_message['message_type'] == 'I':
    print("This is an insert message.")
'''

---

## UpdateMessage Class

'''python
# Initialize and decode UpdateMessage
update_msg = UpdateMessage(message=your_raw_message, cursor=your_cursor)
parsed_message = update_msg.decode_update_message()

# Sample usage
if parsed_message['message_type'] == 'U':
    print("This is an update message.")
    print("Difference between old and new values:", parsed_message['diff'])
'''
