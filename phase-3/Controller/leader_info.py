import json
import socket
import traceback
import time
import sys

# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = "Controller"
target = "Node1"
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "LEADER_INFO"
print(f"Request Created : {msg} and sent to {target}")

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))

# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

while (True):
    
    try:
        msg, addr = skt.recvfrom(1024)
    except:
        print(f"ERROR while fetching from socket : {traceback.print_exc()}")

    # Decoding the Message received from Node 1
    decoded_msg = json.loads(msg.decode('utf-8'))
    print ("..........")
    print ("LEADER_INFO:", decoded_msg["key"], "is current leader.")
    print ("..........")
    break