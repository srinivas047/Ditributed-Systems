import time
import socket
import json
import socket
from threading import Thread
import traceback
import os
from random import randint

server_states = ["FOLLOWER", "CANDIDATE", "LEADER"]

nnodes = 5
nodes_info = []
for i in range(nnodes):
    nodes_info.append("Node" + str(i + 1)) 

stop_threads = False
leader = ""

def create_msg(sender, request, term, key, value):
    msg = {
        "sender_name": sender,
        "request": request,
        "term": term,
        "key": key,
        "value": value}
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes


def listener(skt, data):

    while True:

        if (stop_threads == True):
            continue

        try:
            msg, addr = skt.recvfrom(1024)
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from Node 1
        decoded_msg = json.loads(msg.decode('utf-8'))
        #print(f"Message Received : {decoded_msg} From : {addr}")
        data.append((decoded_msg))

def messenger(skt, hostname, data):

    while True:        

        if (stop_threads == True):
            continue

        for msg, nodes in data:
            data.pop(0)
            for node in nodes:
                if (node == hostname): continue
                try:
                    skt.sendto(msg, (node, 5555))
                    #print (f"Sent {msg} to {node}")
                except:
                    print(f"ERROR while fetching from socket : {traceback.print_exc()}")


if __name__ == "__main__":

    nodeid = os.environ["NODE_ID"]
    hostname = "Node" + str(nodeid)
    print("Starting Node", hostname)

    cur_term = 0
    nvotes = 0
    votedFor = ""
    server_state = "FOLLOWER"
    received_msg = []
    sendout_msg = []
    

    # Creating Socket and binding it to the target container IP and port
    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    # Bind the node to sender ip and port
    UDP_Socket.bind((hostname, 5555))

    th_listener = Thread(target=listener, args=[UDP_Socket, received_msg])
    th_listener.start()

    th_messenger = Thread(target=messenger, args=[UDP_Socket, hostname, sendout_msg])
    th_messenger.start()

    print ("Setup listener thread and messenger thread.")
    print ("RUNNING..")

    tstart = round(time.time() * 1000)
    hbeat_intv = 100
    hstart = -1
    temp_nodes = [x for x in nodes_info]

    #### SAJJA: tester code - Need to be removed.
    # if (nodeid == "4"):
    #     server_state = "LEADER"
    #     cur_term += 1


    while(True):

        timeout_intv = randint(250, 350)
        time_now = round(time.time() * 1000)

        if (stop_threads == True):
            continue

        if server_state == "LEADER":
            #print(hostname, "is the leader.", received_msg)

            time.sleep(0.001)
            
            for rec_msg in received_msg:
                received_msg.pop(0)


                #### BEGIN - TEST PURPOSE ######
                if (rec_msg["request"] == "CONVERT_FOLLOWER"):
                    if (server_state == "LEADER"):
                        leader = ""
                    print (f"{hostname} has changed from {server_state} to FOLLOWER.")
                    server_state = "FOLLOWER"
                    tstart = round(time.time() * 1000)
                    break

                elif (rec_msg["request"] == "TIMEOUT"):
                    tstart = time_now - 2 * timeout_intv
                    print (f"{hostname} is forced to timeout.")
                    if (server_state == "LEADER"):
                        print(f"{hostname} is leader. Ignored timeout.")
                    else:
                        print (f"{hostname} STOPPED.")
                    break

                elif (rec_msg["request"] == "SHUTDOWN"):
                    print (f"{hostname} is forced to shutdown.")
                    stop_threads = True
                    break

                elif (rec_msg["request"] == "LEADER_INFO"):
                    request = "LEADER_INFO_ACK"
                    msg_bytes = create_msg(hostname, request, cur_term, leader, None)
                    sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                #### END - TEST PURPOSE ######

                new_term = 0
                if (rec_msg["term"] != None):
                    new_term = int(rec_msg["term"])
                if new_term > cur_term:
                    #print(f"updated term from {cur_term} to {new_term}")
                    cur_term = new_term
                elif new_term < cur_term:
                    continue

                
                if rec_msg["request"] == "APPEND_RPC":
                    server_state = "FOLLOWER"
                    cur_term = new_term
                    tstart = round(time.time() * 1000)
                    request = "APPEND_RPC_ACK"
                    msg_bytes = create_msg(hostname, request, cur_term, None, None)
                    sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                    leader = ""
                    print("LEADER already available for term", cur_term, ". LEADER will become Follower")
                    break

            time_now = round(time.time() * 1000)
            
            if (hstart == -1 or (time_now - hstart > hbeat_intv)):
                request = "APPEND_RPC"
                msg_bytes = create_msg(hostname, request, cur_term, None, None)
                # print(f"Sending heartbeat from {hostname} after {time_now - hstart}")
                sendout_msg.append((msg_bytes, nodes_info))
                hstart = time_now
                
            

        elif server_state == "CANDIDATE":
            #print(hostname, "is a candiate.", received_msg)
            time.sleep(0.001)

            # start election and count votes.
            for rec_msg in received_msg:
                received_msg.pop(0)

                #### BEGIN - TEST PURPOSE ######
                if (rec_msg["request"] == "CONVERT_FOLLOWER"):
                    server_state = "FOLLOWER"
                    if (server_state == "LEADER"):
                        leader = ""
                    tstart = round(time.time() * 1000)
                    print (f"{hostname} has changed from {server_state} to Follower.")
                    break
                elif (rec_msg["request"] == "TIMEOUT"):
                    tstart = time_now - 2 * timeout_intv
                    print (f"{hostname} is forced to timeout.")
                    break

                elif (rec_msg["request"] == "SHUTDOWN"):
                    print (f"{hostname} is forced to shutdown.")
                    print (f"{hostname} STOPPED.")
                    stop_threads = True
                    break

                elif (rec_msg["request"] == "LEADER_INFO"):
                    request = "LEADER_INFO_ACK"
                    msg_bytes = create_msg(hostname, request, cur_term, leader, None)
                    sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                #### END - TEST PURPOSE ######



                new_term = 0
                if (rec_msg["term"] != None):
                    new_term = int(rec_msg["term"])
                if new_term > cur_term:
                    #print(f"updated term from {cur_term} to {new_term}")
                    cur_term = new_term
                    server_state = "FOLLOWER"
                    print("Outdated term. Candidate", hostname, "will become Follower")
                    tstart = round(time.time() * 1000)
                    nvotes = 0
                    continue
                elif new_term < cur_term:
                    continue

                if rec_msg["request"] == "APPEND_RPC":
                    server_state = "FOLLOWER"
                    cur_term = new_term
                    tstart = round(time.time() * 1000)
                    leader = rec_msg["sender_name"]
                    request = "APPEND_RPC_ACK"
                    msg_bytes = create_msg(hostname, request, cur_term, None, None)
                    sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                    print("Leader already available for term", cur_term, ". Candidate will become Follower")
                    nvotes = 0
                    break
                   
                if rec_msg["request"] == "VOTE_REQUEST":
                    server_state = "FOLLOWER"
                    cur_term = new_term
                    tstart = round(time.time() * 1000)
                    votedFor = rec_msg["sender_name"]
                    request = "VOTE_ACK"
                    msg_bytes = create_msg(hostname, request, cur_term, None, None)
                    sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                    print("Older candidate available. Candidate will become Follower")
                    nvotes = 0
                    break
                

                if rec_msg["request"] == "VOTE_ACK":
                    #temp_nodes.remove(rec_msg["sender_name"])
                    nvotes += 1

                # Majority votes received. Become Leader.
                if (nvotes >= (int(nnodes/2) + nnodes%2)):
                    print("-----")
                    print (f"{hostname} -> ELECTED LEADER WITH {nvotes} VOTES")
                    print("-----")
                    server_state = "LEADER"
                    leader = hostname
                    votedFor = ""
                    nvotes = 0
                    temp_nodes = [x for x in nodes_info]
                    hstart = -1
                    break
            
            if (time_now - tstart < timeout_intv):
                continue

            tstart = round(time.time() * 1000)
            #Send vote requests to all nodes
            request = "VOTE_REQUEST"
            msg_bytes = create_msg(hostname, request, cur_term, None, None)
            sendout_msg.append((msg_bytes, temp_nodes))


        elif server_state == "FOLLOWER":
            #print(hostname, "is a follower.")
            time.sleep(0.001)

            for rec_msg in received_msg:
                received_msg.pop(0)
                
                #print ("gap:", time_now - tstart)
                #### BEGIN - TEST PURPOSE ######
                if rec_msg["request"] == "CONVERT_FOLLOWER":
                    print (f"{hostname} is already a follower.")

                elif (rec_msg["request"] == "TIMEOUT"):
                    tstart = time_now - 2 * timeout_intv
                    print (f"{hostname} is forced to timeout.")
                    break

                elif (rec_msg["request"] == "SHUTDOWN"):
                    print (f"{hostname} is forced to shutdown.")
                    print (f"{hostname} STOPPED.")
                    stop_threads = True
                    break

                elif (rec_msg["request"] == "LEADER_INFO"):
                    request = "LEADER_INFO_ACK"
                    msg_bytes = create_msg(hostname, request, cur_term, leader, None)
                    sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                #### END - TEST PURPOSE ######

                new_term = 0
                if (rec_msg["term"] != None):
                    new_term = int(rec_msg["term"])
                    tstart = round(time.time() * 1000)
                if new_term > cur_term:
                    #print(f"updated term from {cur_term} to {new_term}")
                    cur_term = new_term
                    votedFor = ""
                elif new_term < cur_term:
                    continue
                if (rec_msg["request"] == "VOTE_REQUEST"):
                    if (votedFor == ""):
                        request = "VOTE_ACK"
                        msg_bytes = create_msg(hostname, request, cur_term, None, None)
                        sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                        votedFor = hostname
                    tstart = round(time.time() * 1000)
                elif rec_msg["request"] == "APPEND_RPC":
                    leader = rec_msg["sender_name"]
                    request = "APPEND_RPC_ACK"
                    msg_bytes = create_msg(hostname, request, cur_term, None, None)
                    sendout_msg.append((msg_bytes, [rec_msg["sender_name"]]))
                    tstart = round(time.time() * 1000)

            if (time_now - tstart >= 2 * timeout_intv):
                print ("TIME_OUT:", hostname, "is now candidate.")
                server_state = "CANDIDATE"
                cur_term += 1
                nvotes = 1
                votedFor = ""
                tstart = round(time.time() * 1000)
                continue


print("CODE ENDED.")




