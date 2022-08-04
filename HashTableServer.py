import socket 
import sys 
import argparse 
from HashTable import HashTable
import json 
import os 
import random
import select
import time
import datetime 

class server_program():
    #include port for commandline 
    def __init__(self,project):
        self.connection = None
        self.client_address = None
        self.port = 0
        self.host = ''
        self.project = project
        self.hash_table = HashTable()
        self.count = 0
        self.trans_count = 0
        self.log_count = 0 
        self.timeout = 1
        self.server_socket = None 
        global failure
        global success
        global key_error
        key_error = {"request":"failed","type":"key error"} 
        failure =  {"request":"failure"}
        success =  {"request": "success"} 

    def server(self): 
        # get the hostname
        #host = socket.gethostbyname("")
        #print(host)
        self.server_socket = socket.socket()  # get instance
        self.server_socket.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY, 1)
        #print("Listening on port",self.port)
        self.server_socket.bind((self.host, self.port))  # bind host address and port together
        print("Listening on port", self.server_socket.getsockname()[1])
        self.server_socket.listen(100)
        # listening to one client
        self.load_checkpoint()
        self.add_logs()
        self.handle_request()

    def handle_request(self):
        UDP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        UP_address = (("129.74.152.177", 9097))
        update_message = {
        "type" : "hashtable",
        "owner" : "tchambe2",
        "port" : int(self.server_socket.getsockname()[1]),
        "project" : self.project
        }
        start_time = time.time()
        socket_list = [ self.server_socket ] 
        while socket_list:
            UDP_sock.sendto(json.dumps(update_message).encode(),UP_address)
            try:
                read_sockets, w_sockets, e_sockets = select.select(socket_list, [], [],self.timeout)
                if time.time() >= start_time + 60:
                    #print("time limit reached, sending update")
                    start_time = time.time()
                    UDP_sock.sendto(json.dumps(update_message).encode(),UP_address)
                #elapsed_time = round(stop - start, 2)
                for sock in read_sockets:
                    #print('loop over read')
                    if sock == self.server_socket:
                        # check all incoming connections
                        self.connection, self.client_address = self.server_socket.accept()               
                        #adding connection to set 
                        socket_list.append(self.connection)
                        #print('added connection')
                        #print("Connection from client address: " + str(self.client_address))
                        #print()
                        #print(socket_list)
                    else:
                        #print('next iteration of sock')
                        char = sock.recv(1).decode()
                        if not char:
                            #going to next iteration
                            socket_list.remove(sock)
                            sock.close()
                            #print('socket closed')
                            break
                        msg = self.get_msg_len(char,sock)
                        result = self.recv_json(msg,sock)
                        result_final = "{" + result
                        j = json.loads(result_final)
                        if j["method"] == 'insert':
                            self.insert(j,sock)
                        if j["method"] == 'lookup':
                            self.lookup(j,sock)
                        if j["method"] == 'remove':
                            self.remove(j,sock)
                        if j["method"] == 'regex':
                            self.scan(j,sock)
                        # self.connection.send(data.encode())  # send data to the client
                        #self.connection.close()  # close the connection
            except ConnectionResetError:
                self.handle_request()
       
    def insert(self,j,sock):
        self.trans_count += 1
        self.log_count += 1 
        self.log(j)
        if self.trans_count == 100:
            self.trans_count = 0
            self.new_checkpoint()
        self.hash_table.insert(j)
        self.count += 1
        message = json.dumps(success)
        sock.send(message.encode())
        print("client ", sock.getpeername(),"stub ",self.count, "insert method")
        #self.hash_table.print_hash()

    def lookup(self,j,sock):
        lookup_item = self.hash_table.lookup(j)
        self.count += 1
        key_error_recv = json.dumps(key_error)
        if key_error_recv  == lookup_item:
            length_request = len(key_error_recv.encode())
            #sock.send((str(length_request)+'"').encode())
            sock.sendall((str(length_request)+'"'+key_error_recv).encode())
            #print("client, ",sock.getpeername(),"stub ",self.count,"lookup method")
        else:
            message = json.dumps(lookup_item)
            length_request = len(message.encode())
            #sock.send(str(length_request).encode())
            sock.sendall((str(length_request) + message).encode())
            #print("client, ",sock.getpeername(),"stub ",self.count,"lookup method")
            
    def remove(self,j,sock):
        self.trans_count += 1
        self.log_count += 1 
        self.log(j)
        if self.trans_count == 100:
            self.trans_count = 0
            self.new_checkpoint()
        output  = self.hash_table.remove(j)
        self.count += 1
        key_error_recv = json.dumps(key_error)
        if key_error_recv == output:
            length_request = len(key_error_recv.encode())
            #sock.send((str(length_request)+'[').encode())
            sock.sendall((str(length_request)+'['+key_error_recv).encode())
            print("client, ",sock.getpeername(),"stub ",self.count,"remove method")
        else: 
            message = json.dumps(output)
            length_request = len(message.encode())
            #sock.send(str(length_request).encode())
            sock.sendall((str(length_request) + message).encode())
            print("client, ",sock.getpeername(),"stub ",self.count,"remove method")
    
    def scan(self,j,sock):
        output = self.hash_table.scan(j)
        self.count += 1 
        if not output:
            message = ('({request:failed, type:no output return with scan}')
            length_request = len(message.encode())
            #sock.send(str(length_request).encode())
            sock.sendall((str(length_request)+message).encode())
        message = ''.join(str(i) for i in output)
        length_request = len(message.encode())
        #sock.send(str(length_request).encode())
        sock.sendall((str(length_request) + message).encode())
        #print("client, ",sock.getpeername(),"stub ",self.count,"scan  method")    
        
    def get_msg_len(self,char,connection):
        total_length = ''
        while char != '{':
            total_length += char
            char = connection.recv(1).decode()
        return int(total_length)

    def recv_json(self,msg_len,connection):
        bytes_recd = 0
        chunks = ''
        while bytes_recd <  msg_len:
            chunk = connection.recv(min(msg_len - bytes_recd, 2048))
            chunks += chunk.decode()
            bytes_recd += len(chunk)
            if bytes_recd == (msg_len-1):
                break
        return chunks

    def log(self,j):
        with open('table.txn','a') as outfile:
            json.dump(j,outfile)
            outfile.write('modification_added'+'\n')
            outfile.flush()
            os.fsync(outfile.fileno())
        #log_check = open('table.txn','r')
        #data = log_check.readlines()
        #print("Checking Current Log")
        #print()
        #print(data)
        
    def add_logs(self):
        if os.path.isfile('table.txn'):
             log_check = open('table.txn','r')
             data = log_check.readlines()
             #print("printing log")
             #print(data)
             #print()
             for i in range(len(data)):
                 if i == 0:
                    continue
                 temp = (data[i].replace('modification_added'+'\n',""))
                 log = json.loads(temp)
                 if log["method"] == "insert":
                     self.hash_table.insert(log)
                 if log["method"] == "remove":
                     self.hash_table.remove(log)
             #print("hash update, reading back old logs")
             #print()
             #self.hash_table.print_hash()
             return
        else:
            with open('table.txn','w') as log:
                log.write('New instance of transaction log')
                log.write('\n')
                log.flush()
                os.fsync(log.fileno())
            return 


    def new_checkpoint(self):
        table = self.hash_table.get_table()
        #print("compressing log and checkpoint")
        with open('temp.cpkt','w') as checkpoint:
            checkpoint.write(json.dumps(table))
            checkpoint.flush()
            os.fsync(checkpoint.fileno())
        try:
            os.rename('temp.cpkt','table.cpkt')
            with open('table.txn','w') as log:
                log.write('New instance of transaction log')
                log.write('\n')
                log.flush()
                os.fsync(log.fileno())
        except:
            with open('table.cpkt','w') as checkpoint:
                checkpoint.write(json.dumps(table))
                checkpoint.flush()
                os.fsync(checkpoint.fileno())
            with open('table.txn','w') as log:
                log.write('New instance of transaction log')
                log.write('\n')
                log.flush()
                os.fsync(log.fileno())
        #json_file = open('table.cpkt','r')
        #data = json_file.readlines()
        #print("reading new hash_checkpoint")
        #print()
        #print(data)
        #print()
        #print("printing current hash")
        #self.hash_table.print_hash()

    def load_checkpoint(self):
        if os.path.isfile('table.cpkt'):
           # print("loading checkpoint")
            json_file = open('table.cpkt','r')
            dict_holder = ''
            for i in json_file:
                dict_holder += i
            data = json.loads(dict_holder)
            #print(data)
            for i in data.keys():
                self.hash_table.hash_dict[i] = data[i]
            #print("printing loaded checkpoint hash")
            #print()
            #self.hash_table.print_hash()
            return
        else:
            #print("no loaded checkpoint found")
            return 

if __name__ == '__main__':
    server_program(str(sys.argv[1])).server()
