import socket
import json
import sys 
import http.client 

class Client():
    def __init__(self,host,port,server):
        self.host = host
        self.port = port
        self.server = server
        self.client = socket.socket()
        self.client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.attempt_connection(self.host,self.port)

    def attempt_connection(self,host,port):
        try: 
            self.client.connect((host,port))
            self.connected = True
        except:
            #print('exception for',port)
            self.connected = False 
   
    def insert(self,key,value):
        try:
            request = {"method":"insert","key":key,"value":value}
            dumps_request = json.dumps(request)
            length_request = len(dumps_request.encode())
            #self.client.send(str(length_request).encode())                 
            self.client.sendall((str(length_request) + dumps_request).encode()) 
            data = self.client.recv(1024).decode()
            if not data:
                return -1
            #print(data)
            json_data = json.loads(data)
            print(json_data)
        except BrokenPipeError:
            return -1 
        except ConnectionResetError:
            return -1 

    def remove(self,key):
        try: 
            request = {"method":"remove","key":key}
            dumps_request = json.dumps(request)
            length_request = len(dumps_request.encode())
            #self.client.send(str(length_request).encode())
            self.client.sendall((str(length_request) + dumps_request).encode())
            msg_len = self.get_msg_len()
            if msg_len == -1:
                return -1 
            output = self.recv_json(msg_len)
            #key_error = {"request":"failed","type":"key error"}
            print(output)
        except BrokenPipeError:
            return -1
        except ConnectionResetError:
            return -1  

    def remove_noprint(self,key):
        try:
            request = {"method":"remove","key":key}
            dumps_request = json.dumps(request)
            length_request = len(dumps_request.encode())
            #self.client.send(str(length_request).encode())
            self.client.sendall((str(length_request) + dumps_request).encode())
            msg_len = self.get_msg_len()
            if msg_len == -1:
                return -1
            output = self.recv_json(msg_len)
        except BrokenPipeError:
            return -1
        except ConnectionResetError:
            return -1
    
    def insert_noprint(self,key,value):
        try:
            request = {"method":"insert","key":key,"value":value}
            dumps_request = json.dumps(request)
            length_request = len(dumps_request.encode())
            #self.client.send(str(length_request).encode())                 
            self.client.sendall((str(length_request) + dumps_request).encode())
            data = self.client.recv(1024).decode()
            if not data:
                return -1
            #print(data)
            json_data = json.loads(data)
            #print(json_data)
        except BrokenPipeError:
            return -1
        except ConnectionResetError:
            return -1

    def lookup(self,key):
        try:
            request = {"method":"lookup","key":key}
            dumps_request = json.dumps(request)
            length_request = len(dumps_request.encode())
            #self.client.send(str(length_request).encode())
            self.client.sendall((str(length_request) + dumps_request).encode())
            msg_len = self.get_msg_len_lookup()
            if msg_len == -1:
                return -1 
            output = self.recv_json(msg_len)
            new_output = output.replace("\\","")
            key_error = {"request":"failed","type":"key error"}
            print(new_output)
        except BrokenPipeError:
            return -1 
        except ConnectionResetError:
            return -1 
    
    def scan(self,reg_value):
        try:
            request = {"method":"regex","key":reg_value}
            dumps_request = json.dumps(request)
            length_request = len(dumps_request.encode())
            #self.client.send(str(length_request).encode())
            self.client.sendall((str(length_request) + dumps_request).encode())
            msg_len = self.get_msg_len_scan()
            if msg_len == -1:
                return -1 
            output = self.recv_json(msg_len)
            return output
        except BrokenPipeError:
            return -1
        except ConnectionResetError:
            return -1 
        
    def get_msg_len_lookup(self):
        total_length = ''
        char = self.client.recv(1).decode()
        if not char:
           return -1 
        while char != '"':
            total_length += char
            char = self.client.recv(1).decode()
        return int(total_length)
    
    def get_msg_len_scan(self):
        total_length= ''
        char = self.client.recv(1).decode()
        if not char: 
            return -1 
        while char != '(':
            total_length += char
            char = self.client.recv(1).decode() 
        return int(total_length)

    def get_msg_len(self):
        total_length = ''
        char = self.client.recv(1).decode()
        if not char:
           return -1 
        while char != '[':
            total_length += char
            char = self.client.recv(1).decode()
        return int(total_length)

    def recv_json(self,msg_len):
        bytes_recd = 0
        chunks = ''
        while bytes_recd <  msg_len:
            chunk = self.client.recv(min(msg_len - bytes_recd, 2048))
            chunks += chunk.decode()
            bytes_recd += len(chunk)
            if bytes_recd == (msg_len-1):
                break
        return chunks
'''
if __name__ == '__main__':
    Client().client_program()
'''
