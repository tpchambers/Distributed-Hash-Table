from HashTableClient import Client
import hashlib
import socket
import json
import sys 
import http.client
import time 

class ClusterClient():
    def __init__(self,project,server_number,number_copies):
        ## discover servers and add to list of servers
        self.server_cluster = []
        self.number_copies = number_copies
        connection = http.client.HTTPConnection('catalog.cse.nd.edu:9097')
        connection.request("GET","/query.json")
        response = connection.getresponse()
        data = response.read().decode()
        connection.close()
        json_dump = json.loads(data)
        self.cluster_dict = {}
        self.cluster_keys = {}
        self.server_number = server_number
        self.port_list = []
        self.key_list = []
        self.scan_list=[]
        for i in json_dump:
            temp = json.dumps(i)
            temp_2 = json.loads(temp)
            try:
                for i in range(server_number):
                    if temp_2["type"] == "hashtable" and temp_2["project"] == project+"-"+str(i):
                        client = Client(temp_2["name"],int(temp_2["port"]),temp_2["project"])
                        if client.connected != False:
                            self.cluster_dict[i] = client
                            self.cluster_keys[i] = []
                            self.key_list.append(i)
                        else:
                            client = None
            except KeyError:
                pass

        self.size = len(self.cluster_dict.keys()) 

    def hash_key(self,key):
        return (int(hashlib.sha512(key.encode('utf-8')).hexdigest(),16) % self.size)

    def insert(self,key,value):
        count = 0
        hash_number = self.hash_key(key)
        i = 0 
        while True:
            if hash_number + i < self.server_number and i != self.number_copies:
                if count != 0:
                    if self.cluster_dict[hash_number+i].insert_noprint(key,value) == -1:
                        self.find_available_port(hash_number+i,key,value,'insert_noprint')
                        #i += 1 
                        #remaining = self.number_copies - i
                    self.add_key(hash_number+i,key)
                    i += 1 
                    remaining = self.number_copies - i
                else:
                    if self.cluster_dict[hash_number+i].insert(key,value) == -1:
                        self.find_available_port(hash_number+i,key,value,'insert')
                        #i += 1
                        #remaining = self.number_copies - i
                    count += 1
                    self.add_key(hash_number+i,key)
                    i += 1
                    remaining = self.number_copies - i
            else:
                for i in range(remaining):
                    if self.cluster_dict[i].insert_noprint(key,value) == -1:
                        self.find_available_port(i,key,value,'insert_noprint')
                    self.add_key(i,key)
                break
      

    def remove(self,key):
        value = None
        count = 0 
        if any(k != [] for k in self.cluster_keys.values()):
            for k,v in self.cluster_keys.items(): 
                for i in v:
                    if i == key:
                        if count != 0:
                            if self.cluster_dict[k].remove_noprint(key) == -1:
                                self.find_available_port(k,key,value,'remove_noprint')
                        else:
                            if self.cluster_dict[k].remove(key) == -1:
                                self.find_available_port(k,key,value,'remove')
                            count += 1 
        else:
            for i in range(self.server_number):
                if self.cluster_dict[i].remove(key) == -1:
                    self.find_available_port(i,key,value,'remove')                          
    
    def lookup(self,key):
        value = None
        count = 0 
        if any(k != [] for k in self.cluster_keys.values()):
            for k,v in self.cluster_keys.items():
                for i in v:
                    if i == key:
                        #print(self.cluster_dict[k].port)
                        if self.cluster_dict[k].lookup(key) == -1:
                            count += 1
                            if count != self.number_copies:
                                continue
                            else:
                                self.find_available_port(k,key,value,'lookup')
                        else:
                            return
        else:
            for i in range(self.server_number):
                if self.cluster_dict[i].lookup(key) == -1:
                    self.find_available_port(i,key,value,'lookup')
                else:
                    return
    def scan(self,key):
        final_scan_list = []
        value = None
        for k,v in self.cluster_dict.items():
            try: 
               output=self.cluster_dict[k].scan(key).split(')')
               for i in output:
                   if i not in self.scan_list:
                       self.scan_list.append(i)
            except:
                self.find_available_port(k,key,value,'scan')
        if self.scan_list:
            print(self.scan_list[:-1])
        self.scan_list = [] 
           

    def add_key(self,index,key):
        if key not in self.cluster_keys[index]:
            self.cluster_keys[index].append(key)
    
    def find_available_port(self,j,key,value,method):
        #print('called again')
        connection = http.client.HTTPConnection('catalog.cse.nd.edu:9097')
        connection.request("GET","/query.json")
        response = connection.getresponse()
        data = response.read().decode()
        connection.close()
        json_dump = json.loads(data)
        for i in json_dump:
            temp = json.dumps(i)
            temp_2 = json.loads(temp)
            try:
                if temp_2["type"] == "hashtable" and temp_2["owner"] == 'tchambe2' and temp_2["project"] == self.cluster_dict[j].server:
                    client = Client(temp_2["name"],int(temp_2["port"]),temp_2["project"])
                    if client.connected != False:
                        self.cluster_dict[j] = client
                        if method == 'insert':
                            self.cluster_dict[j].insert(key,value)
                            self.add_key(j,key)
                            return
                        elif method == 'insert_noprint':
                            self.cluster_dict[j].insert_noprint(key,value)
                            self.add_key(j,key)
                            return
                        elif method == 'remove_noprint':
                            self.cluster_dict[j].remove_noprint(key)
                            return
                        elif method == 'remove':
                            self.cluster_dict[j].remove(key) 
                            return
                        elif method == 'lookup':
                            self.cluster_dict[j].lookup(key)
                            return
                        elif method == 'scan':
                            output = self.cluster_dict[j].scan(key).split(')')
                            for i in output:
                                if i not in self.scan_list:
                                    self.scan_list.append(i)
                            return
                    else:
                        continue
            except KeyError:
                pass
        time.sleep(5)
        self.find_available_port(j,key,value,method)
    
if __name__ == '__main__':
    
    client_instance = ClusterClient(str(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]))  
    '''
    client_instance.insert("fred",{"json":"test"}) 
    client_instance.insert("freddington",{"first":"test_again"}) 
    #client_instance.remove("fred") 
    client_instance.insert("fred2",{"json2":"test2"})
    client_instance.remove("fred")
    client_instance.remove("freddington")
    client_instance.remove("fred2") 
    #print(client_instance.cluster_keys)
    '''

    data = {
    'name': 'Patrick Jane',
    'age': 45,
    'children': ['Susie', 'Mike', 'Philip'],
    'divorced' : 'Yes',
    'testing' :'finance',
    'okay' : 'probably not'
  }  
    '''
    iter_list=[]
    loop_number = 1000
    for i in range(loop_number):
        start_iter = time.time()
        key = "insert_test" + str(i)
        client_instance.insert(key,data)
        end_iter = time.time()
        iter_list.append(end_iter-start_iter)
    '''
    '''
    '''
     
    for i in range(1000):
       key = "insert_test" + str(i)
       client_instance.lookup(key)
    ''' 
    for i in range(1000):
        key = "insert_test" + str(i)
        client_instance.remove(key)
    '''
    '''
    client_instance.cluster_dict[0].insert("freddington",{"first":"test_twice"})
    client_instance.cluster_dict[1].insert("freddy",{"first":"yet_twice"})
    client_instance.cluster_dict[1].insert("freddington",{"first":"test_twice"})
    client_instance.scan("insert")
    time.sleep(2)
    client_instance.scan("fred") 
    '''
