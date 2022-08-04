import json 
import re 

class HashTable():
    def __init__(self):
        self.hash_dict = {}
        global key_error
        key_error = {"request":"failed","type":"key error"}
    def insert(self,json_load):
        self.hash_dict[json_load['key']]=json_load['value']

    def lookup(self,json_load):
        try:
            values = self.hash_dict[json_load['key']]
            return json.dumps(values)
        except KeyError:
            return json.dumps(key_error)
    def remove(self,json_load):
        try:
            key = json_load['key']
            value = self.hash_dict[json_load['key']]
            self.hash_dict.pop(json_load['key'])
        except KeyError:
            return json.dumps(key_error)
        return (key,value)
    
    def scan(self,json_load):
        regex = json_load['key']
        matches = []
        re_object = re.compile(regex)
        for key, value in self.hash_dict.items():
            if re_object.search(key):
                matches.append((key,value))
        return matches 

    def print_hash(self):
        print(self.hash_dict)   

    def get_table(self):
        return self.hash_dict
