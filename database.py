# -*- coding: utf-8 -*-

import sys
import pandas as pd
from pymongo import MongoClient

class Database():
    def __init__(self): ## Setting connection with Mongo Server
          
        try:
            self.conn = MongoClient()
            print("Connected successfully!!!")
            self.db = self.conn.Twitter_DB
            
            self.collectionH = self.db.Health
            self.collectionF = self.db.Food
            self.collectionS = self.db.Soccer
            
        except:  
            print("Could not connect to MongoDB")       
    
    ## Send data to collections
    def send_to_database(self, final_dict):
        
        ## send tweet to collection Soccer
        if final_dict["tag"] == 'Soccer rule':
            self.collectionS.insert_one(final_dict)
            
        ## send tweet to collection Health
        elif final_dict["tag"] == 'Health rule':
          self.collectionH.insert_one(final_dict)

        ## send tweet to collection Food
        else:
            self.collectionF.insert_one(final_dict)
        
            
        print("\nData sent to collection %s\n"%(final_dict["tag"].split(" ")[0]))
        
        
    ## Excute queries in collections: Soccer, Health and Food
    def query_database(self, query1, query2=None):
      
        queryH = self.collectionH.aggregate(query1)
        queryF = self.collectionF.aggregate(query1)
        queryS = self.collectionS.aggregate(query1)
        
        health = queryH.next()
        food = queryF.next()
        soccer = queryS.next()
        
        if query2 != None:
            
            query2H = self.collectionH.aggregate(query2)
            query2F = self.collectionF.aggregate(query2)
            query2S = self.collectionS.aggregate(query2)
            
            health2 = query2H.next()
            food2 = query2F.next()
            soccer2 = query2S.next()
            
            health = health|health2
            food = food|food2
            soccer = soccer|soccer2
        
        return health, food, soccer

        