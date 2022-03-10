#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 23:34:03 2022

@author: marcos
"""

import sys
import json
import pandas as pd
from database import Database
import requests
from datetime import datetime
from pymongo import MongoClient

class StreamSQL():
    def __init__(self, bearer_token):
        
        self.bearer_token = bearer_token

    ## method for Stream authentication
    def bearer_oauth(self, r):
    
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

    ## fetch actual rules of the Stream
    def get_rules_stream(self):
        self.response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearer_oauth
        )
        if self.response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(self.response.status_code, self.response.text)
            )
        print("\nFetching rules of Steam..\n")
        return self.response.json()
    
    ## delete actual rules of the Stream
    def delete_rules_stream(self, rules):
        if rules is None or "data" not in rules:
            return None
    
        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print("\nDeleting rules of Stream..\n")
    
    ## set new rules to the Stream
    def set_rules_stream(self, delete):
        # You can adjust the rules if needed
        sample_rules = [
            {"value": "Futebol lang:pt", "tag": "Soccer rule"},
            {"value": "Saúde lang:pt", "tag": "Health rule"},
            {"value": "Comida lang:pt", "tag": "Food rule"}
        ]
        payload = {"add": sample_rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        print("\nSetting rules to Stream..\n")
    
    ## Create and organize main dataframe columns 
    def column_adjust(self, json_response):
        
        list_match = list(json_response["matching_rules"][0].items())
        
        final_dict = json_response["data"]|{list_match[1][0]:list_match[1][1]}           

        list_date = final_dict["created_at"].split("T")
        
        final_dict["Date"] =  final_dict.pop("created_at")

        final_dict["Date"] = list_date[0]
        final_dict["Hour"] = list_date[1].split(".")[0]
  
        time = datetime.strptime(final_dict["Hour"], "%H:%M:%S")

        if time >= datetime.strptime("00:00:00", "%H:%M:%S") and time <= datetime.strptime("05:59:59", "%H:%M:%S"):
            final_dict = final_dict|{"Period" : "Dawn"}
        elif time >= datetime.strptime("06:00:00", "%H:%M:%S") and time <= datetime.strptime("11:59:59", "%H:%M:%S"):
            final_dict = final_dict|{"Period" : "Morining"}
        elif time >= datetime.strptime("12:00:00", "%H:%M:%S") and time <= datetime.strptime("17:59:59", "%H:%M:%S"):
            final_dict = final_dict|{"Period" : "Evenning"}
        else:
            final_dict = final_dict|{"Period" : "Night"}
        
        
        return final_dict
        
    ## Open Stream connection and send to SQL database
    def stream_sql(self, set):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at", 
            auth=self.bearer_oauth, stream=True)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        print("\nStream connection stablished!\n")
        
                    
        db = Database()
        
        for response_line in response.iter_lines():
            if response_line:
                
                json_response = json.loads(response_line)
    
                # Filtering columns to send to database
                final_dict = self.column_adjust(json_response)

                # send tweet to collections         
                db.send_to_database(final_dict)

                    
                
                
                
                
                
                
                
                