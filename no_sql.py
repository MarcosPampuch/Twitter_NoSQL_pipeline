#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 25 14:47:22 2022

@author: marcos
"""

from pymongo import MongoClient
from database import Database

# Connect to database and collections
db = Database()

## a) Qual o horário do tweet mais antigo e do mais recente para cada regra?

# Query
pipeline = [
            { "$sort": {"Date": 1, "Hour": 1}},
            {"$group" :
        {
            "_id": "null" ,
            "lastHour": { "$last": "$Hour" },
            "lastDay": {"$last": "$Date"},
            "firstHour": { "$first": "$Hour"},
            "firstDay": {"$first": "$Date"}
         }}]


## Executing Query with Cursor

health, food, soccer = db.query_database(pipeline)


# Outputting Results
print("\n\nA) Qual o horário do tweet mais antigo e do mais recente para cada regra?")
    
print("\nHEALTH COLLECTION")
print("The oldest Tweet was at {Hour}, date: {Date}".format(Hour=health["lastHour"], Date=health["lastDay"]))
print("The newest Tweet was at {Hour}, date: {Date}\n".format(Hour=health["firstHour"], Date=health["firstDay"]))
    
print("\nFOOD COLLECTION")
print("The oldest Tweet was at {Hour}, date: {Date}".format(Hour=food["lastHour"], Date=food["lastDay"]))
print("The newest Tweet was at {Hour}, date: {Date}\n".format(Hour=food["firstHour"], Date=food["firstDay"]))

print("\nSOCCER COLLECTION")
print("The oldest Tweet was at {Hour}, date: {Date}".format(Hour=soccer["lastHour"], Date=soccer["lastDay"]))
print("The newest Tweet was at {Hour}, date: {Date}\n".format(Hour=soccer["firstHour"], Date=soccer["firstDay"]))



## b) Qual o período do dia em que cada regra se torna mais frequente?

        ## periods: Dawn    ---> FROM 00:00:00 TO 05:59:59
        ## periods: Morning ---> FROM 06:00:00 TO 11:59:59
        ## periods: Evening ---> FROM 12:00:00 TO 17:59:59
        ## periods: Night   ---> FROM 18:00:00 TO 23:59:59
        
# Query
pipeline = [
            {"$group" :{ "_id": "$Period", "Count": {"$sum": 1}}},
            {"$sort": {"Count": -1}}]

health, food, soccer = db.query_database(pipeline)


print("\n\nB) Qual o período do dia em que cada regra se torna mais frequente?")

print("\nHEALTH COLLECTION")
print("Most part of the tweets were during {Period} ({Count})".format(Period=health["_id"], Count=health["Count"]))
    
print("\nFOOD COLLECTION")
print("Most part of the tweets were during {Period} ({Count})".format(Period=food["_id"], Count=food["Count"]))

print("\nSOCCER COLLECTION")
print("Most part of the tweets were during {Period} ({Count})".format(Period=soccer["_id"], Count=soccer["Count"]))



# c) Qual o tweet mais longo em número de caracteres para cada regra? E o mais curto?


pipeline_Biggest = [{"$project":{"idB": "$id", "_id": 0, "lengthB": { "$strLenCP": "$text"} } }, 
                    {"$sort": {"lengthB": -1}}, 
                    {"$limit" : 1}]

pipeline_Smallest = [{"$project":{"idS": "$id", "_id": 0, "lengthS": { "$strLenCP": "$text"} } }, 
                    {"$sort": {"lengthS": 1}}, 
                    {"$limit" : 1}]


health, food, soccer = db.query_database(pipeline_Biggest, pipeline_Smallest)

print("\n\nC) Qual o tweet mais longo em número de caracteres para cada regra? E o mais curto?")

print("\nHEALTH COLLECTION")
print("The biggest tweet has {lengthB} characters (id: {idB})".format(lengthB=health["lengthB"], idB=health["idB"]))
print("The smallest tweet has {lengthS} characters (id: {idS})".format(lengthS=health["lengthS"], idS=health["idS"]))
    
print("\nFOOD COLLECTION")
print("The biggest tweet has {lengthB} characters (id: {idB})".format(lengthB=food["lengthB"], idB=food["idB"]))
print("The smallest tweet has {lengthS} characters (id: {idS})".format(lengthS=food["lengthS"], idS=food["idS"]))

print("\nSOCCER COLLECTION")
print("The biggest tweet has {lengthB} characters (id: {idB})".format(lengthB=soccer["lengthB"], idB=soccer["idB"]))
print("The smallest tweet has {lengthS} characters (id: {idS})".format(lengthS=soccer["lengthS"], idS=soccer["idS"]))























