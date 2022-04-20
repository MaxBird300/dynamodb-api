# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 15:41:32 2020

- DynamoDB boto3 documentation can be found here - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html

@author: maxbi
"""
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import numpy as np
import ast
import sys
from math import floor
from ddbwrapper.utilities import timestamp2unix, getDtypes

def calc_kwh(df, resampleInterval="15min"):

    interval_kwh = df.iloc[1:,0].reset_index(drop=True) - df.iloc[:-1,0].reset_index(drop=True)
    df = pd.DataFrame(data = interval_kwh.values, index=df.index[1:]) # timestamp refers to end of time period
    
    if resampleInterval:
        df.index = pd.to_datetime(df.index, unit="s", origin="unix", utc=True).tz_convert("Europe/London")
        df = df.resample(resampleInterval).sum()
        
    return df

class dynamoTable:
    
    def __init__(self, tableName, excelPath, aws_profile_name="default"):
        
        session = boto3.session.Session(profile_name=aws_profile_name)
        self.dynamodb = session.resource('dynamodb')
        self.Table = session.resource('dynamodb').Table(tableName)
        self.Topics = pd.read_excel(excelPath, sheet_name="Store MQTT points", usecols=["Point ID","Outstation","Name","Units","final_topic"])
        self.weatherTypes = pd.read_excel(excelPath, sheet_name="weather", usecols=["Variable"])
        self.carbonTypes = pd.read_excel(excelPath, sheet_name="carbon", usecols=["Variable"])


    def queryDynamo(self, pk: str, unix_start: int, unix_end: int) -> (dict, bool):
        
        # query the values for a specific topic, from queryTimestamp to present time. 
        kce = Key("PK").eq(pk) & Key("unixTimestamp").between(unix_start, unix_end) # key condition expression  
        response = self.Table.query(KeyConditionExpression=kce)
        
        # As long as "LastEvaluatedKey" is in response it means there are still items from the query which haven't been pulled in (1MB query limit)
        items = response["Items"]
        while "LastEvaluatedKey" in response:
            response = self.Table.query(
                KeyConditionExpression = kce,
                ExclusiveStartKey=response["LastEvaluatedKey"])
            items += response["Items"]
        
        if response["Count"] == 0:
            print(f"{pk} has no data for this time period")
            empty_response = True
        else:
            print(f"Successfully queried data for {pk}")
            empty_response = False
            
        return items, empty_response

    def getTopicData(self, topic: str, unix_start: int, unix_end: int, freq: int) -> pd.DataFrame:
        """
        topic: string, MQTT topic name
        unix_start: int, unix timestamp for start of query
        unix_end: int, unix timestamp for end of query
        freq: int, frequency of returned data in seconds
        
        df: pandas dataframe, index of unix timestamps, column of readings, column name is MQTT topic
        
        """
        
        items, empty_response = self.queryDynamo(topic, unix_start, unix_end)
        if empty_response:
            df = pd.DataFrame(data = {topic: [np.nan]}, index=[unix_start])
        else:
            # pull out values and timestamps from response
            vals = [float(x['value_raw'].value.decode("utf-8")) for x in items] # decode from boto3 binary dtype
            unix_ts = [int(x['unixTimestamp']) for x in items]
            df = pd.DataFrame(data = {topic: vals}, index=unix_ts)       
        
        # bin data in regular intervals at chosen frequency - won't interpolate missing values!
        if freq:
            bins = [unix_start + freq*x for x in range(int((unix_end-unix_start)/freq)+1)]
            df = df.groupby(pd.cut(df.index, bins)).mean()
            df.index = bins[:-1]

        return df
    
    def getTopicsData(self, topic_list: list[str], unix_start: int, unix_end: int, freq: int) -> pd.DataFrame:
        """
        topic_list: list, MQTT topic name strings
        unix_start: int, unix timestamp for start of query
        unix_end: int, unix timestamp for end of query
        freq: int, frequency of returned data in seconds
        
        df: pandas dataframe, index of timestamps, each column is readings for all topics, column name is MQTT topic
        
        """
        
        df = self.getTopicData(topic_list[0], unix_start, unix_end, freq)
        for topic in topic_list[1:]:
            df = pd.concat([df, self.getTopicData(topic, unix_start, unix_end, freq)], axis = 1)
            
        # convert unix timestamp to local london time
        df.index = pd.to_datetime(df.index, unit="s", origin="unix", utc=True).tz_convert("Europe/London")  

        return df
    
    def getEnergyMeters(self, topic_list: list[str], unix_start: int, unix_end: int, resample_interval: str) -> pd.DataFrame:
        """
        topic_list: MQTT topic names for energy meters
        unix_start: unix timestamp for start of query
        unix_end: unix timestamp for end of query
        resample_interval: resample frequency to pass to pandas resample function
        
        df: index of timestamps (refers to end of period), each column is readings for all topics, column name is MQTT topic
        
        """

        raw_meter_0 = self.getTopicData(topic_list[0], unix_start, unix_end, freq=None)
        all_meters = calc_kwh(raw_meter_0, resample_interval)
        
        for topic in topic_list[1:]:
            meter_n = self.getTopicData(topic, unix_start, unix_end, freq=None)
            all_meters = pd.concat([all_meters, calc_kwh(meter_n, resample_interval)], axis=1)
        
        all_meters.columns = topic_list
        
        return all_meters
    
    def formatBatchQuery(self, pk: str, unix_start: int, unix_end: int, data_freq: int, attribute_list: list[str], forecast_horizon: int) -> list[dict]:
        """
        Returns list of batchKeys dicts (each <= 100 items in length) to be used in batchQuery function. Only used for weather and carbon queries at the moment. 

        pk: string, Primary key for the data you want to collect.
        unix_start: int, unix timestamp for start of query
        unix_end: int, unix timestamp for end of query
        data_freq : integer, Number of seconds between data points in Dynamo.
        attribute_list : list, attribute names to get from dynamo
        forecast_horizon : integer, Number of forecast horizons you want to query from dynamo

        batch_key_list : list, of batchKey dicts to be used in batch_get_item

        """
        if pk == "weather":
            offset = int(0) # weather data doesn't need an offset, timestamps already refer to beginning of time period. 
        elif pk == "carbon":
            offset = int(60*30) # in dynamo carbon unixTimestamps refer to end of time period. This changes it to refer to beginning of time period. 
            
        unix_start += offset
        unix_end += offset
        
        # build list of primary keys and sort keys
        sks_list = []
        currentUnix = unix_start
        while currentUnix < unix_end:
            sks_list.append(currentUnix)
            currentUnix += data_freq
        
        pks_list = [pk] * len(sks_list)
        
        # build attributes string for "ProjectionExpression" argument in batchKeys
        attributeStr = "unixTimestamp," # always want to return the unixTimestamp
        for attribute in attribute_list:
            for x in range(forecast_horizon+1):
                attributeStr += attribute + "_" + str(x) + ","
        
        # create a list of batchKeys (each less than 100 items in length)
        batch_key_list = []
        queryNo = floor(len(pks_list)/100) # number of batch_get_item requests you need to make 
        
        currentQuery = 0
        while currentQuery <= queryNo:
            pk_list = pks_list[currentQuery*100:(currentQuery+1)*100]
            sk_list = sks_list[currentQuery*100:(currentQuery+1)*100]
        
            keys = [] # store dictionaries for primary/sort key pairs
            for entry in range(len(pk_list)):
                keys.append({'PK': pk_list[entry],
                             "unixTimestamp": sk_list[entry]})
            
            batchKeys = {self.Table.table_name: {"Keys": keys}}   
            ## Specify which data we want to get from dynamo
            batchKeys[self.Table.table_name]["ProjectionExpression"] = attributeStr[:-1] # remove extra comma
        
            batch_key_list.append(batchKeys)
            currentQuery += 1
        
        return batch_key_list
    
    def batchQuery(self, batch_keys: dict) -> dict:
        """ Calls the batch_get_item function for boto3, for only 100 items at once. """
        if len(batch_keys[self.Table.table_name]["Keys"]) > 100:
            print("More than 100 keys requested in batch_query - request failed.")
            sys.exit(0)
           
        response = self.dynamodb.batch_get_item(RequestItems=batch_keys)
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            # print("Query successful!")
            pass
        else:
            print("Query failed, https code: %i" % response["ResponseMetadata"]["HTTPStatusCode"])
        
        return response
    
    def queryWeatherOrCarbon(self, pk: str, unix_start: int, unix_end: int, attributes: list[str], forecast_horizon: int) -> pd.DataFrame:
        """
        Function to return weather or carbon data collected for the time period [strStart, strEnd]. 
        Timestamps represent the start of the time period. 

        pk : Partition key for data you want to collect.
        unix_start: unix timestamp for start of query
        unix_end: unix timestamp for end of query
        attributes : list of attributes to return, complete list can be found in "data collected in DynamoDB.xlsx". Can also be "all" to return all weather data.
        forecast_horizon : Number of forecast horizons to return.

        Returns
        -------
        df: Indexes are london timestamps for the beginning of the time period, columns are weather data and forecast values.

        """
        if pk == "weather":
            data_freq=int(60*60)
            if attributes == "all":
                attributes = [x for x in self.weatherTypes["Variable"]]
        elif pk == "carbon":
            data_freq=int(60*30)
            if attributes == "all":
                attributes = [x for x in self.carbonTypes["Variable"]]
        
        batch_key_list = self.formatBatchQuery(pk, unix_start, unix_end, data_freq, attributes, forecast_horizon)
        
        allRawData = []
        for batchKeys in batch_key_list:
            response = self.batchQuery(batchKeys)
            
            if len(response["UnprocessedKeys"]) != 0:
                print("Batch query failed to get all data, didn't retrieve %i keys" % len(response["UnprocessedKeys"]))
                print("Your code currently doesn't handle retrying to get these unprocessed keys - BE CAREFUL!")
            
            rawData = response["Responses"][self.Table.table_name]
            
            allRawData += rawData
        
        # format returning dataframe in a nice way        
        df = pd.DataFrame(allRawData) # this will have missing data
        
        # specify datatypes
        dtype_dict = getDtypes(attributes, forecast_horizon) 
        df = df.astype(dtype_dict)
        
        if pk == "carbon":
            df["unixTimestamp"] = df["unixTimestamp"] - data_freq # adjust timestamp to reflect start of time period rather than end. This has been correctly adjusted for the query too. 
        
        df["Timestamp"] = pd.to_datetime(df["unixTimestamp"], unit="s", origin="unix", utc=True).dt.tz_convert("Europe/London")   
        df.set_index("Timestamp", drop=True, inplace=True)
        df.sort_values(by="Timestamp", inplace=True)
        # sort columns in correct order
        cols = ["unixTimestamp"] + [x + "_" + str(y) for x in attributes for y in range(forecast_horizon+1)]
        df = df[cols]
                
        return df
    
    def lastKnownValue(self, topics: list[str]) -> pd.DataFrame:
        """ Get the last known value of any topic. """
         
        pks_list = topics
        sks_list = [int(0)] * len(pks_list)
        
        # create a list of batchKeys (each less than 100 items in length)
        batch_key_list = []
        queryNo = floor(len(pks_list)/100) # number of batch_get_item requests you need to make 
        
        currentQuery = 0
        while currentQuery <= queryNo:
            pk_list = pks_list[currentQuery*100:(currentQuery+1)*100]
            sk_list = sks_list[currentQuery*100:(currentQuery+1)*100]
        
            keys = [] # store dictionaries for primary/sort key pairs
            for entry in range(len(pk_list)):
                keys.append({'PK': pk_list[entry],
                             "unixTimestamp": sk_list[entry]})
            
            batchKeys = {self.Table.table_name: {"Keys": keys}}   
            
            batch_key_list.append(batchKeys)
            currentQuery += 1
        
        messagesList = []
        topicsList = []
        for batchKeys in batch_key_list:
            response = self.batchQuery(batchKeys)
            
            if len(response["UnprocessedKeys"]) != 0:
                print("Batch query failed to get all data, didn't retrieve %i keys" % len(response["UnprocessedKeys"]))
                print("Your code currently doesn't handle retrying to get these unprocessed keys - BE CAREFUL!")
            
            messagesList += [x["message"] for x in response["Responses"][self.Table.table_name]] # list of mqtt payloads represented as a JSON string
            topicsList += [x["PK"] for x in response["Responses"][self.Table.table_name]] # list of topic strings
        
        df = pd.DataFrame()
        for entry in range(len(messagesList)):
            if messagesList[entry] == '{"ICL":': # this occurs for some faulty points in the GSHP list
                df = df.append([[topicsList[entry], np.nan, np.nan]])
            else:
                messageDict = ast.literal_eval(messagesList[entry])
                # print(messageDict)
                try:
                    df = df.append([[topicsList[entry], int(messageDict["ICL_ts"]), messageDict["ICL"]]])
                except KeyError:
                    df = df.append([[topicsList[entry], int(0), messageDict["ICL"]]])
        
        df.insert(loc=0, column="Timestamp", value = pd.to_datetime(df[1], unit="s", origin="unix", utc=True))          
        df["Timestamp"] = df["Timestamp"].dt.tz_convert("Europe/London") # convert UTC timestamp to local london time 
        
        df.columns = ["Timestamp","Topic","unixTimestamp","Value"]
        
        # sort topic values into the same order they came into the function
        df.set_index("Topic", inplace=True)
        df = df.reindex(topics)
        
        return df
          
        