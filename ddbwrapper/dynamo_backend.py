# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 15:41:32 2020

- DynamoDB boto3 documentation can be found here - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html

@author: maxbi
"""
import boto3
import pandas as pd
import numpy as np
import json
import sys
from math import floor
from ddbwrapper.utilities import getDtypes, timestamp2unix

def calc_kwh(cumulative_series, resample_str: str=None):
    """
    Calculate kWh consumption between consecutive readings from a series of cumulative kWh meter readings.
    
    This function converts the Unix timestamp index to a London timezone-aware datetime index, and
    returns a new series where each value represents the kWh consumed between the current reading and
    the following one. The index of the returned series corresponds to the start of each metering period.
    
    Parameters:
        cumulative_series (pd.Series): A pandas Series with:
            - Unix timestamps (in seconds) as the index.
            - Cumulative kWh meter readings as the values.
    
    Returns:
        pd.Series: A series containing the kWh consumed for each period, with an index of London timezone-aware
                   datetime objects referring to the start of the period.
    """
    # Convert the Unix timestamp index to datetime in UTC and then to Europe/London timezone.
    london_index = pd.to_datetime(cumulative_series.index, unit='s', utc=True).tz_convert("Europe/London")
    
    # Create a copy and update its index.
    s = cumulative_series.copy()
    s.index = london_index
    
    # Calculate the difference between consecutive readings.
    # s.diff() calculates consumption with the difference appearing at the later timestamp.
    # shift(-1) moves these differences to the earlier timestamp, making the index refer to the start of the period.
    consumption = s.diff().shift(-1)
    
    # Drop the final NaN value, which does not represent a complete period.
    consumption = consumption.dropna()
    
    if resample_str:
        return consumption.resample(resample_str).sum()
    return consumption

class dynamoTable:
    
    def __init__(self, table_name: str):
        
        session = boto3.session.Session()
        self.dynamodb = session.resource('dynamodb')
        self.Table = session.resource('dynamodb').Table(table_name)

    def queryDynamo(self, pk: str, unix_start: int, unix_end: int) -> (dict, bool):
        
        # query the values for a specific topic, from queryTimestamp to present time. 
        kce = boto3.dynamodb.conditions.Key("PK").eq(pk) & boto3.dynamodb.conditions.Key("unixTimestamp").between(unix_start, unix_end) # key condition expression  
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

    def getTopicData(self, topic: str, unix_start: int, unix_end: int, freq: int) -> pd.Series:
        """
        topic: string, MQTT topic name
        unix_start: int, unix timestamp for start of query
        unix_end: int, unix timestamp for end of query
        freq: int, frequency of returned data in seconds
        
        df: pandas dataframe, index of unix timestamps, column of readings, column name is MQTT topic
        
        """
        
        items, empty_response = self.queryDynamo(topic, unix_start, unix_end)
        if empty_response:
            s = pd.Series(data=[np.nan], index=[unix_start], name=topic)
        else:
            # pull out values and timestamps from response
            vals = [np.float64(x['value_raw'].value.decode("utf-8")) for x in items] # decode from boto3 binary dtype, output list of str
            unix_ts = [int(x['unixTimestamp']) for x in items]
            s = pd.Series(data=vals, index=unix_ts, name=topic)     
        
        # bin data in regular intervals at chosen frequency - won't interpolate missing values!
        if freq:
            bins = [unix_start + freq*x for x in range(int((unix_end-unix_start)/freq)+1)]
            s = s.groupby(pd.cut(s.index, bins)).mean()
            s.index = bins[:-1]

        return s
    
    def getTopicsData(self, topic_list: list[str], unix_start: int, unix_end: int, freq: int) -> pd.DataFrame:
        """
        topic_list: list, MQTT topic name strings
        unix_start: int, unix timestamp for start of query
        unix_end: int, unix timestamp for end of query
        freq: int, frequency of returned data in seconds
        
        df: pandas dataframe, index of timestamps, each column is readings for all topics, column name is MQTT topic
        
        """

        list_of_topic_data = [self.getTopicData(topic, unix_start, unix_end, freq) for topic in topic_list]
        df = pd.concat(list_of_topic_data, axis=1)
        
        # convert unix timestamp to local london time
        df.index = pd.to_datetime(df.index, unit="s", origin="unix", utc=True).tz_convert("Europe/London")  

        return df
    
    def getEnergyMeters(self, topic_list: list[str], unix_start: int, unix_end: int, resample_interval: str) -> pd.DataFrame:
        """
        topic_list: MQTT topic names for energy meters
        unix_start: unix timestamp for start of query
        unix_end: unix timestamp for end of query
        resample_interval: resample frequency to pass to pandas resample function
        
        df: index of timestamps (refers to start of period), each column is readings for all topics, column name is MQTT topic
        
        """

        list_of_meter_data = [self.getTopicData(topic, unix_start, unix_end, freq=None) for topic in topic_list]
        list_of_kwh_meter_data = [calc_kwh(raw_meter, resample_interval) for raw_meter in list_of_meter_data]

        all_meters = pd.concat(list_of_kwh_meter_data, axis=1)
        all_meters.columns = topic_list
        
        return all_meters
    
    def getCopMonitoringData(self, device_id: str, unix_start: int, unix_end: int) -> pd.DataFrame:
        
        topic = f'kingslynn/gateway/hark.danfoss.adapter/data/{device_id}' # e.g. device_id = 'DT01_COP_1'
        items, empty_response = self.queryDynamo(topic, unix_start, unix_end) # items is a list of dicts, each dict contains sensor readings for one point in time
        
        if empty_response:
            controller_data_df = pd.DataFrame(data=[np.nan], index=[unix_start])
            
        else:
            data_at_each_timestep = []           
            for controller_reading in items: # controller_reading contains all data points for one timestep
                timestep_series = pd.Series(
                    controller_reading['value']['readings'],
                    name = int(controller_reading['unixTimestamp'])
                    )
                data_at_each_timestep.append(timestep_series)
            
            controller_data_df = pd.concat(
                data_at_each_timestep,
                axis=1
                ).T  

        controller_data_df.index = pd.to_datetime(controller_data_df.index, unit="s", origin="unix", utc=True).tz_convert("Europe/London")
        
        return controller_data_df
    
    
    def getRdmControllerData(self, controller_id: str, unix_start: int, unix_end: int, freq=int) -> pd.DataFrame:
        """" Query data for a controller connected to the RDM panel"""
        
        topic = f"rdm/2293SainsburysKingsLynnHardwick/subscription/{controller_id}"
        items, empty_response = self.queryDynamo(topic, unix_start, unix_end) # items is a list of dicts, each dict contains sensor readings for one point in time
        if empty_response:
            controller_data_df = pd.DataFrame(data=[np.nan], index=[unix_start])
            
        else:
            data_at_each_timestep = []           
            for controller_reading in items: # controller_reading contains all data points for one timestep
                point_values = [controller_reading['value']['readings'][x]['value'] for x in range(len(controller_reading['value']['readings']))]
                point_names = [controller_reading['value']['readings'][x]['name'] for x in range(len(controller_reading['value']['readings']))]
                point_units = [controller_reading['value']['readings'][x]['units'] for x in range(len(controller_reading['value']['readings']))]
                point_unix = int(controller_reading['unixTimestamp'])
                
                final_point_names = [f"{point_names[x]} ({point_units[x]})" for x in range(len(point_names))]
                
                timestep_series = pd.Series(
                    data = point_values,
                    index = final_point_names,
                    name = point_unix
                    )
                
                data_at_each_timestep.append(timestep_series)
                
            controller_data_df = pd.concat(
                data_at_each_timestep,
                axis=1
                ).T

        controller_data_df.index = pd.to_datetime(controller_data_df.index, unit="s", origin="unix", utc=True).tz_convert("Europe/London")
        
        return controller_data_df
      
    def get_n2ex_prices(self, unix_start: int, unix_end: int):
        
        items, empty_response = self.queryDynamo('nordpool_elec_price', unix_start, unix_end)
        if ~empty_response:
            prices = [float(items[x]['elec_price_GBP/MWh']) for x in range(len(items))]
            unix_timestamps = [int(items[x]['unixTimestamp']) for x in range(len(items))]
    
            return pd.Series(
                data = prices,
                index = pd.to_datetime(unix_timestamps, unit="s", origin="unix", utc=True).tz_convert("Europe/London"),
                name = 'n2ex_elec_price_£/MWh'
                )
        else:
            print('No data available for this time period.')
            return pd.Series(data=[np.nan], index=[unix_start], name = 'n2ex_elec_price_£/MWh')
    
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
        if pk == "weather" or pk == "weatherkit-weather":
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
        attributes : list of attributes to return, complete list can be found in "data collected in DynamoDB.xlsx".
        forecast_horizon : Number of forecast horizons to return.

        Returns
        -------
        df: Indexes are london timestamps for the beginning of the time period, columns are weather data and forecast values.

        """
        if pk == "weather" or pk == "weatherkit-weather":
            data_freq=int(60*60)
        elif pk == "carbon":
            data_freq=int(60*30)
        
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
        
        payload_dicts = []
        topic_names = []
        for batchKeys in batch_key_list:
            response = self.batchQuery(batchKeys)
            
            if len(response["UnprocessedKeys"]) != 0:
                print("Batch query failed to get all data, didn't retrieve %i keys" % len(response["UnprocessedKeys"]))
                print("Your code currently doesn't handle retrying to get these unprocessed keys - BE CAREFUL!")
            
            topic_names += [x["PK"] for x in response["Responses"][self.Table.table_name]] # list of topic strings
            payload_dicts += [json.loads(x["message"]) for x in response["Responses"][self.Table.table_name]] # list of mqtt payloads represented as dictionaries
            topic_vals = [x["ICL"] for x in payload_dicts]
            topic_unix_ts = [int(x["ICL_ts"]) for x in payload_dicts]
        
        df = pd.DataFrame(data = topic_vals, index=topic_names, columns=["Value"], dtype=np.float32)
        df["unixTimestamp"] = topic_unix_ts
        utc_timestamps = pd.to_datetime(topic_unix_ts, unit="s", origin="unix", utc=True)
        london_timestamps = utc_timestamps.tz_convert("Europe/London")
        df.insert(loc=0, column="Timestamp", value=london_timestamps) # convert UTC timestamp to local london time 
        return df.reindex(topics)
    
    
    
if __name__ == "__main__":
    """" Main function for testing purposes """
    
    dynamo_table = dynamoTable("test1")  
    unix_start = timestamp2unix("09:00 20/01/2025")
    unix_end = timestamp2unix("11:00 20/01/2025")
    
    device_id = 'DT01_COP_1'
    test = dynamo_table.getCopMonitoringData(device_id, unix_start, unix_end)
    
    
    
    
    # cabinet_id = "packCabinet/081"


    # raw_telemetry = dynamo_table.getRdmControllerData(cabinet_id, unix_start, unix_end)
    
    # unix_start = timestamp2unix("00:00 01/10/2022")
    # unix_end = timestamp2unix("00:00 08/11/2022")
    
    
    # controller_id = "packCabinet/003"
    
    # topic_list = [
    #     'kings-lynn/OS-23/sensor/cold-aisle-1-temp-C',
    #     'kings-lynn/OS-23/sensor/cold-aisle-2-temp-C',
    #     'kings-lynn/OS-23/sensor/supply-air-duct-tem-C',
    #     'kings-lynn/OS-23/sensor/return-air-duct-temp-C',
    #     'kings-lynn/OS-23/sensor/ave-sales-temp-C',
    #     'kings-lynn/OS-23/sensor/outside-air-temp-C',
    #     'kings-lynn/OS-23/sensor/elevation-enabled-bool',
    #     'kings-lynn/OS-23/sensor/supply-fan-1-speed-%',
    #     'kings-lynn/OS-23/sensor/heating-valve-openess-%',
    #     'kings-lynn/OS-23/sensor/elevation-on-off-bool',
    #     'kings-lynn/OS-23/sensor/elevation-always-on-bool',
    #     'kings-lynn/OS-23/sensor/calculated-supply-air-temp-sp-C',
    #     'kings-lynn/OS-23/sensor/diff-pressure-switch-bool',
    #     'kings-lynn/OS-23/sensor/gshp-supply-temp-C',
    #     'kings-lynn/OS-23/sensor/duct-low-temp-trip-bool',
    #     'kings-lynn/OS-23/sensor/duct-low-temp-latch-bool'
    #     ]
    
    # meter_list = [
    #     'kings-lynn/eict-meter/bakery-LV-kwh',
    #     'kings-lynn/eict-meter/geothermal-plant-kwh',
    #     'kings-lynn/eict-meter/fridge-DT1-kwh',
    #     'kings-lynn/eict-meter/frozen-food-kwh',
    #     'kings-lynn/eict-meter/customer-cafe-kwh',
    #     'kings-lynn/eict-meter/food-to-go-kwh',
    #     'kings-lynn/eict-meter/AHU1-kwh',
    #     'kings-lynn/eict-meter/AHU2-kwh',
    #     'kings-lynn/eict-meter/AHU3-kwh',
    #     'kings-lynn/eict-meter/fridge-DT2-kwh',
    #     'kings-lynn/eict-meter/coldstores-kwh',
    #     'kings-lynn/eict-meter/sales-area-ltg-kwh',
    #     'kings-lynn/eict-meter/ac-unit-DB-kwh',
    #     'kings-lynn/eict-meter/staff-kitchen-kwh',
    #     'kings-lynn/eict-meter/sales-busbar-kwh',
    #     'kings-lynn/eict-meter/car-park-ltg-kwh',
    #     'kings-lynn/eict-meter/domestic-ltg-kwh',
    #     'kings-lynn/eict-meter/bulkstock-kwh',
    #     'kings-lynn/eict-meter/shop-front-ltg-1-kwh',
    #     'kings-lynn/eict-meter/unloading-bay-kwh',
    #     'kings-lynn/eict-meter/main-meter-kwh',
    #     'kings-lynn/eict-meter/concession-unit-kwh',
    #     'kings-lynn/eict-meter/h-and-v-panel-kwh',
    #     'kings-lynn/eict-meter/solar-pv-kwh',
    #     'kings-lynn/eict-meter/petrol-station-PFS-kwh',
    #     'kings-lynn/eict-meter/GOL-kwh',
    #     'kings-lynn/eict-meter/GM-sales-ltg-kwh',
    #     'kings-lynn/eict-meter/GM-sales-area-feature-ltg-kwh',
    #     ]

    
    # cabinet_data = dynamo_table.getRdmControllerData(controller_id, unix_start, unix_end) # items is a list of dicts, each dict contains sensor readings for one point in time
    # bms_data = dynamo_table.getTopicsData(topic_list, unix_start, unix_end, freq=5*60)
    # meter_data = dynamo_table.getEnergyMeters(meter_list, unix_start, unix_end, resample_interval="15min")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    