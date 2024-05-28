# -*- coding: utf-8 -*-
"""
Created on Wed May 19 12:43:13 2021

@author: maxbi
"""
from datetime import datetime
import pytz
import numpy as np

def timestamp2unix(strTime):
    """
    Takes in a string time which is in the current London timezone, and converts it to a unix time for querying DynamoDB

    Parameters
    ----------
    strTime : string,
        Timestamp in the format "%H:%M %d/%m/%Y"

    Returns
    -------
    integer,
        Unix time of the given strTime

    """

    unaware_dt = datetime.strptime(strTime, "%H:%M %d/%m/%Y") 
    timezone = pytz.timezone("Europe/London")
    london_dt = timezone.localize(unaware_dt)
    utc_dt = london_dt.astimezone(pytz.timezone("UTC")) # convert time to utc reading for querying dynamodb
    first_epoch = datetime(1970, 1, 1).replace(tzinfo=pytz.UTC)
    
    return int((utc_dt - first_epoch).total_seconds())

def timestamp2datetime(strTime):
    unaware_dt = datetime.strptime(strTime, "%H:%M %d/%m/%Y") 
    timezone = pytz.timezone("Europe/London")
    london_dt = timezone.localize(unaware_dt)
    
    return london_dt


def getDtypes(attributes, forecastHorizon):
    """
    Auxillary function to generate dictionary of datatypes for data queried from dynamo.

    Parameters
    ----------
    attributes : list,
        Attributes queried from dynamo.
    forecastHorizon : integer,
        Number of forecast horizons which have been queried.

    Returns
    -------
    attributeDtypes : dict,
        Dictionary to pass to dataframe to specify dtypes of all data queried.

    """
    dtypes = {
        "apparentTemperature": np.float64, # old darksky
        "temperatureApparent": np.float64, # new weatherkit
        "cloudCover": np.float64,
        "dewPoint": np.float64, # old darksky
        "temperatureDewPoint": np.float64, # new weatherkit
        "humidity": np.float64,
        "precipIntensity": np.float64, # old darksky
        "precipitationAmount": np.float64, # new weatherkit
        "precipProbability": np.float64, # old darksky
        "precipitationChance": np.float64, # new weatherkit
        "pressure": np.float64,
        "temperature": np.float64,
        "uvIndex": np.float64,
        "visibility": np.float64,
        "windBearing": np.float64,
        "windGust": np.float64,
        "windSpeed": np.float64,
        "windDirection": np.float64, 
        "carbonFactor": np.float64,
        "carbonIndex": str
        }   
    
    attributeDtypes = dict()
    attributeDtypes["unixTimestamp"] = np.int32
    for attribute in attributes:
        dtype = dtypes[attribute]
        for x in range(forecastHorizon+1):
            attributeDtypes[attribute + "_" + str(x)] = dtype    
    
    return attributeDtypes
