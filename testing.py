# -*- coding: utf-8 -*-
"""
Created on Tue Jul  5 14:15:54 2022

@author: Max
"""
import ddbwrapper as dbw

table = dbw.dynamoTable("bmsTrial")
unix_start = dbw.timestamp2unix("01/01/2022")
unix_end = dbw.timestamp2unix("02/01/2022")

meter_list = [
    'kings-lynn/eict-meter/main-meter-kwh',
    'kings-lynn/eict-meter/concession-unit-kwh',
    'kings-lynn/eict-meter/h-and-v-panel-kwh',
    'kings-lynn/eict-meter/solar-pv-kwh',
    ]

topic_list = [
    'kings-lynn/OS-23/sensor/cold-aisle-1-temp-C',
    'kings-lynn/OS-23/sensor/cold-aisle-2-temp-C',
    'kings-lynn/OS-23/sensor/supply-air-duct-temp-C',
    'kings-lynn/OS-23/sensor/return-air-duct-temp-C',
    'kings-lynn/OS-23/sensor/ave-sales-temp-C',
]

topic_data = table.getTopicsData(topic_list, unix_start, unix_end, freq=15*60)
meter_data = table.getEnergyMeters(meter_list, unix_start, unix_end, resample_interval="60min")
weather_data = table.queryWeatherOrCarbon("weather", unix_start, unix_end, "apparentTemperature", forecast_horizon=5)
carbon_data = table.queryWeatherOrCarbon("carbon", unix_start, unix_end, "all", forecast_horizon=5)