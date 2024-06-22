import os
from paho.mqtt import client as mqtt_client
import time
import asyncio
import numpy as np
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
import random
from datetime import timedelta
import pandas as pd

from Battery import Battery
from BatteryPool import BatteryPool
from Activation import Activation
from History import History

load_dotenv('local.env')

# Database connection details (local database for testing)
battery_host = os.getenv("battery_host")
battery_port = os.getenv("battery_port")
battery_database = os.getenv("battery_database")

# url to get data from hazelcast (to change when we get the real ones from backend)
url_stg = os.getenv("url_stg") 


# Parameters to initialize
n_iterations = 3
number_of_battery = 5
n_batt_charging = 3

low_soc_threshold=0.2
high_soc_threshold=0.95
volt_nom = 3.7
capacity = 3000
max_power = 5000 #watt

current_time_ms_start_simulation = datetime.fromisoformat("2024-05-17 14:21:33.573")
current_time_act = current_time_ms_start_simulation - timedelta(hours=1)
current_time_2h = current_time_ms_start_simulation - timedelta(hours=2)
current_time_2h_primo = current_time_ms_start_simulation - timedelta(hours=4)



#----------MAIN----------#

async def main():

    conn = History.create_db_engine(battery_host , battery_port , battery_database)
    
    # Create Battery Charging and Discharging history
    await History.create_Battery_dict_history_2h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_2h_primo)
    await History.create_Battery_dict_history_2h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_2h)
    await History.create_Battery_dict_history_act(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_act)
    await History.create_Battery_dict_history_1h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_ms_start_simulation)
    
    # Create initial battery dictionary
    Battery_dict = History.create_Battery_dict_initial(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,current_time_ms_start_simulation)

    # Create initial battery pool and publish data on db
    virtual_pool = BatteryPool(dict_Battery=Battery_dict)  # Create initial battery pool
    await virtual_pool.publish_to_db(conn)                 # Publish data on db
    
    
    count = 0
    while count<n_iterations:
        # Reciving command from hazelcast
        response = await Activation.get_hazelcast_data(url_stg)
        power_real = response["value"]
        start_time_str = response["date_value"]
        start_time = datetime.fromisoformat(start_time_str)
        power = (power_real)*(1)
        print(start_time,power)

        # update initial_soc
        data_at_command = await virtual_pool.get_initial_data(conn,start_time)
        initial_soc = [battery["battery_soc"] for battery in data_at_command]

        # Fetching updated batteries data
        all_battery_upd = await virtual_pool.get_latest_data(conn)

        # Compute baseline power
        baseline_power = await Activation.compute_baseline_power(virtual_pool,conn,start_time)

        # convert all_battery_upd list to a dictionary
        dict_Battery_upd = Battery.create_Battery_dict_upd(all_battery_upd,capacity,initial_soc,volt_nom,low_soc_threshold,high_soc_threshold,max_power)
        # create a battery pool starting from all_battery_upd
        virtual_pool = BatteryPool(dict_Battery=dict_Battery_upd)

        # Compute measured power as the sum of the net power flow of all batteries at the start_time --> might be changed converting the initial_data in a dictionary of battery objects
        measured_power = sum((battery["battery_charge_power"]-battery["battery_discharge_power"]) for battery in data_at_command)
        print(measured_power)

        # Compute delta power required to reach the power setpoint
        delta = (baseline_power - measured_power) + power # power negative if injection in the grid, positive if consumption
        print(f"Delta: {delta}")

        # Choosing batteries to discharge considering the baseline 
        result = await Activation.chose_batteries(data_at_command,max_power,delta)

        # Discharging chosen batteries, return dictionary with all batteries updated
        Battery_upd = await Activation.discharge_battery(result, virtual_pool, start_time,delta,power)      
        
        # New Battery pool --> just to publish on db
        virtual_pool_p = BatteryPool(dict_Battery=Battery_upd)        
        # Inserting data from New Battery pool in the db --> just to publish on db
        await virtual_pool_p.publish_to_db(conn)      
        
        count +=1
        time.sleep(1)


if __name__=='__main__':
    asyncio.run(main())


