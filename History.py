import os
from paho.mqtt import client as mqtt_client
import time
import asyncio
from Battery import Battery
from BatteryPool import BatteryPool
from Activation import Activation
import numpy as np
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
import random
from datetime import timedelta
import pandas as pd

load_dotenv('local.env')

# Database connection details (local database for testing)
battery_host = os.getenv("BATTERY_HOST")  
battery_port =  os.getenv("BATTERY_PORT")
battery_database = os.getenv("BATTERY_DATABASE")

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

class History:

    def create_Battery_dict_initial(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,current_time_ms_start_simulation):
        high_charge_indices = random.sample(range(number_of_battery), n_batt_charging)
        dict_Battery = {}
        for i in range(number_of_battery):
            id = i
            initial_soc = np.random.random()
            current_soc = initial_soc

            off_take_elec_active_power = 0
            feed_in_elec_power = 0
            house_consumed_power = 0
            generated_power = 0
            battery_charge_power = 100 if i in high_charge_indices else 0
            battery_discharge_power = 100 if i not in high_charge_indices else 0
            battery_discharge_command = 0

            dict_Battery[i] = Battery(id,capacity, current_soc, initial_soc, volt_nom, low_soc_threshold,high_soc_threshold,current_time_ms_start_simulation,max_power,
                                      off_take_elec_active_power,feed_in_elec_power,house_consumed_power,generated_power,battery_charge_power,battery_discharge_power,battery_discharge_command)
        return dict_Battery

    async def create_Battery_dict_history_1h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_ms_start_simulation):

        dict_Battery_h = {}
        def generate_history_range(current_time_ms_start_simulation):

            time_delta = timedelta(minutes=5)
            duration = timedelta(hours=1)
            history_range = []

            for i in range(13):
              timestamp = current_time_ms_start_simulation - time_delta * i
              if timestamp >= current_time_ms_start_simulation - duration:
                history_range.append(timestamp)

            # Invert order 
            if history_range:  
              history_range = list(reversed(history_range))
              history_range = history_range[:-1]  # Slice to exclude the last element (start time of simulation)

            return history_range

        history_range = generate_history_range(current_time_ms_start_simulation)

        for current_time_ms_start_simulation in history_range:

            high_charge_indices = random.sample(range(number_of_battery), n_batt_charging)

            for i in range(number_of_battery):
                id = i
                initial_soc = None
                current_soc = None

                off_take_elec_active_power = 0
                feed_in_elec_power = 0
                house_consumed_power = 0
                generated_power = 0
                battery_charge_power = 100 if i in high_charge_indices else 0
                battery_discharge_power = 100 if i not in high_charge_indices else 0
                battery_discharge_command = 0

                dict_Battery_h[i] = Battery(id,capacity, current_soc, initial_soc, volt_nom, low_soc_threshold,high_soc_threshold,current_time_ms_start_simulation,max_power,
                                          off_take_elec_active_power,feed_in_elec_power,house_consumed_power,generated_power,battery_charge_power,battery_discharge_power,battery_discharge_command)


            virtual_pool = BatteryPool(dict_Battery=dict_Battery_h)  # Create history battery pool
            await virtual_pool.publish_to_db(conn)

        return

    async def create_Battery_dict_history_act(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_ms_start_simulation):

        dict_Battery_h = {}
        def generate_history_range(current_time_ms_start_simulation):

            time_delta = timedelta(minutes=5)
            duration = timedelta(hours=1)
            history_range = []

            for i in range(13):
              timestamp = current_time_ms_start_simulation - time_delta * i
              if timestamp >= current_time_ms_start_simulation - duration:
                history_range.append(timestamp)

            # Invert order 
            if history_range:  
              history_range = list(reversed(history_range))
              history_range = history_range[:-1]  # Slice to exclude the last element (start time of simulation)

            return history_range

        history_range = generate_history_range(current_time_ms_start_simulation)

        for current_time_ms_start_simulation in history_range:

            high_charge_indices = random.sample(range(number_of_battery), n_batt_charging)

            for i in range(number_of_battery):
                id = i
                initial_soc = None
                current_soc = None

                off_take_elec_active_power = 0
                feed_in_elec_power = 0
                house_consumed_power = 0
                generated_power = 0
                battery_charge_power = 100 if i in high_charge_indices else 0
                battery_discharge_power = 100 if i not in high_charge_indices else 0
                battery_discharge_command = 1

                dict_Battery_h[i] = Battery(id,capacity, current_soc, initial_soc, volt_nom, low_soc_threshold,high_soc_threshold,current_time_ms_start_simulation,max_power,
                                          off_take_elec_active_power,feed_in_elec_power,house_consumed_power,generated_power,battery_charge_power,battery_discharge_power,battery_discharge_command)


            virtual_pool = BatteryPool(dict_Battery=dict_Battery_h)  # Create history battery pool
            await virtual_pool.publish_to_db(conn)

        return

    async def create_Battery_dict_history_2h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_ms_start_simulation):

        dict_Battery_h = {}
        def generate_history_range(current_time_ms_start_simulation):

            time_delta = timedelta(minutes=5)
            duration = timedelta(hours=2)
            history_range = []

            for i in range(25):
              timestamp = current_time_ms_start_simulation - time_delta * i
              if timestamp >= current_time_ms_start_simulation - duration:
                history_range.append(timestamp)

            # Invert order 
            if history_range:  
              history_range = list(reversed(history_range))
              history_range = history_range[:-1]  # Slice to exclude the last element (start time of simulation)

            return history_range

        history_range = generate_history_range(current_time_ms_start_simulation)

        for current_time_ms_start_simulation in history_range:

            high_charge_indices = random.sample(range(number_of_battery), n_batt_charging)

            for i in range(number_of_battery):
                id = i
                initial_soc = None
                current_soc = None

                off_take_elec_active_power = 0
                feed_in_elec_power = 0
                house_consumed_power = 0
                generated_power = 0
                battery_charge_power = 100 if i in high_charge_indices else 0
                battery_discharge_power = 100 if i not in high_charge_indices else 0
                battery_discharge_command = 0

                dict_Battery_h[i] = Battery(id,capacity, current_soc, initial_soc, volt_nom, low_soc_threshold,high_soc_threshold,current_time_ms_start_simulation,max_power,
                                          off_take_elec_active_power,feed_in_elec_power,house_consumed_power,generated_power,battery_charge_power,battery_discharge_power,battery_discharge_command)


            virtual_pool = BatteryPool(dict_Battery=dict_Battery_h)  # Create history battery pool
            await virtual_pool.publish_to_db(conn)

        return


    def create_db_engine(battery_host,battery_port,battery_database):
        conn = psycopg2.connect(host=battery_host, port=battery_port, dbname=battery_database)
        return conn


#----------MAIN----------# 
# 
# to test the functions

async def main():
    conn = History.create_db_engine(battery_host , battery_port , battery_database)
    # Create Battery Charging and Discharging history
    await History.create_Battery_dict_history_2h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_2h_primo)
    await History.create_Battery_dict_history_2h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_2h)
    await History.create_Battery_dict_history_act(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_act)
    await History.create_Battery_dict_history_1h(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,conn,current_time_ms_start_simulation)
    # Create initial battery dictionary
    Battery_dict = History.create_Battery_dict_initial(number_of_battery,low_soc_threshold,high_soc_threshold,volt_nom,capacity,max_power,current_time_ms_start_simulation)

    virtual_pool = BatteryPool(dict_Battery=Battery_dict)  # Create initial battery pool
    await virtual_pool.publish_to_db(conn)

if __name__=='__main__':
    asyncio.run(main())