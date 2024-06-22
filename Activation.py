import numpy as np
import pandas as pd
import requests
import json
import os
import asyncio
from datetime import datetime, timedelta
import aiohttp
import re
import math

from Battery import Battery
from BatteryPool import BatteryPool

class Activation:

    async def get_hazelcast_data(url):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    response.raise_for_status()

                    # Check content type
                    content_type = response.headers.get('Content-Type')
                    if content_type != 'text/plain':
                        print(f"Unexpected content type for {url}: {content_type}")
                        return None

                    # Parse plain text (assuming specific format)
                    data_string = await response.text()
                    pattern = r'date_value":"(?P<date_value>.+?)\".*?"value":(?P<value>[-+]?\d+\.\d+)'                    
                    match = re.search(pattern, data_string)
                    if match:
                        return {
                            "date_value": (match.group("date_value")),
                            "value": float(match.group("value"))
                        }
                    else:
                        print("Failed to parse plain text data")
                    return None

            except aiohttp.ClientError as e:
                print(f"Error fetching data from {url}: {e}")
            return None

    async def chose_batteries(data_at_command_time,max_power,delta):

        # will return also if the delta is met or not by operating on the batteries

        # Sort battery data based on charge and discharge power
        sorted_data_charging = sorted(data_at_command_time, key=lambda x: (x.get('battery_charge_power'), -x.get('battery_soc')), reverse=False) # Battery data in ascending order based on charge power and descending order based on SOC when charge power is the same (to discharge the batteries with higher SOC first)
        #print("sorted_data_charging: ",sorted_data_charging)
        sorted_data_discharging = sorted(data_at_command_time, key=lambda x: (x.get('battery_discharge_power'),x.get('battery_soc')), reverse=False) # Battery data in ascending order based on discharge power and ascending order on SOC when discharge power is the same (to charge the batteries with lower SOC first)    

         
        def manage_battery(sorted_data_charging,sorted_data_discharging, delta,max_power):
            # Initialize empty lists with defined columns for storing results

            battery_to_stopcharge = []
            battery_to_stopdischarge = []
            battery_to_discharge = []
            battery_to_charge = [] #not used in this case --> to check
            

          
            if delta < 0: # Power injection
                delta = abs(delta) #just for calculation purposes
                # Iterate through each battery data point
                for battery in sorted_data_charging:
                  device_id = battery['device_id']
                  soc = battery['battery_soc']
                  charge_power = battery['battery_charge_power']

                  # Check if battery is charging and can be stopped 
                  if charge_power > 0 and delta >= 0:
                    delta -= charge_power
                    if delta >= 0:
                        battery_to_stopcharge.append({'device_id': device_id, 'new_charge_power': 0})
                    else:
                        battery_to_stopcharge.append({'device_id': device_id, 'new_charge_power': - delta})
                        break  # Stop iterating if delta is met
                    
                # Check if remaining delta requires discharging
                if delta > 0:
                  # Find a suitable battery for discharge based on SOC
                  for battery in sorted_data_charging:
                    device_id = battery['device_id']
                    soc = battery['battery_soc']
                    # Prioritize batteries with higher SOC for discharging
                    if delta - max_power > 0: # one battery is not enough to cover the remaining delta
                        battery_to_discharge.append({'device_id': device_id, 'new_discharge_power': max_power})
                    else: # one battery is enough to cover the remaining delta
                        battery_to_discharge.append({'device_id': device_id, 'new_discharge_power': delta})
                        break

                if delta > 0:
                    print(f"Power required is too high, only {len(battery_to_discharge)} batteries can be discharged to cover the remaining delta")
                else:
                    print(f"Power required is covered by stop charging {len(battery_to_stopcharge)} batteries and discharging {len(battery_to_discharge)} batteries")
                
                # Return the results dictionary
                return {
                    'battery_to_stopcharge': battery_to_stopcharge,
                    'battery_to_discharge': battery_to_discharge
                }

            elif delta > 0: # Power consumption

                # Iterate through each battery data point
                for battery in sorted_data_discharging:
                  device_id = battery['device_id']
                  soc = battery['battery_soc']
                  discharge_power = battery['battery_discharge_power']

                  # Check if battery is discharging and can be stopped 
                  if discharge_power > 0 and delta >= 0:
                    delta -= discharge_power
                    if delta >= 0:
                        battery_to_stopdischarge.append({'device_id': device_id, 'new_discharge_power': 0})
                    else:
                        battery_to_stopdischarge.append({'device_id': device_id, 'new_discharge_power': discharge_power + delta})
                        break  # Stop iterating if delta is met

                if delta > 0:
                    print(f"Power required is too high, only {len(battery_to_stopdischarge)} batteries can be stopped to cover the remaining delta")
                
                # Check if remaining delta requires charging --> not used in this case

                return {
                   'battery_to_stopdischarge': battery_to_stopdischarge
                }
        
        # Call the function with the sorted data and delta --> DEPENDING ON THE DELTA, DIFFERENT RESULTS!!
        result = manage_battery(sorted_data_charging,sorted_data_discharging, delta,max_power)

        return result

    async def discharge_battery(result, virtual_pool_upd, start_time,delta,power):

        print(result)
        
        discharged_batteries = {}
        async def update_battery(battery):
            battery_id = str(battery.id)

            if delta < 0: # Power injection, get the batteries to stop charging and eventually discharge
                battery_to_stop_charge = pd.DataFrame(result["battery_to_stopcharge"])
                battery_to_discharge = pd.DataFrame(result["battery_to_discharge"])
                print("battery_to_stop_charge: ",battery_to_stop_charge)
                print("battery_to_discharge: ",battery_to_discharge)
                analized = False

                if not battery_to_stop_charge.empty and battery_id in battery_to_stop_charge['device_id'].values:

                    if battery.is_high_soc(battery.current_soc):
                            
                        battery.current_time += timedelta(minutes=5)
                        battery.charge(0, start_time, power)
                        print(f"Battery {battery_id} SOC is high, stopping charge")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery
                        analized = True

                    else:
                    
                        battery.current_time += timedelta(minutes=5)
                        new_charge_power = float(battery_to_stop_charge[battery_to_stop_charge['device_id'] == battery_id]['new_charge_power'].iloc[0])
                        battery.charge(new_charge_power, start_time, power)
                        print(f"changing charge power of battery {battery_id} to {new_charge_power} Watts")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery  
                        analized = True

                if not battery_to_discharge.empty and battery_id in battery_to_discharge['device_id'].values :

                    # check if battery soc is low
                    if battery.is_low_soc(battery.current_soc):

                        battery.current_time += timedelta(minutes=5)
                        battery.discharge(0, start_time,power)
                        print(f"Battery {battery_id} SOC is low, stopping discharge")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery
                        analized = True  

                    else:

                        battery.current_time += timedelta(minutes=5)
                        new_discharge_power = float(battery_to_discharge[battery_to_discharge['device_id'] == battery_id]['new_discharge_power'].iloc[0])
                        battery.discharge(new_discharge_power, start_time,power)
                        print(f"Discharging battery {battery_id} at {new_discharge_power} Watts")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery
                        analized = True   

                if analized == False:

                    if battery.is_low_soc(battery.current_soc):

                        battery.current_time += timedelta(minutes=5)
                        battery.discharge(0, start_time,power)
                        print(f"Battery {battery_id} SOC is low, stopping discharge")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery

                    elif battery.is_high_soc(battery.current_soc):

                        battery.current_time += timedelta(minutes=5)
                        battery.charge(0, start_time,power)
                        print(f"Battery {battery_id} SOC is high, stopping charge")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery

                    else:

                        battery.current_time += timedelta(minutes=5)
                        battery.unchanged(start_time,power)
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery  # Unchanged battery

            elif delta > 0: # Power consumption, get the batteries to stop discharging

                battery_to_stop_discharge = pd.DataFrame(result["battery_to_stopdischarge"])

                if not battery_to_stop_discharge.empty and battery_id in battery_to_stop_discharge['device_id'].values :

                    if battery.is_low_soc(battery.current_soc):

                        battery.current_time += timedelta(minutes=5)
                        battery.discharge(0, start_time,power)
                        print(f"Battery {battery_id} SOC is low, stopping discharge")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery  

                    else:

                        battery.current_time += timedelta(minutes=5)
                        new_discharge_power = float(battery_to_stop_discharge[battery_to_stop_discharge['device_id'] == battery_id]['new_discharge_power'].iloc[0])
                        battery.discharge(new_discharge_power, start_time,power)
                        print(f"changing discharge power of battery {battery_id} to {new_discharge_power} Watts")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery  

                else: 

                    if battery.is_low_soc(battery.current_soc):

                        battery.current_time += timedelta(minutes=5)
                        battery.discharge(0, start_time,power)
                        print(f"Battery {battery_id} SOC is low, stopping discharge")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery

                    elif battery.is_high_soc(battery.current_soc):

                        battery.current_time += timedelta(minutes=5)
                        battery.charge(0, start_time,power)
                        print(f"Battery {battery_id} SOC is high, stopping charge")
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery

                    else:

                        battery.current_time += timedelta(minutes=5)
                        battery.unchanged(start_time,power)
                        battery.current_soc = battery.get_soc(battery.current_time)
                        discharged_batteries[battery_id] = battery  # Unchanged battery
            
            else:
                print("No power to be injected or consumed")

                if battery.is_low_soc(battery.current_soc):

                    battery.current_time += timedelta(minutes=5)
                    battery.discharge(0, start_time,power)
                    print(f"Battery {battery_id} SOC is low, stopping discharge")
                    battery.current_soc = battery.get_soc(battery.current_time)
                    discharged_batteries[battery_id] = battery

                elif battery.is_high_soc(battery.current_soc):

                    battery.current_time += timedelta(minutes=5)
                    battery.charge(0, start_time,power)
                    print(f"Battery {battery_id} SOC is high, stopping charge")
                    battery.current_soc = battery.get_soc(battery.current_time)
                    discharged_batteries[battery_id] = battery

                else:

                    battery.current_time += timedelta(minutes=5)
                    battery.unchanged(start_time,power)
                    battery.current_soc = battery.get_soc(battery.current_time)
                    discharged_batteries[battery_id] = battery  # Unchanged battery
                
                

        # Iterate through batteries
        tasks = [update_battery(battery) for battery in virtual_pool_upd.dict_pool.values()]
        await asyncio.gather(*tasks)
    
        return discharged_batteries

    async def compute_baseline_power(virtual_pool,conn,start_time):

        data = await virtual_pool.get_power_for_baseline(conn,start_time)
        
        flat_data = [item for sublist in data for item in sublist['data']]
        df = pd.DataFrame(flat_data, columns=["ts", "device_id", "net_power_flow","battery_discharge_command"])
        print(df)   

        df['ts'] = pd.to_datetime(df['ts'])

        # Sum 'net_power_flow'of each device by 'ts' 
        df_grouped = df.groupby('ts')['net_power_flow'].sum().reset_index(name='total_net_power_flow')
        print(df_grouped)

        # Compute baseline power as the average of the total net power flow in the last 2 hours
        baseline_power = df_grouped['total_net_power_flow'].mean()
        print(f"Baseline power: {baseline_power}")
        return baseline_power
    
#    def chose_batteries_no_base(battery_data,power,max_power):
#
#        n_batteries = min(50,math.ceil(power/max_power))
#
#        if math.ceil(power/max_power) > 50:
#            print("Power required is too high, choosing 50 batteries")
#        else:
#            print(f"Choosing {n_batteries} batteries")
#        
#        sorted_data = sorted(battery_data, key=lambda x: x.get('battery_soc'), reverse=True)     # Battery data in descending order based on SOC
#        chosen_ids = [battery.get('device_id') for battery in sorted_data[:n_batteries]]
#        print("discharge batteries: ",chosen_ids)
#        return chosen_ids

