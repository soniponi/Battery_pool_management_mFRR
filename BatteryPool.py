from Battery import Battery
import asyncio
import psycopg2


class BatteryPool:
    def __init__(self, dict_Battery):
        self.dict_pool = {}
        for Battery_id, Battery in dict_Battery.items():
            self.dict_pool[Battery_id] = Battery

    def get_all_battery_ids(self):
       
        return list(self.dict_pool.keys())
    
    async def publish_to_db(self,conn):
        for Battery in self.dict_pool.values():
            await Battery.insert_battery_data_ts(conn)
        
    async def get_latest_data(self, conn):
        tasks = []
        for Battery in self.dict_pool.values():
            #print(Battery.id)
            tasks.append(Battery.fetch_battery_data_ts(conn, Battery.id))  # Pass id 
        data = await asyncio.gather(*tasks)
        return data
    
    async def get_initial_data(self, conn,start_time):
        tasks = []
        for Battery in self.dict_pool.values():
            #print(Battery.id)
            tasks.append(Battery.fetch_initial_data(conn, Battery.id,start_time))  # Pass id 
        data = await asyncio.gather(*tasks)
        return data

    async def get_power_for_baseline(self, conn, start_time):
        data_list = []
        for Battery in self.dict_pool.values():
            battery_id = Battery.id  # Extract battery ID
            data = Battery.fetch_power_data_for_baseline(conn, battery_id, start_time)
            data_list.append(data)  # Append data directly (assuming immediate return)

        #print(data_list)  
        return data_list

                
            
