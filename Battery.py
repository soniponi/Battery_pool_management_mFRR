
import decimal
from datetime import datetime, timedelta


class Battery:
    def __init__(self, id, capacity, current_soc, initial_soc, nominal_voltage, low_soc_threshold,high_soc_threshold,current_time,
                 max_power,off_take_elec_active_power,feed_in_elec_power,house_consumed_power,generated_power,battery_charge_power,battery_discharge_power,battery_discharge_command):
        
        self.id = id
        self.capacity = capacity  # Battery capacity in Amp-hours (Ah)
        self.current_soc = current_soc  # State of charge (0.0 to 1.0)
        self.initial_soc = initial_soc
        self.nominal_voltage = nominal_voltage
        self.current_time = current_time
        self.start_command_time = None  # Time when the last discharge command was issued
        self.discharge_command = battery_discharge_command  # Stores the last discharge command (power)
        self.low_soc_threshold = low_soc_threshold
        self.high_soc_threshold = high_soc_threshold
        self.max_power = max_power
        self.off_take_elec_active_power = off_take_elec_active_power
        self.feed_in_elec_power = feed_in_elec_power
        self.house_consumed_power = house_consumed_power
        self.generated_power = generated_power
        self.battery_charge_power = battery_charge_power
        self.battery_discharge_power = battery_discharge_power
        self.net_power_flow = self.battery_charge_power - self.battery_discharge_power
        # define weather the battery is discharging or charging based on the value of self.net_power_flow
        self.discharging = True if self.net_power_flow < 0 else False
        self.charging = True if self.net_power_flow > 0 else False
        self.power = - self.net_power_flow 

    def discharge(self, new_discharge_power , start_time, power_hazelcast):
        self.power = new_discharge_power  # Power being drawn from the battery (Watts)
        self.battery_discharge_power = new_discharge_power
        self.discharging = True
        self.charge = False
        self.start_command_time = start_time
        self.discharge_command = power_hazelcast  # Update last discharge command

    def charge(self, new_charge_power, start_time, power_hazelcast):
        self.power = - new_charge_power
        self.battery_charge_power = new_charge_power
        self.charging = True
        self.discharging = False
        self.start_command_time = start_time
        self.discharge_command = power_hazelcast  # Update last discharge command

    def unchanged(self,start_time,power_hazelcast):
        self.power = - self.net_power_flow
        self.start_command_time = start_time
        self.discharging = True if self.net_power_flow < 0 else False
        self.charging = True if self.net_power_flow > 0 else False
        self.discharge_command = power_hazelcast  # Update last discharge command

    def get_soc(self, current_time_ms):

        elapsed_time = (current_time_ms - self.start_command_time).total_seconds() / (3600)
        discharge_current = self.power / self.nominal_voltage
        discharged_capacity = discharge_current * elapsed_time
        self.current_soc = max(0, self.initial_soc - discharged_capacity / self.capacity)
        return self.current_soc

    def is_low_soc(self, up_soc):
        return up_soc <= self.low_soc_threshold
    
    def is_high_soc(self, up_soc):
        return up_soc >= self.high_soc_threshold
    
    async def insert_battery_data(self,conn):

        insert_query = """INSERT INTO BatteryData (id,Soc,Soc_initial, CommandId, startTime) VALUES (%s,%s,%s, %s, %s)"""
        cursor = conn.cursor()
        cursor.execute(insert_query, (self.id,self.current_soc,self.initial_soc, self.discharge_command,self.current_time))
        conn.commit()  
        print(f"Battery data inserted successfully!")

    async def fetch_battery_data(self,conn,Battery_id):

        fetch_query = f"""SELECT * FROM BatteryData WHERE id = %s ORDER BY starttime DESC LIMIT 1 ;"""  
        cursor = conn.cursor()
        cursor.execute(fetch_query,(Battery_id,))
        row = cursor.fetchone()
        if row:
            return {"id":row[0],"Soc":row[1],"Soc_initial":row[2], "CommandId":row[3], "startTime":row[4]}
        else:
            return None  # No data found
        
    async def initialize_battery(id, capacity, current_soc, initial_soc, volt_nom, low_soc_threshold, current_time):
    
        battery = Battery(id, capacity, current_soc, initial_soc, volt_nom, low_soc_threshold,current_time)
        return battery

    async def insert_battery_data_ts(self,conn):

        insert_query = """INSERT INTO metrics_inverter (ts , portfolio_id , home_id , device_id , provider , device_type , battery_soc , 
                                                        off_take_elec_active_power , feed_in_elec_power , house_consumed_power , generated_power , 
                                                        battery_charge_power , battery_discharge_power , battery_discharge_command) VALUES (%s,%s,%s,%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor = conn.cursor()
        cursor.execute(insert_query, (self.current_time,self.id,self.id,self.id,"Producer","Battery",self.current_soc,self.off_take_elec_active_power,
                                      self.feed_in_elec_power,self.house_consumed_power,self.generated_power,
                                      self.battery_charge_power,self.battery_discharge_power,self.discharge_command))
        conn.commit()  
        #print(f"Battery data inserted successfully!")

    async def fetch_battery_data_ts(self,conn,Battery_id):

        fetch_query = f"""SELECT * FROM metrics_inverter WHERE device_id = CAST(%s AS VARCHAR) ORDER BY ts DESC LIMIT 1 ;"""  
        cursor = conn.cursor()
        cursor.execute(fetch_query,(Battery_id,))
        row = cursor.fetchone()
        if row:
            return {"ts" : row[0], "portfolio_id" : row[1], "home_id" : row[2], "device_id" : row[3], 
                    "provider" : row[4], "device_type" : row[5], "battery_soc" : row[6], "off_take_elec_active_power" : row[7],
                    "feed_in_elec_power" : row[8],"house_consumed_power" : row[9],"generated_power" : row[10],
                    "battery_charge_power" : row[11],"battery_discharge_power" : row[12],"battery_discharge_command" : row[13]}
        else:
            return None  # No data found
        
    async def fetch_initial_data(self,conn,Battery_id,start_time):

        fetch_query = f"""SELECT * FROM metrics_inverter WHERE device_id = CAST(%s AS VARCHAR) and ts = '{start_time}' LIMIT 1 ;"""  
        cursor = conn.cursor()
        cursor.execute(fetch_query,(Battery_id,))
        row = cursor.fetchone()
        if row:
            return { "ts" : row[0], "portfolio_id" : row[1], "home_id" : row[2], "device_id" : row[3], 
                    "provider" : row[4], "device_type" : row[5], "battery_soc" : row[6], "off_take_elec_active_power" : row[7],
                    "feed_in_elec_power" : row[8],"house_consumed_power" : row[9],"generated_power" : row[10],
                    "battery_charge_power" : row[11],"battery_discharge_power" : row[12],"battery_discharge_command" : row[13]}
        else:
            return None  # No data found
        
    # convert all_battery_upd list to a dictionary of battery objects
    def create_Battery_dict_upd(all_battery_upd,capacity,initial_soc,nominal_voltage,low_soc_threshold,high_soc_threshold,max_power):
        
        all_battery_upd_dict = {}
        for i in range(len(all_battery_upd)):

            all_battery_upd_dict[i] = Battery(
                all_battery_upd[i]["device_id"],
                capacity,
                all_battery_upd[i]["battery_soc"],
                initial_soc[i],
                nominal_voltage,
                low_soc_threshold,
                high_soc_threshold,
                all_battery_upd[i]["ts"],
                max_power,
                all_battery_upd[i]["off_take_elec_active_power"],
                all_battery_upd[i]["feed_in_elec_power"],
                all_battery_upd[i]["house_consumed_power"],
                all_battery_upd[i]["generated_power"],
                all_battery_upd[i]["battery_charge_power"],
                all_battery_upd[i]["battery_discharge_power"],
                all_battery_upd[i]["battery_discharge_command"]
            )
        return all_battery_upd_dict
    
    def fetch_power_data_for_baseline(self,conn,Battery_id,start_time):
        
        # Query to find avg power per each battery_id every 15 minutes in the last 2 hours(before the start_time)
        # Sum the avg power of each device_id for each 15 minutes time interval 
        # Compute the average between the 8 slots of 15 minutes

        fetch_query = f"""SELECT ts, device_id , battery_charge_power - battery_discharge_power as net_power_flow,battery_discharge_command
                            FROM metrics_inverter
                            WHERE device_id  = '{Battery_id}'
                            and ts < '{start_time}'
                            and ts >= '{start_time - timedelta(hours=2)}'      
                            ;"""  
        
        
        cursor = conn.cursor()
        cursor.execute(fetch_query)
        rows = cursor.fetchall()
        #print(rows)  
        if rows:
            
            return {"data": rows}
        else:
            return None  # No data found
       

    


