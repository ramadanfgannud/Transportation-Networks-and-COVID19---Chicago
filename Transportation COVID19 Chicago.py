#!/usr/bin/env python
# coding: utf-8

# In[4]:


#Ramadan Gannud 
# 2020 / 2021
#Transportation Networks and COVID19 in Chicago


import os
import time
import datetime
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3
conn = sqlite3.connect('Transportation.db') # open the connection
cursor = conn.cursor()


# In[3]:


#Time in Python
d = datetime.datetime.strptime('01/01/2019', '%d/%m/%Y')

print(d.strftime('%a %b %d,%Y'))
print(d.strftime('%Y-%m-%d'))


# In[1]:


#Time in SQLite database should be 'YYYY-mm-DD HH:MM:SS' so 

print(cursor.execute('select strftime("%S","2018-10-11 11:45:00")').fetchall())
print(cursor.execute('select strftime("%M","2018-10-11 11:45:00")').fetchall())
print(cursor.execute('select strftime("%H","2018-10-11 11:45:00")').fetchall())
print(cursor.execute('select strftime("%d","2018-10-11 11:45:00")').fetchall())
print(cursor.execute('select strftime("%m","2018-10-11 11:45:00")').fetchall())
print(cursor.execute('select strftime("%H","2018-10-11 11:45:00")').fetchall())
print(cursor.execute('SELECT strftime("%Y-%m-%d","2018-10-11 11:45:00");').fetchall())
print(cursor.execute('SELECT strftime("%m","2018-10-11");').fetchall())
print(cursor.execute('SELECT strftime("%Y","2019-10-11");').fetchall())


# In[204]:


#SCHEMA

#Transportation Network Providers - Trips (Nov, 2018 - May 27, 2020) (159M Rows) (21 Columns):
TNPTrips = '''CREATE TABLE TNPTrips (Trip_ID TEXT PRIMARY KEY NOT NULL, 
                Trip_Start_Timestamp TEXT,
                Trip_End_Timestamp TEXT, 
                Trip_Seconds INT, 
                Trip_Miles INT, 
                Pickup_Census_Tract TEXT, 
                Dropoff_Census_Tract TEXT, 
                Pickup_Community_Area INT, 
                Dropoff_Community_Area INT, 
                Fare NUMERIC, 
                Tip INT, 
                Additional_Charges NUMERIC, 
                Trip_Total NUMERIC, 
                Shared_Trip_Authorized TEXT, 
                Trips_Pooled INT,
                Pickup_Centroid_Latitude REAL, 
                Pickup_Centroid_Longitude REAL, 
                Pickup_Centroid_Location TEXT,
                Dropoff_Centroid_Latitude REAL, 
                Dropoff_Centroid_Longitude REAL, 
                Dropoff_Centroid_Location TEXT);'''

#Transportation Network Providers - Vehicles (2015 - May 27, 2020) (6.66M Rows) (9 Columns):
TNPVehicles = '''CREATE TABLE TNPVehicles (MONTH_REPORTED TEXT NOT NULL, 
                    STATE TEXT, 
                    MAKE TEXT, 
                    MODEL TEXT, 
                    COLOR TEXT, 
                    YEAR INT, 
                    LAST_INSPECTION_MONTH TEXT, 
                    NUMBER_OF_TRIPS INT,
                    MULTIPLE_TNPS TEXT);'''

#Transportation Network Providers - Drivers (2015 - May 27, 2020) (6.4M Rows) (7 Columns):
TNPDrivers = '''CREATE TABLE TNPDrivers (MONTH_REPORTED TEXT NOT NULL, 
                    DRIVER_START_MONTH TEXT, 
                    CITY TEXT,
                    STATE TEXT, 
                    ZIP INT, 
                    NUMBER_OF_TRIPS INT, 
                    MULTIPLE_TNPS TEXT);'''

#Taxi Trips - (2019 - 07-31-2020) ((16.5 M + 2.98M) Rows) (23 Columns):
TaxiTrips = '''CREATE TABLE TaxiTrips (Trip_ID TEXT PRIMARY KEY NOT NULL,
                    Taxi_ID TEXT,
                    Trip_Start_Timestamp TEXT,
                    Trip_End_Timestamp TEXT,
                    Trip_Seconds INT,
                    Trip_Miles NUMERIC,
                    Pickup_Census_Tract REAL,
                    Dropoff_Census_Tract REAL,
                    Pickup_Community_Area INT,
                    Dropoff_Community_Area INT,
                    Fare NUMERIC,
                    Tips NUYMERIC,
                    Tolls NUMERIC,
                    Extras NUMERIC,
                    Trip_Total NUMERIC,
                    Payment_Type TEXT,
                    Company TEXT,
                    Pickup_Centroid_Latitude REAL,
                    Pickup_Centroid_Longitude REAL,
                    Pickup_Centroid_Location TEXT,
                    Dropoff_Centroid_Latitude REAL,
                    Dropoff_Centroid_Longitude REAL,
                    Dropoff_Centroid_Location TEXT);'''

#Traffic Crashes - Crashes (2015 to (08-24-2020)) (433,208 Rows) (49 Columns):
Crashes = '''CREATE TABLE Crashes (CRASH_RECORD_ID TEXT PRIMARY KEY NOT NULL, 
                    RD_NO TEXT, 
                    CRASH_DATE_EST_I TEXT, 
                    CRASH_DATE TEXT, 
                    POSTED_SPEED_LIMIT INT, 
                    TRAFFIC_CONTROL_DEVICE TEXT, 
                    DEVICE_CONDITION TEXT, 
                    WEATHER_CONDITION TEXT, 
                    LIGHTING_CONDITION TEXT, 
                    FIRST_CRASH_TYPE TEXT, 
                    TRAFFICWAY_TYPE TEXT, 
                    LANE_CNT INT, 
                    ALIGNMENT TEXT, 
                    ROADWAY_SURFACE_COND TEXT, 
                    ROAD_DEFECT TEXT, 
                    REPORT_TYPE TEXT, 
                    CRASH_TYPE TEXT, 
                    INTERSECTION_RELATED_I TEXT, 
                    NOT_RIGHT_OF_WAY_I TEXT, 
                    HIT_AND_RUN_I TEXT, 
                    DAMAGE TEXT, 
                    DATE_POLICE_NOTIFIED TEXT, 
                    PRIM_CONTRIBUTORY_CAUSE TEXT, 
                    SEC_CONTRIBUTORY_CAUSE TEXT, 
                    STREET_NO INT, 
                    STREET_DIRECTION TEXT, 
                    STREET_NAME TEXT, 
                    BEAT_OF_OCCURRENCE INT, 
                    PHOTOS_TAKEN_I TEXT, 
                    STATEMENTS_TAKEN_I TEXT, 
                    DOORING_I TEXT, 
                    WORK_ZONE_I TEXT, 
                    WORK_ZONE_TYPE TEXT, 
                    WORKERS_PRESENT_I TEXT, 
                    NUM_UNITS INT, 
                    MOST_SEVERE_INJURY TEXT, 
                    INJURIES_TOTAL INT, 
                    INJURIES_FATAL INT, 
                    INJURIES_INCAPACITATING INT, 
                    INJURIES_NON_INCAPACITATING INT, 
                    INJURIES_REPORTED_NOT_EVIDENT INT,
                    INJURIES_NO_INDICATION INT, 
                    INJURIES_UNKNOWN INT, 
                    CRASH_HOUR INT CHECK(CRASH_HOUR < 25 AND CRASH_HOUR >= 0), 
                    CRASH_DAY_OF_WEEK INT CHECK(CRASH_DAY_OF_WEEK < 8 AND CRASH_DAY_OF_WEEK  >= 1), 
                    CRASH_MONTH INT CHECK(CRASH_MONTH < 13 AND CRASH_MONTH  >= 1), 
                    LATITUDE REAL, 
                    LONGITUDE REAL, 
                    LOCATION TEXT);'''

#Traffic Crashes - People (2015 to present) (944K Rows) (30 Columns):
CrashesPeople = '''CREATE TABLE CrashesPeople (PERSON_ID TEXT, 
                        PERSON_TYPE TEXT, 
                        CRASH_RECORD_ID TEXT, 
                        RD_NO TEXT, 
                        VEHICLE_ID TEXT,
                        CRASH_DATE TEXT,
                        SEAT_NO INT, 
                        CITY TEXT, 
                        STATE TEXT, 
                        ZIPCODE INT, 
                        SEX TEXT, 
                        AGE INT, 
                        DRIVERS_LICENSE_STATE, 
                        DRIVERS_LICENSE_CLASS TEXT, 
                        SAFETY_EQUIPMENT TEXT,
                        AIRBAG_DEPLOYED TEXT, 
                        EJECTION TEXT, 
                        INJURY_CLASSIFICATION TEXT, 
                        HOSPITAL TEXT, 
                        EMS_AGENCY TEXT, 
                        EMS_RUN_NO TEXT, 
                        DRIVER_ACTION TEXT, 
                        DRIVER_VISION TEXT, 
                        PHYSICAL_CONDITION TEXT, 
                        PEDPEDAL_ACTION TEXT, 
                        PEDPEDAL_VISIBILITY TEXT, 
                        PEDPEDAL_LOCATION TEXT, 
                        BAC_RESULT TEXT, 
                        BAC_RESULT_VALUE NUMERIC, 
                        CELL_PHONE_USE TEXT);'''

#Hospitals:
# 7596 Hospitals in 3373 cities, 57 states, 5640 zip codes, and 1605 counties
# 7335 Names, 867 ALT_NAME
Hospitals = '''CREATE TABLE Hospitals (OBJECTID INT ,
                    ID TEXT PRIMARY KEY NOT NULL,
                    NAME TEXT,
                    ADDRESS TEXT,
                    CITY TEXT,
                    STATE TEXT,
                    ZIP INT,
                    TYPE TEXT,
                    STATUS TEXT,
                    POPULATION INT,
                    COUNTY TEXT,
                    COUNTYFIPS INT,
                    COUNTRY TEXT,
                    LATITUDE REAL,
                    LONGITUDE REAL,
                    NAICS_CODE INT,
                    NAICS_DESC TEXT,
                    SOURCE TEXT,
                    SOURCEDATE TEXT,
                    VAL_METHOD TEXT,
                    VAL_DATE TEXT,
                    WEBSITE TEXT,
                    STATE_ID TEXT,
                    ALT_NAME TEXT,
                    ST_FIPS TEXT,
                    OWNER TEXT,
                    TTL_STAFF INT,
                    BEDS INT,
                    TRAUMA INT,
                    HELIPAD INT);'''


# In[209]:


#Populate Transportation data

import sys
from io import StringIO
from tokenize import generate_tokens
xrange = range

#parts(s) function splits on comma and ignores commas within double qoutes
def parts(s):  
    """Split a python-tokenizable expression on comma operators"""
    compos = [-1] # compos stores the positions of the relevant commas in the argument string
    compos.extend(t[2][1] for t in generate_tokens(StringIO(s).readline) if t[1] == ',')
    compos.append(len(s))
    return [ s[compos[i]+1:compos[i+1]] for i in xrange(len(compos)-1)]


def populate(file,Bsize):
    fd = open(file, 'r',encoding="utf8")
    line = fd.readline()   #To read Header
    Batch = []
    loadCounter = 0
    excuted = 0
    
    if file == 'Transportation_Network_Providers_-_Trips_-_2020.csv':
        insert = "INSERT INTO TNPTrips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    
    elif file == 'Transportation_Network_Providers_Vehicles.csv':
        insert = "INSERT INTO TNPVehicles VALUES (?,?,?,?,?,?,?,?,?);"
    
    elif file == 'Transportation_Network_Providers_Drivers.csv':
        insert = "INSERT INTO TNPDrivers VALUES (?,?,?,?,?,?,?);"
    
    elif file == 'Taxi_Trips_2019.csv' or file == 'Taxi_Trips_2020.csv':
        insert = "INSERT INTO TaxiTrips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    
    elif file == 'Traffic_Crashes_-_Crashes.csv':
        insert = "INSERT INTO Crashes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    
    elif file == 'Traffic_Crashes_People.csv':
        insert = "INSERT INTO CrashesPeople VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    
    
    while line:
        line = fd.readline()
        values = parts(line)
        values[-1] = values[-1][:-1]   #Removes \n from the last column value
        Batch.append(values)
        
        loadCounter = loadCounter + 1
        excuted = excuted + 1

        if loadCounter == Bsize:
            cursor.executemany(insert, Batch);
            print('{} EXECUTED'.format(excuted))
            
            loadCounter = 0
            Batch = []
            
    #Excute batches left that are less than Bsize
    cursor.executemany(insert, Batch);
    fd.close()
    


# In[27]:


#populate hospitals

def populate_Hospitals(Bsize):
    fd = open('Hospitals.csv', 'r',encoding="utf8")
    line = fd.readline()   #To read Header
    Batch = []
    loadCounter = 0
    excuted = 0

    insert = "INSERT INTO Hospitals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    while line:
        line = fd.readline()
        values = parts(line)

        if len(values) > 1:
            del (values[0:2])
            del (values[7:9])
            values[-1] = values[-1][:-1]
            Batch.append(values)

        loadCounter = loadCounter + 1
        excuted = excuted + 1
        if loadCounter == Bsize:
            cursor.executemany(insert, Batch);
            print('{} EXECUTED'.format(excuted))

            loadCounter = 0
            Batch = []
            
    #Excute batches left that are less than Bsize
    cursor.executemany(insert, Batch);
    fd.close()

    
#cursor.execute('DROP TABLE IF EXISTS Hospitals;')
#cursor.execute(Hospitals)
#populate_Hospitals(1000)


# In[4]:


cursor.execute('SELECT COUNT(*) FROM Hospitals').fetchall()


# In[221]:


#Add new records after June 2020 to TNPTrips

Bsize = 1000
fd = open('Transportation_Network_Providers_-_Trips_-_2020.csv', 'r',encoding="utf8")
line = fd.readline()   #To read Header
Batch = []
loadCounter = 0
excuted = 0

insert = "INSERT OR IGNORE INTO TNPTrips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
while line:
    line = fd.readline()
    l = line.split(',')
    
    if len(l) > 1:
        if int(l[1][6:10]) == 2020 and int(l[1][:2]) > 6:   
            values = parts(line)

            values[-1] = values[-1][:-1]   #Removes \n from the last column value
            values.append('CHICAGO')
            Batch.append(values)

            loadCounter = loadCounter + 1
            excuted = excuted + 1

            if loadCounter == Bsize:
                cursor.executemany(insert, Batch);
                print('{} EXCUTED'.format(excuted))

                loadCounter = 0
                Batch = []
            
#Excute batches left that are less than Bsize
cursor.executemany(insert, Batch);
fd.close()


# In[202]:


#Dates included in TNPTrips
cursor.execute('''select distinct(substr(Trip_Start_Timestamp,1,10)) from TNPTrips''').fetchall()


# In[200]:


#Add new records after June 2020 to TaxiTrips

Bsize = 1000
fd = open('Taxi_Trips_-_2020.csv', 'r',encoding="utf8")
line = fd.readline()   #To read Header
Batch = []
loadCounter = 0
excuted = 0

insert = "INSERT OR IGNORE INTO TaxiTrips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
while line:
    line = fd.readline()
    l = line.split(',')
    
    if int(l[2][6:10]) == 2020 and int(l[2][:2]) > 7:   
        values = parts(line)
        values[-1] = values[-1][:-1]   #Removes \n from the last column value
        Batch.append(values)
        
        loadCounter = loadCounter + 1
        excuted = excuted + 1

        if loadCounter == Bsize:
            cursor.executemany(insert, Batch);
            print('{} EXECUTED'.format(excuted))

            loadCounter = 0
            Batch = []
            
#Excute batches left that are less than Bsize
cursor.executemany(insert, Batch);
fd.close()
    
    
    


# In[201]:


#Dates included in TaxiTrips
cursor.execute('''select distinct(substr(Trip_Start_Timestamp,1,10)) from TaxiTrips''').fetchall()


# In[207]:


#Populate Crashes
# 458,336 rows

cursor.execute('DROP TABLE IF EXISTS Crashes;')
cursor.execute(Crashes)
populate('Traffic_Crashes_-_Crashes.csv',1000)


# In[208]:


cursor.execute('SELECT COUNT(*) FROM Crashes').fetchall()


# In[415]:


#Populate CrashesPeople
# 947,776 rows

#cursor.execute('DROP TABLE IF EXISTS CrashesPeople;')
#cursor.execute(CrashesPeople)
#populate('Traffic_Crashes_People.csv',1000)


# In[416]:


cursor.execute('SELECT COUNT(*) FROM CrashesPeople').fetchall()


# In[460]:


#Populate TNPVehicles
# 6,664,757 rows

#cursor.execute('DROP TABLE IF EXISTS TNPVehicles;') 
#cursor.execute(TNPVehicles)
#populate('Transportation_Network_Providers_Vehicles.csv',1000)


# In[94]:


cursor.execute('SELECT COUNT(*) FROM TNPVehicles').fetchall()


# In[458]:


#Populate TNPDrivers
# 6,402,880 rows

#cursor.execute('DROP TABLE IF EXISTS TNPDrivers;') 
#cursor.execute(TNPDrivers)
#populate('Transportation_Network_Providers_Drivers.csv',1000)


# In[95]:


cursor.execute('SELECT COUNT(*) FROM TNPDrivers').fetchall()


# In[462]:


#Populate TaxiTrips
# 3,113,154 (07-31-2020)
# 16,477,365 (2019)
# 19,590,519 (2019 - 2020)

#cursor.execute('DROP TABLE IF EXISTS TaxiTrips;') 
#cursor.execute(TaxiTrips)
#populate('Taxi_Trips_2020.csv',1000)


# In[464]:


#populate('Taxi_Trips_2019.csv',1000)


# In[96]:


cursor.execute('SELECT COUNT(*) FROM TaxiTrips').fetchall()


# In[466]:


#Populate TNPTrips
# 158,613,604

#cursor.execute('DROP TABLE IF EXISTS TNPTrips;')
#cursor.execute(TNPTrips)
#populate('Transportation_Network_Providers_Trips.csv',1000)


# In[8]:


cursor.execute('SELECT COUNT(*) FROM TNPTrips').fetchall()


# In[229]:


#Number of daily Crashes (2019) - (11-27-2020)

daily_crashes = cursor.execute('''SELECT substr(CRASH_DATE,1,10) AS DATE, COUNT(*) FROM Crashes
                                    WHERE substr(CRASH_DATE,7,4) > '2017'
                                    AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY (DATE)''').fetchall()

daily_crashes = pd.DataFrame(daily_crashes, columns = ['date','crashes'])
daily_crashes.to_csv('daily_crashes.csv',index=False)
daily_crashes


# In[231]:


monthly_crashes = cursor.execute('''SELECT  substr(CRASH_DATE,1,2) AS MONTH, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) 
                                    FROM Crashes
                                    WHERE substr(CRASH_DATE,7,4) > '2017'
                                    AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY YEAR, month
                                    ORDER BY MONTH, YEAR''').fetchall()

monthly_crashes = pd.DataFrame(monthly_crashes, columns = ['month','year','crashes'])
monthly_crashes.to_csv('monthly_crashes.csv',index=False)
monthly_crashes


# In[232]:


yearly_crashes = cursor.execute(''' SELECT substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27' 
                                    GROUP BY YEAR''').fetchall()

yearly_crashes = pd.DataFrame(yearly_crashes, columns = ['year','crashes'])
yearly_crashes


# In[233]:


crashes_day = cursor.execute('''SELECT  substr(CRASH_DATE,4,2) AS DAY, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' 
                                    AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY YEAR, DAY
                                    ORDER BY YEAR''').fetchall()
crashes_day = pd.DataFrame(crashes_day, columns = ['day','year','crashes'])
crashes_day.to_csv('crashes_day.csv',index=False)
crashes_day


# In[234]:


lighting_condition = cursor.execute('''SELECT LIGHTING_CONDITION, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) 
                                    FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY LIGHTING_CONDITION, YEAR
                                    ''').fetchall()

lighting_condition = pd.DataFrame(lighting_condition, columns = ['lighting_condition','year','crashes'])
lighting_condition['crashes_percentage'] = lighting_condition.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1) 
lighting_condition.to_csv('lighting_condition.csv',index=False)
lighting_condition


# In[235]:


hit_and_run = cursor.execute('''SELECT HIT_AND_RUN_I, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) 
                                    FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY HIT_AND_RUN_I, YEAR
                                    ''').fetchall()

hit_and_run = pd.DataFrame(hit_and_run, columns = ['hit_and_run','year','crashes'])
hit_and_run['crashes_percentage'] = hit_and_run.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1)
hit_and_run.to_csv('hit_and_run.csv',index=False)
hit_and_run


# In[236]:


monthly_hit_and_run = cursor.execute('''SELECT HIT_AND_RUN_I, substr(CRASH_DATE,1,2) AS MONTH, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) 
                                    FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY HIT_AND_RUN_I, MONTH, YEAR
                                    ''').fetchall()

monthly_hit_and_run = pd.DataFrame(monthly_hit_and_run, columns = ['hit_and_run','month','year','crashes'])
monthly_hit_and_run['crashes_percentage'] = monthly_hit_and_run.apply(lambda row: row.crashes*100/ int(monthly_crashes.loc[(monthly_crashes['year'] == row.year) & (monthly_crashes['month'] == row.month)]['crashes']) , axis = 1)
monthly_hit_and_run.to_csv('monthly_hit_and_run.csv',index=False)
monthly_hit_and_run


# In[237]:


crash_type = cursor.execute('''SELECT crash_type, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY crash_type, YEAR
                                    ''').fetchall()

crash_type = pd.DataFrame(crash_type, columns = ['crash_type','year','crashes'])
crash_type['crashes_percentage'] = crash_type.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1)
crash_type.to_csv('crash_type.csv',index=False)
crash_type


# In[238]:


# The main cause of the accident 

crash_cause = cursor.execute('''SELECT PRIM_CONTRIBUTORY_CAUSE, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY PRIM_CONTRIBUTORY_CAUSE, YEAR
                                    ''').fetchall()

crash_cause = pd.DataFrame(crash_cause, columns = ['crash_cause','year','crashes'])
crash_cause['crashes_percentage'] = crash_cause.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1)
crash_cause.to_csv('crash_cause.csv',index=False)
crash_cause


# In[239]:


# Street direction accident

street_direction = cursor.execute('''SELECT STREET_DIRECTION, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY STREET_DIRECTION, YEAR
                                    ''').fetchall()

street_direction = pd.DataFrame(street_direction, columns = ['street_direction','year','crashes'])
street_direction['crashes_percentage'] = street_direction.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1)
street_direction.to_csv('street_direction.csv',index=False)
street_direction


# In[240]:


severe_injury = cursor.execute('''SELECT MOST_SEVERE_INJURY, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY MOST_SEVERE_INJURY, YEAR
                                    ''').fetchall()

severe_injury = pd.DataFrame(severe_injury, columns = ['severe_injury','year','crashes'])
severe_injury['crashes_percentage'] = severe_injury.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1)
severe_injury.to_csv('severe_injury.csv',index=False)
severe_injury


# In[241]:


crash_hour = cursor.execute('''SELECT CRASH_HOUR, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY CRASH_HOUR, YEAR
                                    ''').fetchall()
crash_hour = pd.DataFrame(crash_hour, columns = ['crash_hour','year','crashes'])
crash_hour['crashes_percentage'] = crash_hour.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1)
crash_hour.to_csv('crash_hour.csv',index=False)
crash_hour


# In[242]:


crash_weekday = cursor.execute('''SELECT CRASH_DAY_OF_WEEK, substr(CRASH_DATE,7,4) AS YEAR, COUNT(*) FROM Crashes
                                    WHERE YEAR > '2017' AND substr(CRASH_DATE,1,5) < '11/27'
                                    GROUP BY CRASH_DAY_OF_WEEK, YEAR
                                    ''').fetchall()
crash_weekday = pd.DataFrame(crash_weekday, columns = ['crash_weekday','year','crashes'])
crash_weekday['crashes_percentage'] = crash_weekday.apply(lambda row: row.crashes*100/ int(yearly_crashes.loc[yearly_crashes['year'] == row.year]['crashes']) , axis = 1)
crash_weekday.to_csv('crash_weekday.csv',index=False)
crash_weekday


# In[ ]:





# In[11]:


#Create index to improve query performance
cursor.execute('''CREATE INDEX TNPdate1 ON 
                    TNPTrips(substr(Trip_Start_Timestamp,1,10));''').fetchall()
# Save (commit) the changes
conn.commit()


# In[14]:


cursor.execute('''CREATE INDEX YearMonth ON 
                    TNPTrips(substr(Trip_Start_Timestamp,7,4),substr(Trip_Start_Timestamp,1,2));''').fetchall()
conn.commit()


# In[37]:


cursor.execute('''CREATE INDEX YearMonthTaxi ON 
                    TaxiTrips(substr(Trip_Start_Timestamp,7,4),substr(Trip_Start_Timestamp,1,2));''').fetchall()
conn.commit()


# In[13]:


#TNP

daily_trips = cursor.execute('''SELECT substr(Trip_Start_Timestamp,1,10) AS DATE, 
                                    COUNT(*), SUM(Trip_Miles), SUM(Trip_Seconds)/3600, SUM(Fare)
                                    FROM TNPTrips
                                    WHERE substr(Trip_Start_Timestamp,7,4) > '2018'
                                    GROUP BY (DATE)''').fetchall()

daily_trips = pd.DataFrame(daily_trips, columns = ['date', 'TNP_trips', 'TNP_miles', 'TNP_hours', 'fare'])
#daily_trips.to_csv('daily_trips.csv',index=False)
daily_trips = pd.read_csv('daily_trips.csv',sep = ',')
daily_trips


# In[195]:


TNP_trips_time = cursor.execute('''SELECT substr(Trip_Start_Timestamp,7,7) AS HOUR, 
                                          substr(Trip_Start_Timestamp,21,2) AS TIME,
                                          COUNT(*), SUM(Trip_Miles), SUM(Trip_Seconds)/3600, SUM(Fare)
                                          FROM TNPTrips
                                          WHERE substr(Trip_Start_Timestamp,7,4) > '2018'
                                          GROUP BY HOUR, TIME''').fetchall()

TNP_trips_time = pd.DataFrame(TNP_trips_time,columns = ['year_hour', 'time', 'TNP_trips', 'TNP_miles', 'TNP_hours', 'TNP_fare', 'TNP_Avg_tip'])
TNP_trips_time


# In[311]:


monthly_TNPtip = cursor.execute('''SELECT substr(Trip_Start_Timestamp,7,4) AS YEAR, 
                                          substr(Trip_Start_Timestamp,1,2) AS MONTH, 
                                          COUNT(*), SUM(Tip)*100/SUM(Trip_Total), AVG(Tip)
                                          FROM TNPTrips
                                          WHERE Tip != 0 AND YEAR > '2018'
                                          GROUP BY MONTH,YEAR''').fetchall()

monthly_TNPtip = pd.DataFrame(monthly_TNPtip,columns = ['year', 'month', 'TNP_Tippers', 'TNP_Avg_tip%', 'TNP_Avg_tip'])
monthly_TNPtip['Tippers_percentage'] = monthly_TNPtip.apply(lambda row: row.TNP_Tippers*100/ int(monthly_trips.loc[(monthly_trips['year'] == row.year) & (monthly_trips['month'] == row.month)]['TNP_trips']) , axis = 1)
monthly_TNPtip


# In[ ]:





# In[49]:


#TAXI

daily_Taxitrips = cursor.execute('''SELECT substr(Trip_Start_Timestamp,1,10) AS DATE, 
                                    COUNT(*), SUM(Trip_Miles), SUM(Trip_Seconds)/3600, SUM(Fare), AVG(Tips)
                                    FROM Taxitrips
                                    GROUP BY (DATE)''').fetchall()

daily_Taxitrips = pd.DataFrame(daily_Taxitrips,columns = ['date', 'Taxi_trips', 'Taxi_miles', 'Taxi_hours', 'Taxi_fare', 'Taxi_Avg_tip'])
daily_Taxitrips


# In[186]:


monthly_Taxitrips = cursor.execute('''SELECT substr(Trip_Start_Timestamp,7,4) AS YEAR, 
                                         substr(Trip_Start_Timestamp,1,2) AS MONTH,
                                         COUNT(*), SUM(Trip_Miles), SUM(Trip_Seconds)/3600, SUM(Fare)
                                         FROM TaxiTrips
                                         GROUP BY MONTH, YEAR''').fetchall()

monthly_Taxitrips = pd.DataFrame(monthly_Taxitrips,columns = ['year','month', 'Taxi_trips', 'Taxi_miles', 'Taxi_hours', 'Taxi_fare'])
monthly_Taxitrips


# In[313]:


monthly_Taxitip = cursor.execute('''SELECT substr(Trip_Start_Timestamp,7,4) AS YEAR, 
                                          substr(Trip_Start_Timestamp,1,2) AS MONTH, 
                                          COUNT(*), SUM(Tips)*100/SUM(Trip_Total), AVG(Tips)
                                          FROM TaxiTrips
                                          WHERE Tips != 0 AND YEAR > '2018'
                                          GROUP BY MONTH,YEAR''').fetchall()

monthly_Taxitip = pd.DataFrame(monthly_Taxitip,columns = ['year', 'month', 'Taxi_Tippers', 'Taxi_Avg_tip%', 'Taxi_Avg_tip' ])
monthly_Taxitip['Tippers_percentage'] = monthly_Taxitip.apply(lambda row: row.Taxi_Tippers*100/ int(monthly_Taxitrips.loc[(monthly_Taxitrips['year'] == row.year) & (monthly_Taxitrips['month'] == row.month)]['Taxi_trips']) , axis = 1)
monthly_Taxitip


# In[ ]:


# Merge Frames TNP and Taxis 


# In[34]:


#Merge frames TNP

TNP_monthly = pd.merge(monthly_trips, monthly_TNPtip,  how='left', left_on=['year','month'], right_on = ['year','month'])
#TNP_monthly.to_csv('TNP_monthly.csv',index=False)
TNP_monthly = pd.read_csv('TNP_monthly.csv',sep = ',')
TNP_monthly


# In[36]:


TNP_yearly = pd.DataFrame(TNP_monthly.groupby(['year'])['TNP_trips'].sum(), columns = ['TNP_trips'])
TNP_yearly


# In[48]:


# Merge Frame Taxis
Taxi_monthly = pd.merge(monthly_Taxitrips, monthly_Taxitip,  how='left', left_on=['year','month'], right_on = ['year','month'])
#Taxi_monthly.to_csv('Taxi_monthly.csv',index=False)
Taxi_monthly = pd.read_csv('Taxi_monthly.csv',sep = ',')
Taxi_monthly


# In[5]:


Taxi_yearly = pd.DataFrame(Taxi_monthly.groupby(['year'])['Taxi_trips'].sum(), columns = ['Taxi_trips'])
Taxi_yearly


# In[21]:


Yearly_trips = Taxi_yearly.join(TNP_yearly)
Yearly_trips[]



#GOAL : Number of daily trips to hospitals


# In[50]:


# Trips Cordinations example
cursor.execute('''SELECT Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude 
                FROM TNPTrips WHERE substr(Trip_Start_Timestamp,7,4) > '2018' limit 10
                                    ''').fetchall()


# In[51]:


# Chicago Hospitals Cordinations 
# 42 hospitals
hospitals_cords = cursor.execute('''SELECT  ID, LATITUDE , LONGITUDE 
                                        FROM Hospitals 
                                        WHERE CITY = "CHICAGO"''').fetchall()
hospitals_cords = pd.DataFrame(hospitals_cords, columns = ['Hospital_ID','LATITUDE' ,'LONGITUDE'])
hospitals_cords


# In[52]:


#changing string to float on hospital cords
Hospital_IDs = pd.to_numeric(hospitals_cords['Hospital_ID'], downcast="float").tolist()
LATITUDEs = pd.to_numeric(hospitals_cords['LATITUDE'], downcast="float").tolist()
LONGITUDEs = pd.to_numeric(hospitals_cords['LONGITUDE'], downcast="float").tolist()


# In[53]:


cursor.execute('''SELECT  NAME, LATITUDE , LONGITUDE 
                                        FROM Hospitals 
                                        WHERE CITY = "CHICAGO"''').fetchall()


# In[88]:


#Create a column city to join the two tables on 

#cursor.execute('''ALTER TABLE TNPTrips ADD TNPCity TEXT DEFAULT "CHICAGO" ''').fetchall()

##
#This idea does not work 
hospital_trips = cursor.execute('''SELECT TRIP_ID, substr(Trip_Start_Timestamp,1,10) AS DATE, 
                                        Dropoff_Centroid_Latitude FROM_LAT, Dropoff_Centroid_Longitude FROM_LONG,
                                        LATITUDE TO_LAT, LONGITUDE TO_LONG
                                        FROM TNPTrips
                                        LEFT JOIN
                                        Hospitals ON
                                        TNPCity = CITY
                                        WHERE SQRT(SQUARE((TO_LAT-FROM_LAT)*110)+
                                        SQUARE((TO_LONG-FROM_LONG)*COS(TO_LAT)*111)) < 0.1
                                        AND substr(Trip_Start_Timestamp,7,4) > '2018'
                                        GROUP BY(DATE)''').fetchall()


# In[38]:


# TNP Trips to Airports and Hospitals
HospitalTrips = ''' CREATE TABLE HospitalTrips 
                    (TRIP_ID TEXT PRIMARY KEY NOT NULL, 
                    Hospital_ID TEXT,
                    DISTANCE REAL);'''


AirportTrips = ''' CREATE TABLE AirportTrips 
                    (TRIP_ID TEXT PRIMARY KEY NOT NULL, 
                    AIRPORT TEXT,
                    TYPE TEXT);'''


# Taxi Trips to Airports and Hospitals
HospitalTaxis = ''' CREATE TABLE HospitalTaxis 
                    (TRIP_ID TEXT PRIMARY KEY NOT NULL, 
                    Hospital_ID TEXT,
                    DISTANCE REAL);'''

AirportTaxis = ''' CREATE TABLE AirportTaxis 
                    (TRIP_ID TEXT PRIMARY KEY NOT NULL, 
                    AIRPORT TEXT,
                    TYPE TEXT);'''

#cursor.execute('DROP TABLE IF EXISTS HospitalTrips;')
#cursor.execute(HospitalTrips)
#cursor.execute('DROP TABLE IF EXISTS AirportTrips;')
#cursor.execute(AirportTrips)
#cursor.execute('DROP TABLE IF EXISTS HospitalTaxis;')
#cursor.execute(HospitalTaxis)
#cursor.execute('DROP TABLE IF EXISTS AirportTaxis;')
#cursor.execute(AirportTaxis)

# Save (commit) the changes
#conn.commit()


# In[39]:


#Create index to improve query performance
cursor.execute('''CREATE INDEX airport ON 
                    TNPTrips(substr(Trip_Start_Timestamp,1,10),
                        Pickup_Community_Area,
                        Dropoff_Community_Area);''').fetchall()

#Create index to improve query performance
cursor.execute('''CREATE INDEX HospitalTripsdates ON 
                    HospitalTrips(Hospital_ID);''').fetchall()

#Create index to improve query performance
cursor.execute('''CREATE INDEX date ON 
                    TaxiTrips(substr(Trip_Start_Timestamp,1,10));''').fetchall()


# In[55]:


# Dates for TNP trips (After adding new records) after June

dates = cursor.execute('''  SELECT  DISTINCT(substr(Trip_Start_Timestamp,1,10)) AS DATE FROM TNPTrips 
                            WHERE substr(Trip_Start_Timestamp,7,4) = '2020' AND 
                            substr(Trip_Start_Timestamp,1,2) > '06'
                            ORDER BY DATE''').fetchall()
dates = pd.DataFrame(dates, columns = ['date'])
dates


# In[69]:


# Dates for Taxi Trips (After adding new records) After July

dates1 = cursor.execute('''  SELECT  DISTINCT(substr(Trip_Start_Timestamp,1,10)) AS DATE FROM TaxiTrips 
                            WHERE substr(Trip_Start_Timestamp,7,4) = '2020' and
                            substr(Trip_Start_Timestamp,1,2) > '07'
                            ORDER BY DATE''').fetchall()
dates1 = pd.DataFrame(dates1, columns = ['date'])
dates1


# In[59]:


#Get number of Trips to Airports (O'hare, Midway) (Pickup, Dropoff )
# ORD, MDW  #DO, PU

from math import sin, cos, sqrt, atan2, radians

# distance between two points

def distance(lat1,lon1,lat2,lon2):
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance


# In[211]:


#improved query (Batching)


Bsize = 1000
start = time.time()
insert = "INSERT OR IGNORE INTO HospitalTrips VALUES (?,?,?)"



for date in dates['date'].tolist():
    Batch = []
    loadCounter = 0
    daily_counter = 0
    daily_timer = time.time()
    Query = '''  SELECT TRIP_ID,
                    Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                    FROM TNPTrips
                    WHERE Dropoff_Centroid_Latitude != ""
                    AND Dropoff_Centroid_Longitude != ""
                    AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
    trips_cords = cursor.execute(Query)
    trips_cords = pd.DataFrame(trips_cords, columns = ['TRIP_ID','Dropoff_Latitude', 'Dropoff_Longitude'])
    
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords.iloc[t][0]
        lat1 = trips_cords.iloc[t][1]
        lon1 = trips_cords.iloc[t][2]
        for i in range(len(hospitals_cords)):
            Hospital_ID =  hospitals_cords.iloc[i][0]
            lat2 = hospitals_cords.iloc[i][1]
            lon2 = hospitals_cords.iloc[i][2]

            #d = distance(lat1,lon1,lat2,lon2)
            d = (((lat2-lat1)*110)**2+((lon2-lon1)*cos(lat2)*111)**2)**0.5
            if d < 0.25:
                Batch.append([Trip_ID,Hospital_ID,d])
                loadCounter = loadCounter + 1
                daily_counter += 1
            
                if loadCounter == Bsize:    
                    cursor.executemany(insert,Batch)
                    print('{} EXECUTED'.format(daily_counter))
                    loadCounter = 0
                    Batch=[]
                break
    
    cursor.executemany(insert, Batch);
    print(date)
    print(daily_counter)
    print('time = {}'.format(time.time()-daily_timer))
    print('total = {}'.format(time.time()-start))
    print('\n')
    
#Excute batches left that are less than Bsize


end = time.time()
print(end - start)


# In[242]:


#Test1
t = time.time()
for j in range(1000):
    for i in range(len(hospitals_cords)):
        Hospital_ID =  hospitals_cords.iloc[i][0]
        lat2 = hospitals_cords.iloc[i][1]
        lon2 = hospitals_cords.iloc[i][2]
        d = (((lat2-lat1)*110)**2+((lon2-lon1)*cos(lat2)*111)**2)**0.5
            
print(time.time()-t)


# In[241]:


#Test2
#improves performace
t = time.time()
for j in range(1000):
    for i in range(len(hospitals_cords)):
        Hospital_ID =  Hospital_IDs[i]
        lat2 = LATITUDEs[i]
        lon2 = LONGITUDEs[i]
        d = (((lat2-lat1)*110)**2+((lon2-lon1)*cos(lat2)*111)**2)**0.5
            
print(time.time()-t)


# In[262]:


#Test3
#Read from dataFrame

Query = '''  SELECT TRIP_ID,
                    Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                    FROM TNPTrips
                    WHERE Dropoff_Centroid_Latitude != ""
                    AND Dropoff_Centroid_Longitude != ""
                    AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
trips_cords = cursor.execute(Query)
trips_cords = pd.DataFrame(trips_cords, columns = ['TRIP_ID','Dropoff_Latitude', 'Dropoff_Longitude'])
tt = time.time()
x = 0
for t in range(len(trips_cords)):
        Trip_ID = trips_cords.iloc[t][0]
        lat1 = trips_cords.iloc[t][1]
        lon1 = trips_cords.iloc[t][2]
        x += 1
print(x)
print(time.time()-tt)


# In[263]:


#Test4
#Read with no dataFrame
#This improves performance

Query = '''  SELECT TRIP_ID,
                    Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                    FROM TNPTrips
                    WHERE Dropoff_Centroid_Latitude != ""
                    AND Dropoff_Centroid_Longitude != ""
                    AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
trips_cords = cursor.execute(Query).fetchall()
#trips_cords = pd.DataFrame(trips_cords, columns = ['TRIP_ID','Dropoff_Latitude', 'Dropoff_Longitude'])
tt = time.time()
x = 0
for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat1 = trips_cords[t][1]
        lon1 = trips_cords[t][2]
        x += 1
print(x)
print(time.time()-tt)


# In[264]:


#Test5
#This shows that previousTest 4 is more effecient

Query = '''  SELECT TRIP_ID,
                    Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                    FROM TNPTrips
                    WHERE Dropoff_Centroid_Latitude != ""
                    AND Dropoff_Centroid_Longitude != ""
                    AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
trips_cords = cursor.execute(Query)
#trips_cords = pd.DataFrame(trips_cords, columns = ['TRIP_ID','Dropoff_Latitude', 'Dropoff_Longitude'])
tt = time.time()
x = 0
for t in trips_cords:
        Trip_ID = t[0]
        lat1 = t[1]
        lon1 = t[2]
        x += 1
print(x)
print(time.time()-tt)


# In[60]:


#improved query 2
#dates['date'][181:241].tolist() (April 2019/2020)

Bsize = 1000
start = time.time()
insert = "INSERT OR IGNORE INTO HospitalTrips VALUES (?,?,?)"



for date in dates['date'].tolist():
    Batch = []
    loadCounter = 0
    daily_counter = 0
    daily_timer = time.time()
    Query = '''  SELECT TRIP_ID,
                    Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                    FROM TNPTrips
                    WHERE Dropoff_Centroid_Latitude != ""
                    AND Dropoff_Centroid_Longitude != ""
                    AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
    trips_cords = cursor.execute(Query).fetchall()
    #trips_cords = pd.DataFrame(trips_cords, columns = ['TRIP_ID','Dropoff_Latitude', 'Dropoff_Longitude'])
    
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat1 = trips_cords[t][1]
        lon1 = trips_cords[t][2]
        
        for i in range(len(hospitals_cords)):
            Hospital_ID =  Hospital_IDs[i]
            lat2 = LATITUDEs[i]
            lon2 = LONGITUDEs[i]

            d = (((lat2-lat1)*110)**2+((lon2-lon1)*cos(lat2)*111)**2)**0.5
            if d < 0.25:
                Batch.append([Trip_ID,Hospital_ID,d])
                loadCounter = loadCounter + 1
                daily_counter += 1
            
                if loadCounter == Bsize:    
                    cursor.executemany(insert,Batch)
                    #print('{} EXECUTED'.format(daily_counter))
                    loadCounter = 0
                    Batch=[]
                break
    
    cursor.executemany(insert, Batch);
    print(date)
    print(daily_counter)
    print('time = {}'.format(time.time()-daily_timer))
    print('total = {}'.format(time.time()-start))
    print('\n')


end = time.time()
print(end - start)


# In[61]:


cursor.execute('''select count(*) from HospitalTrips''').fetchall()


# In[62]:


# Save (commit) the changes
conn.commit()


# In[ ]:


#Query1

# 1200 meter from point
lat_ohare, lon_ohare = 41.976779, -87.895518
# 600 meter from point to midway
lat_mw, lon_mw = 41.788411, -87.745028

Bsize = 1000
start = time.time()
insert = "INSERT OR IGNORE INTO AirportTrips VALUES (?,?,?)"
datesl = dates['date'].tolist()


for date in datesl:
    Batch = []
    loadCounter = 0
    daily_counter = 0
    daily_timer = time.time()
    Query = '''  SELECT TRIP_ID, Pickup_Centroid_Latitude, Pickup_Centroid_Longitude
                    Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                    FROM TNPTrips
                    WHERE (Dropoff_Centroid_Latitude != "" AND Dropoff_Centroid_Longitude != "") 
                    OR (Pickup_Centroid_Latitude != "" AND Pickup_Centroid_Longitude != "") AND
                    (Pickup_Community_Area = 56 OR Pickup_Community_Area = 76 OR Pickup_Community_Area ="") OR
                    (Dropoff_Community_Area = 56 OR Dropoff_Community_Area = 76 OR Dropoff_Community_Area = "")
                    AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
    
    trips_cords = cursor.execute(Query).fetchall()
    
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat_pickup = trips_cords[t][1]
        lon_pickup = trips_cords[t][2]
        lat_dropoff = trips_cords[t][3]
        lon_dropoff = trips_cords[t][4]
        
        if lat_pickup != "":
            if distance(lat_pickup,lon_pickup,lat_ohare,lon_ohare) < 1.2 :
                Batch.append([Trip_ID,'OH','PU'])
                loadCounter = loadCounter + 1
                daily_counter += 1
                if loadCounter == Bsize:    
                    cursor.executemany(insert,Batch)
                    loadCounter = 0
                    Batch=[]
                continue
            elif distance(lat_pickup,lon_pickup,lat_mw,lon_mw) < 0.6:
                Batch.append([Trip_ID,'MDW','PU'])
                loadCounter = loadCounter + 1
                daily_counter += 1
                if loadCounter == Bsize:    
                    cursor.executemany(insert,Batch)
                    loadCounter = 0
                    Batch=[]
                continue
        
        if lat_dropoff != "":
            if distance(lat_dropoff,lon_dropoff,lat_ohare,lon_ohare) < 1.2:
                Batch.append([Trip_ID,'OH','DO'])
                loadCounter = loadCounter + 1
                daily_counter += 1
            elif distance(lat_dropoff,lon_dropoff,lat_mw,lon_mw) < 0.6:
                Batch.append([Trip_ID,'MDW','DO'])
                loadCounter = loadCounter + 1
                daily_counter += 1
            if loadCounter == Bsize:    
                cursor.executemany(insert,Batch)
                loadCounter = 0
                Batch=[]

    
    cursor.executemany(insert, Batch);
    print(date)
    print(daily_counter)
    print('time = {}'.format(time.time()-daily_timer))
    print('total = {}'.format(time.time()-start))
    print('\n')


end = time.time()
print(end - start)


# In[ ]:


#Query2

# 1200 meter from point
lat_ohare, lon_ohare = 41.976779, -87.895518
# 600 meter from point to midway
lat_mw, lon_mw = 41.788411, -87.745028

Bsize = 1000
start = time.time()
insert = "INSERT OR IGNORE INTO AirportTrips VALUES (?,?,?)"
datesl = dates['date'][14:].tolist()


for date in datesl:
    Batch = []
    loadCounter = 0
    daily_counter = 0
    daily_timer = time.time()
    
    #pickups
    Query = '''  SELECT TRIP_ID, Pickup_Centroid_Latitude, Pickup_Centroid_Longitude
                FROM TNPTrips
                WHERE Pickup_Centroid_Latitude != "" AND Pickup_Centroid_Longitude != "" AND
                (Pickup_Community_Area = 56 OR Pickup_Community_Area = 76 OR Pickup_Community_Area = "")
                AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
    
    trips_cords = cursor.execute(Query).fetchall()
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat_pickup = trips_cords[t][1]
        lon_pickup = trips_cords[t][2]

        if distance(lat_pickup,lon_pickup,lat_ohare,lon_ohare) < 1.2 :
            Batch.append([Trip_ID,'OH','PU'])
            loadCounter = loadCounter + 1
            daily_counter += 1

        elif distance(lat_pickup,lon_pickup,lat_mw,lon_mw) < 0.6:
            Batch.append([Trip_ID,'MDW','PU'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        if loadCounter == Bsize:    
            cursor.executemany(insert,Batch)
            loadCounter = 0
            Batch=[]
            
    cursor.executemany(insert, Batch);
    
    
    #dropoffs
    Query = '''  SELECT TRIP_ID, Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                FROM TNPTrips
                WHERE Dropoff_Centroid_Latitude != "" AND Dropoff_Centroid_Longitude != "" AND 
                (Dropoff_Community_Area = 56 OR Dropoff_Community_Area = 76 OR Dropoff_Community_Area = "")
                AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"

    trips_cords = cursor.execute(Query).fetchall()
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat_dropoff = trips_cords[t][1]
        lon_dropoff = trips_cords[t][2]
               
        if distance(lat_dropoff,lon_dropoff,lat_ohare,lon_ohare) < 1.2:
            Batch.append([Trip_ID,'OH','DO'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        elif distance(lat_dropoff,lon_dropoff,lat_mw,lon_mw) < 0.6:
            Batch.append([Trip_ID,'MDW','DO'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        if loadCounter == Bsize:    
            cursor.executemany(insert,Batch)
            loadCounter = 0
            Batch=[]

    
    cursor.executemany(insert, Batch);
    print(date)
    print(daily_counter)
    print('time = {}'.format(time.time()-daily_timer))
    print('total = {}'.format(time.time()-start))
    print('\n')


end = time.time()
print(end - start)


# In[63]:


#Query2 After Indexing

# 1200 meter from point
lat_ohare, lon_ohare = 41.976779, -87.895518
# 600 meter from point to midway
lat_mw, lon_mw = 41.788411, -87.745028

Bsize = 1000
start = time.time()
insert = "INSERT OR IGNORE INTO AirportTrips VALUES (?,?,?)"
datesl = dates['date'].tolist()


for date in datesl:
    Batch = []
    loadCounter = 0
    daily_counter = 0
    daily_timer = time.time()
    
    #pickups
    Query = '''  SELECT TRIP_ID, Pickup_Centroid_Latitude, Pickup_Centroid_Longitude
                FROM TNPTrips
                WHERE Pickup_Centroid_Latitude != "" AND Pickup_Centroid_Longitude != "" AND
                (Pickup_Community_Area = 56 OR Pickup_Community_Area = 76 OR Pickup_Community_Area = "")
                AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
    
    trips_cords = cursor.execute(Query).fetchall()
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat_pickup = trips_cords[t][1]
        lon_pickup = trips_cords[t][2]

        if distance(lat_pickup,lon_pickup,lat_ohare,lon_ohare) < 1.2 :
            Batch.append([Trip_ID,'OH','PU'])
            loadCounter = loadCounter + 1
            daily_counter += 1

        elif distance(lat_pickup,lon_pickup,lat_mw,lon_mw) < 0.6:
            Batch.append([Trip_ID,'MDW','PU'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        if loadCounter == Bsize:    
            cursor.executemany(insert,Batch)
            loadCounter = 0
            Batch=[]
            
    cursor.executemany(insert, Batch);
    
    
    #dropoffs
    Query = '''  SELECT TRIP_ID, Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                FROM TNPTrips
                WHERE Dropoff_Centroid_Latitude != "" AND Dropoff_Centroid_Longitude != "" AND 
                (Dropoff_Community_Area = 56 OR Dropoff_Community_Area = 76 OR Dropoff_Community_Area = "")
                AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"

    trips_cords = cursor.execute(Query).fetchall()
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat_dropoff = trips_cords[t][1]
        lon_dropoff = trips_cords[t][2]
               
        if distance(lat_dropoff,lon_dropoff,lat_ohare,lon_ohare) < 1.2:
            Batch.append([Trip_ID,'OH','DO'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        elif distance(lat_dropoff,lon_dropoff,lat_mw,lon_mw) < 0.6:
            Batch.append([Trip_ID,'MDW','DO'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        if loadCounter == Bsize:    
            cursor.executemany(insert,Batch)
            loadCounter = 0
            Batch=[]

    
    cursor.executemany(insert, Batch);
    print(date)
    print(daily_counter)
    print('time = {}'.format(time.time()-daily_timer))
    print('total = {}'.format(time.time()-start))
    print('\n')


end = time.time()
print(end - start)


# In[64]:


cursor.execute('''select count(*) from AirportTrips''').fetchall()


# In[66]:


cursor.execute('''select airport, type, count(*) from AirportTrips
                    group by airport, type''').fetchall()


# In[9]:


cursor.execute('''SELECT count(TRIP_ID) FROM TNPTrips WHERE Pickup_Centroid_Latitude = ""
                        AND Pickup_Centroid_Longitude = "" ''').fetchall()


# In[287]:


cursor.execute('''SELECT count(TRIP_ID) FROM TNPTrips WHERE Dropoff_Centroid_Latitude = ""
                        AND Dropoff_Centroid_Longitude = "" ''').fetchall()


# In[288]:


cursor.execute('''SELECT count(TRIP_ID) FROM TNPTrips WHERE (Dropoff_Centroid_Latitude = ""
                        AND Dropoff_Centroid_Longitude = "") OR
                        (Pickup_Centroid_Latitude = ""
                        AND Pickup_Centroid_Longitude = "")''').fetchall()


# In[294]:


cursor.execute('''SELECT count(TRIP_ID) FROM TNPTrips WHERE (Dropoff_Centroid_Latitude = ""
                        AND Dropoff_Centroid_Longitude = "") AND
                        (Pickup_Centroid_Latitude = ""
                        AND Pickup_Centroid_Longitude = "")''').fetchall()


# In[324]:


# OBTAIN AREA OF AIRPORTS

xx = []
for i in range(len(areas)):
    Query = '''SELECT Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude FROM TNPTrips 
                    WHERE Dropoff_Centroid_Latitude != ""
                         AND Dropoff_Centroid_Longitude != "" AND 
                         Dropoff_Community_Area != "" AND
                         Dropoff_Community_Area = ''' + str(areas[i][0]) + " limit 10"
    cc = cursor.execute(Query).fetchall()

    print(areas[i][0])
    for j in range(len(cc)):
        lat_dropoff = cc[j][0]
        lon_dropoff = cc[j][1]
        d1 = distance(lat_dropoff,lon_dropoff,lat_ohare,lon_ohare)
        d2 = distance(lat_dropoff,lon_dropoff,lat_mw,lon_mw)
        print(d1,d2)
    print('\n')
    
# Area 56 midway
# Area 76 o'hare
    


# In[310]:


cursor.execute('''SELECT  count(*) FROM TNPTrips where Dropoff_Community_Area = "" and  Dropoff_Centroid_Latitude != ""
                         AND Dropoff_Centroid_Longitude != ""
                ''').fetchall()


# In[291]:


#This means that there is 32,337 have no pickup or dropoffs
(10041515+11275115)-21284293


# In[19]:


##TEXIS Hospitals and Airports


# In[70]:


#improved query 2 (After Adding New Records)
#copy

Bsize = 1000
start = time.time()
insert = "INSERT OR IGNORE INTO HospitalTaxis VALUES (?,?,?)"



for date in dates1['date'].tolist():
    Batch = []
    loadCounter = 0
    daily_counter = 0
    daily_timer = time.time()
    Query = '''  SELECT TRIP_ID,
                    Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                    FROM TaxiTrips
                    WHERE Dropoff_Centroid_Latitude != ""
                    AND Dropoff_Centroid_Longitude != ""
                    AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
    trips_cords = cursor.execute(Query).fetchall()
    #trips_cords = pd.DataFrame(trips_cords, columns = ['TRIP_ID','Dropoff_Latitude', 'Dropoff_Longitude'])
    
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat1 = trips_cords[t][1]
        lon1 = trips_cords[t][2]
        
        for i in range(len(hospitals_cords)):
            Hospital_ID =  Hospital_IDs[i]
            lat2 = LATITUDEs[i]
            lon2 = LONGITUDEs[i]

            d = (((lat2-lat1)*110)**2+((lon2-lon1)*cos(lat2)*111)**2)**0.5
            if d < 0.25:
                Batch.append([Trip_ID,Hospital_ID,d])
                loadCounter = loadCounter + 1
                daily_counter += 1
            
                if loadCounter == Bsize:    
                    cursor.executemany(insert,Batch)
                    #print('{} EXECUTED'.format(daily_counter))
                    loadCounter = 0
                    Batch=[]
                break
    
    cursor.executemany(insert, Batch);
    print(date)
    print(daily_counter)
    print('time = {}'.format(time.time()-daily_timer))
    print('total = {}'.format(time.time()-start))
    print('\n')


end = time.time()
print(end - start)


# In[ ]:


#03/22/2020, 03/28/2020,03/29/2020, 04/04/2020,04/05/2020 No one took a cap to the hospital in Chicago


# In[31]:


#Get number of Taxis to Airports (O'hare, Midway) (Pickup, Dropoff )
# ORD, MDW  #DO, PU


# In[71]:


#Query2 After Indexing

# 1200 meter from point
lat_ohare, lon_ohare = 41.976779, -87.895518
# 600 meter from point to midway
lat_mw, lon_mw = 41.788411, -87.745028

Bsize = 1000
start = time.time()
insert = "INSERT OR IGNORE INTO AirportTaxis VALUES (?,?,?)"
dates1 = dates1['date'].tolist()


for date in dates1:
    Batch = []
    loadCounter = 0
    daily_counter = 0
    daily_timer = time.time()
    
    #pickups
    Query = '''  SELECT TRIP_ID, Pickup_Centroid_Latitude, Pickup_Centroid_Longitude
                FROM TaxiTrips
                WHERE Pickup_Centroid_Latitude != "" AND Pickup_Centroid_Longitude != "" AND
                (Pickup_Community_Area = 56 OR Pickup_Community_Area = 76 OR Pickup_Community_Area = "")
                AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"
    
    trips_cords = cursor.execute(Query).fetchall()
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat_pickup = trips_cords[t][1]
        lon_pickup = trips_cords[t][2]

        if distance(lat_pickup,lon_pickup,lat_ohare,lon_ohare) < 1.2 :
            Batch.append([Trip_ID,'OH','PU'])
            loadCounter = loadCounter + 1
            daily_counter += 1

        elif distance(lat_pickup,lon_pickup,lat_mw,lon_mw) < 0.6:
            Batch.append([Trip_ID,'MDW','PU'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        if loadCounter == Bsize:    
            cursor.executemany(insert,Batch)
            loadCounter = 0
            Batch=[]
            
    cursor.executemany(insert, Batch);
    
    
    #dropoffs
    Query = '''  SELECT TRIP_ID, Dropoff_Centroid_Latitude, Dropoff_Centroid_Longitude
                FROM TaxiTrips
                WHERE Dropoff_Centroid_Latitude != "" AND Dropoff_Centroid_Longitude != "" AND 
                (Dropoff_Community_Area = 56 OR Dropoff_Community_Area = 76 OR Dropoff_Community_Area = "")
                AND substr(Trip_Start_Timestamp,1,10) = ''' + "'" + date + "';"

    trips_cords = cursor.execute(Query).fetchall()
    for t in range(len(trips_cords)):
        Trip_ID = trips_cords[t][0]
        lat_dropoff = trips_cords[t][1]
        lon_dropoff = trips_cords[t][2]
               
        if distance(lat_dropoff,lon_dropoff,lat_ohare,lon_ohare) < 1.2:
            Batch.append([Trip_ID,'OH','DO'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        elif distance(lat_dropoff,lon_dropoff,lat_mw,lon_mw) < 0.6:
            Batch.append([Trip_ID,'MDW','DO'])
            loadCounter = loadCounter + 1
            daily_counter += 1
            
        if loadCounter == Bsize:    
            cursor.executemany(insert,Batch)
            loadCounter = 0
            Batch=[]

    
    cursor.executemany(insert, Batch);
    print(date)
    print(daily_counter)
    print('time = {}'.format(time.time()-daily_timer))
    print('total = {}'.format(time.time()-start))
    print('\n')


end = time.time()
print(end - start)


# In[72]:


# Save (commit) the changes
conn.commit()


# In[73]:


print(cursor.execute(''' select count(*) from HospitalTaxis ''').fetchall())
print(cursor.execute(''' select count(*) from AirportTaxis ''').fetchall())
print(cursor.execute(''' select count(*) from HospitalTrips ''').fetchall())
print(cursor.execute(''' select count(*) from AirportTrips ''').fetchall())


# In[13]:


#Daily trips to Hospitals
daily_TNPHospitals = cursor.execute('''  SELECT   substr(b.Trip_End_Timestamp,1,10) AS DATE, count(*) 
                                    FROM HospitalTrips a LEFT JOIN TNPTrips b 
                                    ON a.Trip_ID = b.TRIP_ID
                                    WHERE substr(Trip_End_Timestamp,7,4) > '2018'
                                    GROUP BY DATE 
                                    ORDER BY DATE''').fetchall()

daily_TNPHospitals = pd.DataFrame(daily_TNPHospitals, columns = ['date','HTrips'])
#daily_TNPHospitals.to_csv('daily_TNPHospitals.csv',index=False)
daily_TNPHospitals = pd.read_csv('daily_TNPHospitals.csv',sep = ',')
daily_TNPHospitals


# In[29]:


#Daily Taxis to Hospitals
daily_TaxiHospitals = cursor.execute('''  SELECT   substr(b.Trip_End_Timestamp,1,10) AS DATE, count(*) 
                                    FROM HospitalTaxis a LEFT JOIN TaxiTrips b 
                                    ON a.Trip_ID = b.TRIP_ID
                                    WHERE substr(Trip_End_Timestamp,7,4) > '2018'
                                    GROUP BY DATE 
                                    ORDER BY DATE''').fetchall()

daily_TaxiHospitals = pd.DataFrame(daily_TaxiHospitals, columns = ['date','HTaxis'])
daily_TaxiHospitals.to_csv('daily_TaxiHospitals.csv',index=False)
daily_TaxiHospitals = pd.read_csv('daily_TaxiHospitals.csv',sep = ',')
daily_TaxiHospitals


# In[31]:


def time_frame(df):
    
    c = []
    for i in range(len(df)):
        h = df.iloc[i]['hour']
        t = df.iloc[i]['time']

        if h > 0 and h < 6 and t == 'AM':
            c.append('0 - 6 AM')
        if h == 12 and t == 'AM':
            c.append('0 - 6 AM')
        if h > 5 and h < 12 and t == 'AM':
            c.append('6 AM - 12 PM')
        if h > 0 and h < 6 and t == 'PM':
            c.append('12 - 6 PM')
        if h == 12 and t == 'PM':
            c.append('12 - 6 PM')        
        if h > 5 and h < 12 and t == 'PM':
            c.append('6 PM - 12 AM')
    
    return c


# In[48]:


#Daily TNP trips to Airports

daily_TNPAirport = cursor.execute('''  SELECT substr(b.Trip_Start_Timestamp,1,10) AS DATE,
                                    substr(Trip_End_Timestamp,12,2) AS Hour,
                                    substr(Trip_End_Timestamp,21,2) AS Time, 
                                    AIRPORT, TYPE, count(*) 
                                    FROM AirportTrips a LEFT JOIN TNPTrips b 
                                    ON a.Trip_ID = b.TRIP_ID
                                    WHERE substr(Trip_Start_Timestamp,7,4) > '2018'
                                    GROUP BY DATE, Hour, Time, AIRPORT, TYPE 
                                    ORDER BY DATE, Hour, Time, AIRPORT, TYPE''').fetchall()

daily_TNPAirport = pd.DataFrame(daily_TNPAirport, columns = ['date','hour','time','airport','type','trips'])
daily_TNPAirport['hour'] = daily_TNPAirport['hour'].astype('int')
daily_TNPAirport['timeFrame'] = time_frame(daily_TNPAirport)
daily_TNPAirport = daily_TNPAirport.drop('hour',axis=1)
daily_TNPAirport = daily_TNPAirport.groupby(['date','airport','timeFrame','type']).mean().reset_index()
#daily_TNPAirport.to_csv('daily_TNPAirport.csv',index=False)
daily_TNPAirport = pd.read_csv('daily_TNPAirport.csv',sep = ',')
daily_TNPAirport


# In[59]:


#Daily TAXI trips to Airports

daily_TaxiAirport = cursor.execute('''  SELECT substr(Trip_Start_Timestamp,1,10) AS DATE,
                                    substr(Trip_End_Timestamp,12,2) AS Hour,
                                    substr(Trip_End_Timestamp,21,2) AS Time, 
                                    AIRPORT, TYPE, count(*) 
                                    FROM AirportTaxis a LEFT JOIN TaxiTrips b 
                                    ON a.Trip_ID = b.TRIP_ID
                                    WHERE substr(Trip_Start_Timestamp,7,4) > '2018'
                                    GROUP BY DATE, Hour, Time, AIRPORT, TYPE 
                                    ORDER BY DATE, Hour, Time, AIRPORT, TYPE''').fetchall()

daily_TaxiAirport = pd.DataFrame(daily_TaxiAirport, columns = ['date','hour','time','airport','type','taxis'])

daily_TaxiAirport['hour'].replace('', np.nan, inplace=True)
daily_TaxiAirport = daily_TaxiAirport.dropna()
daily_TaxiAirport['hour'] = daily_TaxiAirport['hour'].astype('int')
daily_TaxiAirport['timeFrame'] = time_frame(daily_TaxiAirport)
daily_TaxiAirport = daily_TaxiAirport.drop('hour',axis=1)
daily_TaxiAirport = daily_TaxiAirport.groupby(['date','airport','timeFrame','type']).mean().reset_index()
#daily_TaxiAirport.to_csv('daily_TaxiAirport.csv',index=False)
daily_TaxiAirport = pd.read_csv('daily_TaxiAirport.csv',sep = ',')
daily_TaxiAirport


# In[133]:


daily_TaxiAirport.groupby('date').count()

