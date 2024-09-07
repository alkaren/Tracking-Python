import time
import serial
import socket
import json
import sys
import sqlite3
import ast
import RPi.GPIO as GPIO
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR
import sched
import os
import datetime
import subprocess


path = "/home/pi/DTRACK-Service/INDE/"
path_error="/home/pi/DTRACK-Service/LOG/Error_scraps.txt"

def log_error(path, error):
    try:
        f= open(path, "a")
        f.write(str(error) + "\n")
        f.close
    except Exception as e:
        os.remove(path)

# Fungsi Buat folder untuk menulis data didalamnya
def buat_folder(folder):
  if not os.path.exists(folder):
    os.makedirs(folder)


#2. MEMBUAT FOLDER TANGGAL DI DIDALAM RASPVERRY
def tujuan(path):
	baru = datetime.datetime.now()
	folder = baru.strftime("%Y-%m-%d") #Nama folder
	folder_path = path + folder +'/'  #Path di foldernya
	buat_folder(folder_path)
	return folder_path

#MASUKAN DATA KE TXT BASED ON GPS TIME
def kumpul(mydata, file_path, waktu_gps):
    format = "%Y-%m-%d %H:%M:%S"
    data_time = datetime.datetime.strptime(waktu_gps, format)
    data_string = json.dumps(mydata)
    try:
        with open(file_path, 'a') as f:
            f.write(data_string + '\n')
    except Exception as e:
        error_pesan = str(e) +", time ="+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print("masalah kumpul = {}".format(error_pesan))

def s():
    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(11, GPIO.OUT)
    GPIO.output(11, GPIO.LOW)

def setConfig(configPath):
    config = json.load(open(configPath, "r"))
    return config

def read_gps_initial_data(ser):
    for _ in range(16):
        ser.readline().decode("utf-8")

def sqliteStart(databasePath):
    con = sqlite3.connect(databasePath)
    
    return con
    
def parseNmea(rawNmea, jsonData):
    sentence = rawNmea[0]
    if "RMC" in sentence:
        if rawNmea[8] == "" or rawNmea[8].replace(".", "").isnumeric() == False:
            
            jsonData['rmc']['direction'] = "0"
        else :
            jsonData['rmc']['direction'] = rawNmea[8]
        
        
    if "VTG" in sentence:
        if rawNmea[7] != "":
            jsonData['vtg']['speed'] = float(rawNmea[7])

    if "GGA" in sentence:
        if rawNmea[4] != "" and rawNmea[5] != "":
            if rawNmea[5] == "W":
                sign = -1 
            else:
                sign = 1
            jsonData['gga']['longitude'] = round(((float(rawNmea[4][:3])+(float(rawNmea[4][3:])/60))*sign),7)
        if rawNmea[2] != "" and rawNmea[3] != "":
            if rawNmea[3] == "S":
                sign = -1 
            else:
                sign = 1
            jsonData['gga']['latitude'] = round(((float(rawNmea[2][:2])+(float(rawNmea[2][2:])/60))*sign), 7)
            
            
        if rawNmea[9] != "" and rawNmea[11] != "":
            jsonData['gga']['altitude'] = round((float(rawNmea[9]) + float(rawNmea[11])),3)
        if rawNmea[7] != "":
            jsonData['gga']['satellite'] = int(rawNmea[7])
    
    return jsonData

def readGpsData(config):    
    jsonData = {
        "rmc" : {
            "direction":"-1",
        },
        "vtg" : {
            "speed":-1
        },
        "gga" : {
            "longitude":-1,
            "latitude":-1,
            "altitude":-1,
            "satellite":-1
        }     
    }
    try:
        ser = serial.Serial(port = '/dev/ttyUSB_GPS', baudrate = 9600, timeout = 1)  # open serial port
        for _ in range(3):
            try:
                rawNmea = ser.readline().decode("utf-8").strip("\n").strip("\r").split(',')
                #print(rawNmea)
            except serial.serialutil.SerialException as e:
                error_pesan = str(e) +", time ="+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                log_error(path_error, error_pesan)
                print("masalah serial = {}".format(error_pesan))
            except Exception as e:
                error_pesan = str(e) +", time ="+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                log_error(path_error, error_pesan)
                print("masalah rawnmea = {}".format(error_pesan))
            else:
                jsonData = parseNmea(rawNmea, jsonData)
                #print(rawNmea)
    except Exception as e:
        error_pesan = str(e) +", time ="+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        log_error(path_error, error_pesan)
        print("masalah serial = {}".format(error_pesan))

    data = {
        "id_unit": config['id_unit'],
        "longitude": jsonData['gga']['longitude'],
        "latitude": jsonData['gga']['latitude'],
        "altitude": jsonData['gga']['altitude'],
        "speed": jsonData['vtg']['speed'],
        "direction": jsonData['rmc']['direction'],
        "satellite": jsonData['gga']['satellite'],
        "gps_time": jsonData['gga']['gps_time'],
        "nosimcard": config['nosimcard'],
        "version": config['version'],
        "egi": config['egi'],
        "class": config['class'],
        "company": config['company'],
        "insertdate" : "sqlite_function_1",
        "module_name": "Raspberry Pi",
        "sendstatus" : 0
    }

    
    jsonDataDumps = json.dumps(data, indent = 2)
       

    return data

def round_minute(minutes):
    round_m = int(minutes) // 5*5
    round_m = str(round_m).zfill(2)
    return str(round_m)

def loggingGpsData(config):
    
    timestamp = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    data = readGpsData(config)
    print(data)
    try:
        data['gps_time'] = timestamp  #"2023-03-03 12:12:12"
        column = repr(tuple(data.keys()))
        value = repr(tuple(data.values())).replace("'sqlite_function_1'","strftime('%Y-%m-%d %H:%M:%S','now','localtime')")
        databasePath = config['databaseGPS']
        con = sqliteStart(databasePath)
        query = "INSERT INTO HISTORY " + column + " VALUES " + value
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()
        print("Simpan SQLite sukses")
    except Exception as e:
        error_pesan = str(e) +", time ="+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        log_error(path_error, error_pesan)
        print("masalah sqlite= {}".format(error_pesan))


    try:
        data["insertdate"]=timestamp
        f = open("/home/pi/DTRACK-Service/gps_live.txt", "w")
        f.write(str(data) + "\n")
        f.close
        print("Simpan GPsLive sukses")
    except Exception as e:
        error_pesan = str(e) +", time ="+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        log_error(path_error, error_pesan)
        print("masalah gpslive = {}".format(error_pesan))
        os.remove("/home/pi/DTRACK-Service/gps_live.txt")
    
    try:
        folder_path = tujuan(path)
        berkas=datetime.datetime.now().strftime("%H") + round_minute(datetime.datetime.now().strftime("%02M") ) + ".txt"
        file_path= folder_path+berkas
        #print("timestamp={} path={}".format(datetime.datetime.now(), file_path))
        kumpul (data, file_path, timestamp)
        print("Simpan txt sukses")
    except Exception as e:
        error_pesan = str(e) +", time ="+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        log_error(path_error, error_pesan)
        print("masalah ftp = {}".format(error_pesan))

            
def listener(event):
    timestamp = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f'{timestamp} Job {event.job_id} raised {event.exception.__class__.__name__}')

def APScheduler(config):
    scheduler = BlockingScheduler(job_defaults={'max_instances': 100})   
    scheduler.add_job(loggingGpsData, 'interval',args=[config] ,seconds = 1)       
    scheduler.add_listener(listener, EVENT_JOB_ERROR)
    return scheduler

def main():
   
    setBuzzer()
    configPath = "/home/pi/DTRACK-Service/settingGPS.json"
    
    config = setConfig(configPath)
    
  
    time.sleep(1)
    APScheduler(config).start()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        GPIO.output(11, GPIO.LOW)
        print("program stopped (keyboard Interupt)")
        sys.exit(0)


