import socket
import json
import mariadb
import sys
import datetime

allowedSymbols = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNMЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮйцукенгшщзхъфывапролджэячсмитьбю{}-:;.,_ 0123456789\""

errorFile  = open("temperature_errors","a")
accessFile = open("temperature_requests","a")

settings_datakey = "12345"
settings_user="test",
settins_password="12345",
SERVER_IP = "127.0.0.1"
SERVER_PORT = 12345

def error_write( errorConsoleText, errorText, data )
    print( strtime + " Error, "+ errorConsoleText +"! close connection" )
    strtime =time.datetime.now())
    errStr = strtime + " Bad packet, not allowed, " + errorText + ", By host: "+str(conn.getpeername())
    errStr += "\n----------------------------------------------\n"
    if len(data) == 0:
        errStr += "BAD DATA, NON UNICODE"
    else:
        errStr += data.decode("UTF-8")
    errStr += "\n----------------------------------------------\n"
    errorFile.write(errStr)
    errorFile.flush()

    return

def ifAllowedSymbols(data):
    for symbol in data:
        if not ( symbol in allowedSymbols ):
            return False
    return True



try:
    dbconn = mariadb.connect(
            user=settings_user,
            password=settins_password,
            host="127.0.0.1",
            port=3306,
            database="sensorsdata"
        )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB {e}")
    sys.exit(1)

def addTemperatureToDatabase( dbconn, devname, sensname, temperature ):

    cur = dbconn.cursor()

    sqlsel = "SELECT id FROM sensors WHERE devicename=%s AND name=%s"
    valsel = (devname, sensname)

    cur.execute(sqlsel, valsel)

    id = -1

    if cur.rowcount <= 0:
        print("Append new sensor to database")
        sql = "INSERT INTO sensors(devicename, name) VALUES(%s,%s)"
        val = (devname, sensname)
        cur.execute(sql, val)
        dbconn.commit()
        cur.execute(sqlsel, valsel)
        if cur.rowcount == 1:
            id = cur.fetchone()[0]
    else:
        id = cur.fetchone()[0]

    if id >=1:
        sql = "INSERT INTO temperatures(sensorid, value) VALUES(%s,%s)"
        #print("--- ID "+str(id))
        #print("Temperature "+str(temperature))
        val = (id, temperature)
        cur.execute(sql, val)
        dbconn.commit()

    return

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

sock.bind((SERVER_IP, SERVER_PORT))

sock.listen()

while True:

    conn, addr = sock.accept()

    try:
        data = conn.recv(1024)
    except:
        conn.close()
        continue

    if len(data) == 0:
        conn.close()
        continue

    allowResult = False

    jsonStr = ""

    try:
        jsonStr = data.decode("UTF-8")
        allowResult = ifAllowedSymbols(jsonStr)
    except UnicodeDecodeError:
        strtime = str(datetime.datetime.now())
        print( strtime + " Error, non unicode character!" )
        errStr = strtime + " Bad packet, not allowed, bad symbols in recieved data, By host: "+str(conn.getpeername())
        errStr += "\n----------------------------------------------\n"
        errStr += "BAD DATA, NON UNICODE"
        errStr += "\n----------------------------------------------\n"
        errorFile.write(errStr)
        errorFile.flush()
        conn.close()
        continue

    if allowResult == False:
        strtime = str(datetime.datetime.now())
        print( strtime + " Error, unallowed symbol! close connection" )
        errStr = strtime + " Bad packet, not allowed, bad symbols in recieved data, By host: "+str(conn.getpeername())
        errStr += "\n----------------------------------------------\n"
        errStr += data.decode("UTF-8")
        errStr += "\n----------------------------------------------\n"
        errorFile.write(errStr)
        errorFile.flush()
        conn.close()
        continue

    try:

        jsonreq = json.loads(jsonStr)

    except json.JSONDecodeError as e:
        strtime = str(datetime.datetime.now())
        print( strtime + " Error, invalid JSON syntax! " + str(e) + " close connection, " )
        errStr = strtime + " Bad packet, not allowed, invalid json syntax, By host: "+str(conn.getpeername())
        errStr += "\n----------------------------------------------\n"
        errStr += data.decode("UTF-8")
        errStr += "\n----------------------------------------------\n"
        errorFile.write(errStr)
        errorFile.flush()
        conn.close()
        continue

    devname = ""
    key = ""

    if "device_name" in jsonreq:
        devname = jsonreq["device_name"]
    if "key" in jsonreq:
        key = jsonreq["key"]

    if ( key == settings_datakey ) and ( "sensors_data" in jsonreq ):
        strtime = str(datetime.datetime.now())
        print( strtime + " Good packet, allowed, By host: "+str(conn.getpeername()))
        rqStr = ( strtime + " Good packet \n" )
        accessFile.write(rqStr)
        accessFile.flush()
        sensorsdata = jsonreq["sensors_data"]
        for i in range(1,10):
            sensor_iter_name  = "sensor_"+str(i)+"_name"
            sensor_iter_value = "sensor_"+str(i)+"_value"
            if ( sensor_iter_name in sensorsdata) and (sensor_iter_value in sensorsdata):
                #print(sensorsdata[sensor_iter_name])
                #print(sensorsdata[sensor_iter_value])
                if ( float(sensorsdata[sensor_iter_value]) >= -55 ) and ( float(sensorsdata[sensor_iter_value]) <= 125 ) :
                    addTemperatureToDatabase( dbconn, devname, sensorsdata[sensor_iter_name], sensorsdata[sensor_iter_value] )
                else:
                    print("Packet allowed, but bad sensor value!")
    else:
        strtime = str(datetime.datetime.now())
        print( strtime + " Error, unallowed format or bad key! close connection" )
        errStr = strtime + " Bad packet, unallowed format or bad key, By host: "+str(conn.getpeername())
        errStr += "\n----------------------------------------------\n"
        errStr += data.decode("UTF-8")
        errStr += "\n----------------------------------------------\n"
        errorFile.write(errStr)
        errorFile.flush()
        conn.close()
        continue

    conn.close()
