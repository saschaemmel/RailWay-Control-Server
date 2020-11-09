# ---------------------------------------------------------------------------------
#  _____       _ ___          __                 _____            _             _
# |  __ \     (_) \ \        / /                / ____|          | |           | |
# | |__) |__ _ _| |\ \  /\  / /_ _ _   _ ______| |     ___  _ __ | |_ _ __ ___ | |
# |  _  // _` | | | \ \/  \/ / _` | | | |______| |    / _ \| '_ \| __| '__/ _ \| |
# | | \ \ (_| | | |  \  /\  / (_| | |_| |      | |___| (_) | | | | |_| | | (_) | |
# |_|  \_\__,_|_|_|   \/  \/ \__,_|\__, |       \_____\___/|_| |_|\__|_|  \___/|_|
#                                   __/ |
#                                  |___/
# ---------------------------------------------------------------------------------
# Author: Sascha Patrick Emmel
# Mail: saschaemmel@gmail.com
# ---------------------------------------------------------------------------------
# Versionlog:
# Version   Date        Author                  Changes
# V0.0.0.1  09.11.2020  Sascha Patrick Emmel    Initial Version
# ---------------------------------------------------------------------------------

# ---------------------------------------------------
# Import Directives
# ---------------------------------------------------
from datetime import datetime
from enum import Enum
import paho.mqtt.client as mqtt
import time
import json
import colorama

# import psycopg2 #

# ---------------------------------------------------
# USER-Settings
# ---------------------------------------------------

MQTT_SERVER = "192.168.178.43"
MQTT_TOPIC = "RailWay-Control"
TOPIC_STATUS = "status"

# ---------------------------------------------------
# System Variables
# ---------------------------------------------------
VERSION = "V0.0.0.1"
DEBUGLEVEL = "DEBUG"  # DEBUG / RELEASE
SYSTEMNAME = "FT-TRAIN"

TIMERTIMEMS = 100  # Cyclic Time of the Programm
TIMER100MS = 100
TIMER200MS = 200
TIMER500MS = 500
TIMER1S = 1000
TIMER5S = 5000

frmNam = "RailWay-Control."
client = mqtt.Client()


# ---------------------------------------------------
# ENUMS
# ---------------------------------------------------
class ErrClass(Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    FORCE = 10


# ---------------------------------------------------
# CLASSES
# ---------------------------------------------------
class RailWayElement:
    UUID = 0
    NAME = ""
    TYPE = ""
    LAST_SEEN = ""
    IP = ""
    LED_FRONT = ""
    LED_BACK = ""
    WIFI_LEVEL = ""
    U_BAT = ""
    TEMP = ""

    def __new__(cls, _uuid, _name, _type):
        cls.UUID = _uuid
        cls.NAME = _name
        cls.TYPE = _type

    def set_state(cls, last_seen, led_front, led_back, wifi_level, u_bat, temp):
        cls.LAST_SEEN = last_seen
        cls.LED_FRONT = led_front
        cls.LED_BACK = led_back
        cls.WIFI_LEVEL = wifi_level
        cls.U_BAT = u_bat
        cls.TEMP = temp

    def get_state(self):
        pass

RailWayElements = {}

def stringfromerrclass(err_class):
    # Get errClass String
    err_class_string = ""
    if err_class == ErrClass.DEBUG:
        err_class_string = colorama.Fore.CYAN + "DEBUG" + colorama.Style.RESET_ALL
    elif err_class == ErrClass.INFO:
        err_class_string = colorama.Fore.GREEN + "INFO" + colorama.Style.RESET_ALL
    elif err_class == ErrClass.WARNING:
        err_class_string = colorama.Fore.YELLOW + "WARNING" + colorama.Style.RESET_ALL
    elif err_class == ErrClass.ERROR:
        err_class_string = colorama.Fore.RED + "ERROR" + colorama.Style.RESET_ALL
    elif err_class == ErrClass.FORCE:
        err_class_string = colorama.Fore.WHITE + "SYSTEM" + colorama.Style.RESET_ALL

    return err_class_string

# ---------------------------------------------------
# Writes to Log-File
# ---------------------------------------------------
def writelog(err_class, err_pos, err_text, err_category="SYSTEM"):
    # Get actual DateTime
    act_time = datetime.now().strftime("%d.%m.%Y - %H:%M:%S:%f")

    file = open("/var/log/RailWay-Control/" + err_category + ".log", "a")
    file.write(act_time + " : " + stringfromerrclass(err_class) + " : " + err_pos + " : " + err_text + colorama.Style.RESET_ALL + "\n")
    file.close()


def writelogp(err_class, err_pos, err_text, err_category="SYSTEM"):
    writelog(err_class, err_pos, err_text, err_category)
    print(stringfromerrclass(err_class) + " : " + err_pos + " : " + err_text)


# ---------------------------------------------------
# Connects to MQTT-Broker
# ---------------------------------------------------
def connectToBroker():
    subnam = "connectToBroker"
    try:
        writelog(ErrClass.INFO, frmNam + subnam, "Connect to Broker at " + MQTT_SERVER)
        client.on_connect = on_connect
        client.on_message = on_message

        # Connect to Broker
        client.connect(MQTT_SERVER, 1883, 60)
        # max_inflight_messages_set(self, inflight) = 50

        # Do MQTT NetworkStuff in Backround
        client.loop_start()
    except:
        writelogp(ErrClass.INFO, frmNam + subnam, "Error on connect to Broker")
        exit()


# ---------------------------------------------------
# Reacts on incoming Messages
# ---------------------------------------------------
def on_message(client, userdata, msg):
    subNam = "on_message"

    if msg.topic.find("STATUS") != -1:
        json_data = json.loads(msg.payload)

        writelogp(ErrClass.INFO, frmNam + subNam, "STATUS Received from " + json_data["UUID"], "MQTT")

        if json_data["UUID"] in RailWayElements:
            # Update a known Signal
            tmpElement = RailWayElements[json_data["UUID"]]

            if tmpElement.IP != json_data["IP"]:
                writelogp(ErrClass.ERROR, frmNam + subNam, "IP of Element Changed / Multiple Elements with same IP!")

            else:
                tmpElement.LAST_SEEN = json_data["DATETIME"]
                tmpElement.LED_FRONT = json_data["LED-FRONT"]
                tmpElement.LED_BACK = json_data["LED-BACK"]
                tmpElement.WIFI_LEVEL = json_data["WIFI-LEVEL"]
                tmpElement.U_BAT = json_data["U-BAT"]
                tmpElement.TEMP = json_data["TEMP"]

        else:
            # Its a new Signal!
            tmpElement = RailWayElement
            tmpElement.UUID = json_data["UUID"]
            tmpElement.LAST_SEEN = json_data["DATETIME"]
            tmpElement.IP = json_data["IP"]
            tmpElement.LED_FRONT = json_data["LED-FRONT"]
            tmpElement.LED_BACK = json_data["LED-BACK"]
            tmpElement.WIFI_LEVEL = json_data["WIFI-LEVEL"]
            tmpElement.U_BAT = json_data["U-BAT"]
            tmpElement.TEMP = json_data["TEMP"]

            writelogp(ErrClass.INFO, frmNam + subNam, "Add new Element to DB: " + tmpElement.UUID)

        # Update Element in Dictionary
        RailWayElements[json_data["UUID"]] = tmpElement

        # Checks if there is already a Signal with this ID
        # mydb = mysql.connector.connect(host="localhost",user="railway",passwd="123",database="railway")
        # mycursor = mydb.cursor()
        # mycursor.execute("SELECT 'id' FROM devices WHERE id='" + json_data["UUID"] + "'")
        # myresult = mycursor.fetchone()

        # if myresult is None:
        #     # Insert new Data into device list
        #    sql = "INSERT INTO devices (id, type, number, name, ip, active) VALUES (%s, %s, %s, %s, %s, %s)"
        #    val = (json_data["UUID"], "SIGNAL", "0", "-", json_data["IP"], False)
        #    # print(sql)
        #    # print(val)
        #    mycursor.execute(sql, val)
        #    mydb.commit()
        #    # print(mycursor.rowcount, "record inserted.")
        #    # Insert new Data also into device Status
        #    sql = "INSERT INTO device_status (id, signalstate_front, signalstate_back, voltage, wifi_strength, " \
        #          "temperature) VALUES (%s, %s, %s, %s, %s, %s) "
        #    val = (
        #        json_data["UUID"], json_data["LED-FRONT"], json_data["LED-BACK"], json_data["U-BAT"],
        #        json_data["WIFI-LEVEL"], json_data["TEMP"])
        #    # print(sql)
        #    # print(val)
        #    mycursor.execute(sql, val)
        #    mydb.commit()
        #    # print(mycursor.rowcount, "record inserted.")
        # else:
        #    # Update Data
        #    sql = "UPDATE devices SET type = 'SIGNAL', ip = '" + json_data["IP"] + "' WHERE id = '" + json_data[
        #        "UUID"] + "'"
        #    mycursor.execute(sql)
        #    mydb.commit()
        #    # print(mycursor.rowcount, "record(s) affected")
        #    sql = "UPDATE device_status SET voltage = '" + json_data["U-BAT"] + "', temperature ='" + json_data[
        #        "TEMP"] + "', wifi_strength ='" + json_data["WIFI-LEVEL"] + "', signalstate_front ='" + json_data[
        #              "LED-FRONT"] + "', signalstate_back ='" + json_data["LED-BACK"] + "' WHERE id = '" + json_data[
        #              "UUID"] + "'"
        #    # print(sql)
        #    mycursor.execute(sql)
        #    mydb.commit()
        #    # print(mycursor.rowcount, "record(s) affected")
    elif msg.topic.find("LIFE") != -1:
        json_data = json.loads(msg.payload)
        writelogp(ErrClass.INFO, frmNam + subNam, "LIFE Received from " + json_data["UUID"], "MQTT")
        print("LIFE Received from " + json_data["UUID"])

        # Checks if there is already a Signal with this ID
        # mydb = mysql.connector.connect(host="localhost",user="railway",passwd="123",database="railway")
        # mycursor = mydb.cursor()
        # mycursor.execute("SELECT 'id' FROM devices WHERE id='" + json_data["UUID"] + "'")
        # myresult = mycursor.fetchone()

        # if myresult is not None:
        #    # Update Date
        #    sql = "UPDATE device_status SET life_telegram ='" + json_data["DATETIME"] + "' WHERE id = '" + json_data[
        #        "UUID"] + "'"
        #    # print(sql)
        #    mycursor.execute(sql)
        #    mydb.commit()
        #    # print(mycursor.rowcount, "record(s) affected")
    else:
        writelogp(ErrClass.WARNING, frmNam + subNam, "Received non-valid Message at " + msg.topic)


# ---------------------------------------------------
# Reacts on connecting to Broker - Subscribe Topics
# ---------------------------------------------------
def on_connect(client, userdata, flags, rc):
    print("Connected to Broker with result code " + str(rc))
    writelogp(ErrClass.INFO, frmNam + subNam, "Connected successful to Broker")

    # Subscribe Topics
    client.subscribe(MQTT_TOPIC + "/" + SYSTEMNAME + "/status/#")
    # TODO: Only for Alpha Test
    client.subscribe("MANNHEIM01/SIGNAL001/STATUS")


# ---------------------------------------------------
# Reads Pending Commands from the DB and sends them to Broker and delete it from DB
# ---------------------------------------------------
def checkcommand():
    print("checkcommand")
    # TODO: replace with PG SQL
    # mycursor.execute("SELECT * FROM command")
    # myresult = mycursor.fetchone()

    # if myresult is not None:
    #   for row in myresult:
    #        id = row[0]
    #        datetime = row[1]
    #        topic = row[2]
    #        payload = row[3]
    #        qos = int(row[4])
    #        retain = bool(row[5])
    #        # Send Message to Broker
    #        client.publish(topic, payload, qos, retain)


# ---------------------------------------------------
# Sends a MQTT Telegram to Broker
# ---------------------------------------------------
def sendmqtt(topic, qos, payload, retain):
    subNam = "sendmqtt"
    if client.is_connected():
        # writeLog(ErrClass.INFO, frmNam + subNam, "Send Message to Broker at Topic: " + MQTT_TOPIC + "/" +
        # SYSTEMNAME + "/" + topic, "MQTT")
        client.publish(MQTT_TOPIC + "/" + SYSTEMNAME + "/" + topic, payload, qos, retain)
    else:
        writelogp(ErrClass.WARNING, frmNam + subNam, "Can't send MQTT Message, because of no Broker-Connection")


# ---------------------------------------------------
# Sends an Alive-Telegram to Broker
# ---------------------------------------------------
def sendalive():
    json_data = '{\n"alive": "TRUE"\n}'
    sendmqtt("alive", 0, json_data, False)


# ---------------------------------------------------
# StartUP
# ---------------------------------------------------
subNam = "Main"

colorama.init()

writelogp(ErrClass.FORCE, frmNam + subNam, "RailWay Control " + VERSION + " by Sascha Patrick Emmel c2020")

# TODO: replace Connection to PG SQL
# mydb = mysql.connector.connect(host="localhost", user="railway", passwd="123", database="railway")
# mycursor = mydb.cursor()

connectToBroker()

# ---------------------------------------------------
# MainLoop - Do cyclic Stuff
# ---------------------------------------------------
writelogp(ErrClass.FORCE, frmNam + subNam, "Start Main Loop")
while True:
    # Main Loop - Do cylic stuff
    # checkCommand()
    if TIMER100MS <= 0:  # Do every 100ms
        TIMER100MS = 100
    if TIMER200MS <= 0:  # Do every 200ms
        TIMER200MS = 200
    if TIMER500MS <= 0:  # Do every 500ms
        TIMER500MS = 500
    if TIMER1S <= 0:  # Do every 1 second
        sendalive()
        TIMER1S = 1000
    if TIMER5S <= 0:
        if not client.is_connected():  # Reconnect to Broker if connection is lost
            writelogp(ErrClass.ERROR, frmNam + subNam, "Lost MQTT connection, try to reconnect")
            connectToBroker()
        TIMER5S = 5000

    TIMER100MS = TIMER100MS - TIMERTIMEMS
    TIMER200MS = TIMER200MS - TIMERTIMEMS
    TIMER500MS = TIMER500MS - TIMERTIMEMS
    TIMER1S = TIMER1S - TIMERTIMEMS
    TIMER5S = TIMER5S - TIMERTIMEMS

    time.sleep(TIMERTIMEMS / 1000)  # runs every 100ms
