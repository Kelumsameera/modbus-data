# =========================
# EVENTLET (MUST BE FIRST)
# =========================
import eventlet
eventlet.monkey_patch()

# =========================
# IMPORTS
# =========================
import threading
import time
from datetime import datetime
import pytz
import os

from flask import Flask, request, jsonify
from flask_socketio import SocketIO

from pyModbusTCP.client import ModbusClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# =========================
# APP SETUP
# =========================
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

TZ = pytz.timezone("Asia/Colombo")

# =========================
# INFLUXDB CONFIG
# =========================
INFLUX_URL = "http://influxdb:8086"
INFLUX_ORG = "factory_iot"
INFLUX_BUCKET = "modbus_data"
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")

if not INFLUX_TOKEN:
    raise RuntimeError("❌ INFLUXDB_TOKEN not set")

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

# =========================
# MODBUS DEVICES
# =========================
modbus_devices = [
    {"name": "production_clean_room", "ip": "192.168.0.7", "unit": 1},
    {"name": "assembly_clean_room", "ip": "192.168.0.17", "unit": 2},
]

# =========================
# FY600 WATER TANK
# =========================
FY600_IP = "192.168.0.16"
FY600_UNIT = 1

# =========================
# SAVE FUNCTIONS
# =========================
def save_pressure(device, value):
    p = (
        Point("pressure")
        .tag("device", device)
        .field("value", float(value))
        .time(datetime.utcnow())
    )
    write_api.write(bucket=INFLUX_BUCKET, record=p)

def save_water_tank(level, setpoint, output):
    p = (
        Point("water_tank")
        .tag("device", "fy600")
        .field("level", float(level))
        .field("setpoint", float(setpoint))
        .field("output", float(output))
        .time(datetime.utcnow())
    )
    write_api.write(bucket=INFLUX_BUCKET, record=p)

# =========================
# MODBUS LOOP
# =========================
def modbus_loop():
    while True:
        for d in modbus_devices:
            try:
                c = ModbusClient(
                    host=d["ip"],
                    port=502,
                    unit_id=d["unit"],
                    auto_open=True,
                    auto_close=True
                )

                regs = c.read_holding_registers(0,2)

                if regs:
                    decoder = BinaryPayloadDecoder.fromRegisters(
                        regs,
                        byteorder=Endian.BIG,
                        wordorder=Endian.LITTLE
                    )

                    value = round(decoder.decode_32bit_float(),2)

                    save_pressure(d["name"], value)

                    socketio.emit("modbus_update",{
                        "device":d["name"],
                        "value":value,
                        "time":datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
                    })

            except Exception as e:
                print("❌ Modbus:",e)

        time.sleep(5)

# =========================
# FY600 LOOP
# =========================
def fy600_loop():
    while True:
        try:
            c = ModbusClient(
                host=FY600_IP,
                port=502,
                unit_id=FY600_UNIT,
                auto_open=True,
                auto_close=True
            )

            pv = c.read_input_registers(0x008A,1)
            sv = c.read_holding_registers(0x0000,1)
            outp = c.read_input_registers(0x0087,1)

            level = pv[0] if pv else 0
            setpoint = sv[0]/10 if sv else 0
            output = outp[0]/10 if outp else 0

            save_water_tank(level,setpoint,output)

            socketio.emit("water_tank_update",{
                "level":level,
                "setpoint":setpoint,
                "output":output
            })

        except Exception as e:
            print("❌ FY600:",e)

        time.sleep(2)

# =========================
# QUERY HELPER
# =========================
def query_range(measurement,start,end):

    start_dt = TZ.localize(
        datetime.strptime(start,"%Y-%m-%d %H:%M:%S")
    ).astimezone(pytz.utc)

    end_dt = TZ.localize(
        datetime.strptime(end,"%Y-%m-%d %H:%M:%S")
    ).astimezone(pytz.utc)

    start_utc = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    flux=f'''
    from(bucket:"{INFLUX_BUCKET}")
      |> range(start:{start_utc}, stop:{end_utc})
      |> filter(fn:(r)=> r._measurement=="{measurement}")
      |> sort(columns:["_time"])
    '''

    result=query_api.query(flux)

    rows=[]
    for table in result:
        for r in table.records:
            rows.append({
                "field":r.get_field(),
                "value":r.get_value(),
                "device":r.values.get("device"),
                "time":r.get_time()
                    .astimezone(TZ)
                    .strftime("%Y-%m-%d %H:%M:%S")
            })

    return rows

# =========================
# API ENDPOINTS
# =========================
@app.get("/health")
def health():
    return {"status":"ok"}

@app.get("/pressure/database/filter")
def pressure_filter():
    return jsonify(query_range(
        "pressure",
        request.args.get("start"),
        request.args.get("end")
    ))

@app.get("/water-tank/database/filter")
def tank_filter():
    return jsonify(query_range(
        "water_tank",
        request.args.get("start"),
        request.args.get("end")
    ))

# =========================
# START
# =========================
if __name__=="__main__":
    threading.Thread(target=modbus_loop,daemon=True).start()
    threading.Thread(target=fy600_loop,daemon=True).start()

    socketio.run(app,host="0.0.0.0",port=3000)
