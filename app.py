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
from datetime import datetime, timedelta
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

influx = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = influx.write_api(write_options=SYNCHRONOUS)
query_api = influx.query_api()

# =========================
# MODBUS DEVICES
# =========================
modbus_devices = [
    {"name": "production_clean_room", "ip": "192.168.0.7", "unit": 1},
    {"name": "assembly_clean_room", "ip": "192.168.0.17", "unit": 2},
]

# =========================
# FY600 CONFIG
# =========================
FY600_IP = "192.168.0.16"
FY600_UNIT = 1
fy600_lock = threading.Lock()

# =========================
# SAVE FUNCTIONS
# =========================
def save_modbus(device, value):
    point = (
        Point("modbus_reading")
        .tag("device", device)
        .field("value", float(value))
        .time(datetime.now(TZ))
    )
    write_api.write(bucket=INFLUX_BUCKET, record=point)

def save_fy600(pv, sv, outp):
    point = (
        Point("fy600")
        .field("pv", pv)
        .field("sv", sv)
        .field("output", outp)
        .time(datetime.now(TZ))
    )
    write_api.write(bucket=INFLUX_BUCKET, record=point)

# =========================
# BACKGROUND MODBUS LOGGER
# =========================
def modbus_loop():
    while True:
        for d in modbus_devices:
            try:
                client = ModbusClient(
                    host=d["ip"],
                    port=502,
                    unit_id=d["unit"],
                    timeout=2,
                    auto_open=True,
                    auto_close=True
                )

                regs = client.read_holding_registers(0, 2)
                if regs:
                    decoder = BinaryPayloadDecoder.fromRegisters(
                        regs,
                        byteorder=Endian.BIG,
                        wordorder=Endian.LITTLE
                    )
                    value = round(decoder.decode_32bit_float(), 2)

                    save_modbus(d["name"], value)

                    socketio.emit("modbus_update", {
                        "device": d["name"],
                        "value": value,
                        "time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
                    })

            except Exception as e:
                print(f"❌ Modbus {d['name']} error:", e)

        time.sleep(5)

# =========================
# FY600 READ (SINGLE CYCLE)
# =========================
def fy600_read():
    with fy600_lock:
        client = ModbusClient(
            host=FY600_IP,
            port=502,
            unit_id=FY600_UNIT,
            timeout=2,
            auto_open=True,
            auto_close=True
        )

        data = {
            "status": "disconnected",
            "pv": 0,
            "sv": 0,
            "output": 0,
        }

        if not client.open():
            return data

        try:
            pv = client.read_input_registers(0x008A, 1)
            sv = client.read_holding_registers(0x0000, 1)
            outp = client.read_input_registers(0x0087, 1)

            if pv:
                data["pv"] = pv[0]

            if sv:
                data["sv"] = sv[0] / 10

            if outp:
                data["output"] = outp[0] / 10

            data["status"] = "connected"

            # Save to InfluxDB
            save_fy600(data["pv"], data["sv"], data["output"])

            # 🔥 PUSH TO FRONTEND (Socket.IO)
            socketio.emit("fy600_update", data)

        except Exception as e:
            data["status"] = "error"
            data["error"] = str(e)

        return data

# =========================
# QUERY HELPERS
# =========================
def query_range(measurement, start, end):
    start_utc = TZ.localize(
        datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    ).astimezone(pytz.utc)

    end_utc = TZ.localize(
        datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    ).astimezone(pytz.utc)

    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: time(v:"{start_utc}"), stop: time(v:"{end_utc}"))
      |> filter(fn: (r) => r._measurement == "{measurement}")
      |> sort(columns: ["_time"])
    '''

    result = query_api.query(flux)
    rows = []

    for table in result:
        for r in table.records:
            rows.append({
                "field": r.get_field(),
                "value": r.get_value(),
                "device": r.values.get("device"),
                "time": r.get_time().astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
            })

    return rows

# =========================
# FY600 ENDPOINTS
# =========================
def fy600_loop():
    while True:
        fy600_read()
        time.sleep(2)   # read every 2 seconds


@app.get("/fy600/database")
def fy600_database():
    end = datetime.now(TZ)
    start = end - timedelta(days=7)
    return jsonify(query_range(
        "fy600",
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S")
    ))

@app.get("/fy600/database/filter")
def fy600_database_filter():
    return jsonify(query_range(
        "fy600",
        request.args["start"],
        request.args["end"]
    ))

# =========================
# MODBUS ENDPOINTS
# =========================
@app.get("/modbus/realtime")
def modbus_realtime():
    data = []
    for d in modbus_devices:
        try:
            client = ModbusClient(
                host=d["ip"],
                port=502,
                unit_id=d["unit"],
                timeout=2,
                auto_open=True,
                auto_close=True
            )

            regs = client.read_holding_registers(0, 2)
            if regs:
                decoder = BinaryPayloadDecoder.fromRegisters(
                    regs,
                    byteorder=Endian.BIG,
                    wordorder=Endian.LITTLE
                )

                data.append({
                    "device": d["name"],
                    "value": round(decoder.decode_32bit_float(), 2),
                    "time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
                })

        except Exception as e:
            data.append({"device": d["name"], "error": str(e)})

    return jsonify(data)

@app.get("/modbus/database")
def modbus_database():
    end = datetime.now(TZ)
    start = end - timedelta(days=7)
    return jsonify(query_range(
        "modbus_reading",
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S")
    ))

@app.get("/modbus/database/filter")
def modbus_database_filter():
    return jsonify(query_range(
        "modbus_reading",
        request.args["start"],
        request.args["end"]
    ))

@app.route("/test/write")
def test_write():
    from influxdb_client import Point
    p = (
        Point("test_measurement")
        .tag("source", "manual_test")
        .field("value", 123.45)
        .time(datetime.utcnow())
    )
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
    return {"status": "ok", "msg": "Test point written"}


# =========================
# START APP
# =========================
if __name__ == "__main__":
    threading.Thread(target=modbus_loop, daemon=True).start()
    threading.Thread(target=fy600_loop, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=3000)
