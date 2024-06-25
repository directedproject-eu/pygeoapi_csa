import datetime
import random
import uuid
from time import sleep

import requests

last_result = 10

#url_stub = "http://docker.srv.int.52north.org:5000"
url_stub = "http://localhost:5000"


def post(path: str, payload: dict, content_type: str = "application/json"):
    url = url_stub + path
    headers = {"Content-Type": content_type}
    response = requests.request("POST", url, json=payload, headers=headers)
    print(response.text)


def gen_observation() -> dict:
    global last_result
    last_result = last_result + random.uniform(-0.15, 0.15)
    return {
        "resultTime": datetime.datetime.now().isoformat(),
        "result": last_result
    }


def gen_system():
    id = str(uuid.uuid4())
    return {
        "type": "PhysicalSystem",
        "id": id,
        "definition": "http://www.w3.org/ns/sosa/Sensor",
        "uniqueId": f"urn:x-ogc:systems:{id}",
        "label": "Outdoor Thermometer 001",
        "description": "Digital thermometer located on first floor window 1",
        "typeOf": {
            "href": "https://data.example.org/api/procedures/TP60S?f=sml",
            "uid": "urn:x-myorg:datasheets:ThermoPro:TP60S:v001",
            "title": "ThermoPro TP60S",
            "type": "application/sml+json"
        },
        "identifiers": [
            {
                "definition": "http://sensorml.com/ont/swe/property/SerialNumber",
                "label": "Serial Number",
                "value": "0123456879"
            }
        ],
        "contacts": [
            {
                "role": "http://sensorml.com/ont/swe/roles/Operator",
                "organisationName": "Field Maintenance Corp."
            }
        ],
        "validTime": [
            "2019-08-24T14:15:22Z",
            "2025-08-24T17:15:22Z"
        ],
        "position": {
            "type": "Point",
            "coordinates": [
                51.935814, 7.651898
            ]
        }
    }


def gen_datastream():
    return {
        "id": str(uuid.uuid4()),
        "name": "Indoor Thermometer 001 - Living Room Temperature",
        "description": "Indoor temperature measured on the south wall of the living room at 1.5m above the floor",
        "ultimateFeatureOfInterest@link": {
            "href": "https://data.example.org/api/collections/buildings/items/754",
            "title": "My House"
        },
        "samplingFeature@link": {
            "href": "https://data.example.org/api/samplingFeatures/4478",
            "title": "Thermometer Sampling Point"
        },
        "schema": {
            "obsFormat": "application/om+json",
            "resultTimeSchema": {
                "name": "time",
                "type": "Time",
                "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                "referenceFrame": "http://www.opengis.net/def/trs/BIPM/0/UTC",
                "label": "Sampling Time",
                "uom": {
                    "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                }
            },
            "resultSchema": {
                "name": "temp",
                "type": "Quantity",
                "definition": "http://mmisw.org/ont/cf/parameter/air_temperature",
                "label": "Room Temperature",
                "description": "Ambient air temperature measured inside the room",
                "uom": {
                    "code": "Cel"
                },
                "nilValues": [
                    {
                        "reason": "http://www.opengis.net/def/nil/OGC/0/missing",
                        "value": "NaN"
                    },
                    {
                        "reason": "http://www.opengis.net/def/nil/OGC/0/BelowDetectionRange",
                        "value": "-Infinity"
                    },
                    {
                        "reason": "http://www.opengis.net/def/nil/OGC/0/AboveDetectionRange",
                        "value": "+Infinity"
                    }
                ]
            }
        }
    }


def run():
    # get datastream id

    system = gen_system()
    post("/systems", system, "application/sml+json")

    datastream = gen_datastream()
    post(f"/systems/{system['id']}/datastreams", datastream)

    for i in range(1_000_000):
        obs = []
        for i in range(20):
            obs.append(gen_observation())
        post(f"/datastreams/{datastream['id']}/observations", obs, "application/om+json")
        # sleep(100 / 1000)


if __name__ == "__main__":
    run()
