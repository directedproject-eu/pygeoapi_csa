import json
import struct
import uuid
from datetime import datetime

import asyncpg

from ..util import SchemaParser, Observation

schema = {
    "obsFormat": "application/om+json",
    "resultTime": {
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

example = {
    "resultTime": "2021-03-15T04:53:34Z",
    "result": 23.5
}


class OMJsonSchemaParser(SchemaParser):
    uuid = "472c6406-1f0e-4b80-80d7-750655e0f87e"

    # Example:

    def decode(self, data: any) -> Observation:
        return Observation(
            datastream_id=data["datastream"],
            resultTime=datetime.fromisoformat(data["resultTime"]),
            result=struct.pack("!f", data["result"]),
        )

    def encode(self, obs: asyncpg.Record) -> any:
        # TODO(specki): Check what is officially required here, e.g. are datastream@link or foi@link necessary?
        return {
            "id": str(obs["uuid"]),
            "datastream@id": str(obs["datastream_id"]),
            "resultTime": obs["resulttime"],
            "result": struct.unpack("!f", obs["result"])
        }
