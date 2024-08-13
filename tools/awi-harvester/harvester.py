import json

import requests

# Manually extracted via: https://registry.o2a-data.de/items?q=vessel&qf=metadata.typeName/vessel

ROOT_ITEMS = [
    "90",
    "93",
    "456",
    "506",
    "2851",
    "2873",
    "3846",
    "5007",
    "6072",
    "6357",
    "7108",
    "7601",
    "7676",
    "7900",
    "7999",
    "8045",
    "9140",
    "9198",
    "11658",
    "11662",
]


def post(path: str, payload: dict, content_type: str = "application/json"):
    url = "http://localhost:5000/" + path
    headers = {"Content-Type": content_type}
    response = requests.request("POST", url, json=payload, headers=headers)
    if response.status_code != 204:


def get_item(id: str) -> dict:
    a = requests.get(f'https://registry.o2a-data.de/rest/v2/items/{id}?with=children.id&with=contacts&with=events')
    return json.loads(a.content)


def get_missions():
    a = requests.get('https://registry.o2a-data.de/rest/v2/missions?hits=1')
    return json.loads(a.content)["records"]


def get_parameters(id: str) -> list:
    a = requests.get(f"https://registry.o2a-data.de/rest/v2/items/{id}/parameters")
    response = json.loads(a.content)["records"]

    unit_cache = {}
    type_cache = {}
    parsed = []
    for item in response:
        unit = item["unit"]
        type = item["type"]
        unit_uuid = unit if isinstance(unit, str) else unit["@uuid"]
        type_uuid = type if isinstance(type, str) else type["@uuid"]
        if not unit_uuid in unit_cache:
            unit_cache[unit_uuid] = unit
        if not type_uuid in type_cache:
            type_cache[type_uuid] = type

        unit = unit_cache[unit_uuid]
        type = type_cache[type_uuid]

        parsed.append({
            "name": f"AWI_{sanitize(item['shortName'])}",
            "id": str(item['id']),
            "label": item['name'],
            "type": "ObservableProperty",
            "definition": type["vocableValue"] if type["vocableValue"].startswith(
                "http") else f"awi:{sanitize(type['vocableValue'])}"
        })

    return parsed


def get_contacts(id: str):
    a = requests.get(f"https://registry.o2a-data.de/rest/v2/items/{id}/contacts")
    response = json.loads(a.content)["records"]

    contact_cache = {}
    role_cache = {}
    parsed = []
    for item in response:
        contact = item["contact"]
        role = item["role"]
        contact_uuid = contact if isinstance(contact, str) else contact["@uuid"]
        role_uuid = role if isinstance(role, str) else role["@uuid"]
        if not contact_uuid in contact_cache:
            contact_cache[contact_uuid] = contact
        if not role_uuid in role_cache:
            role_cache[role_uuid] = role

        contact = contact_cache[contact_uuid]
        role = role_cache[role_uuid]

        parsed.append({
            "role": f"awi:{role['systemName']}",
            "name": contact["firstName"] + " " + contact["lastName"],
            "link": {
                "href": f"https://registry.o2a-data.de/rest/v2/items/{id}/contacts/{contact['id']}"
            }
        })

    return parsed


deployments = {}
event_cache = {}
event_type_cache = {}


def parse_events(item_id: str, item_name: str) -> list:
    a = requests.get(f"https://registry.o2a-data.de/rest/v2/items/{item_id}/events")
    response = json.loads(a.content)["records"]

    history_events = []
    for event in response:
        event_uuid = event if isinstance(event, str) else event["@uuid"]
        if not event_uuid in event_cache:
            event_cache[event_uuid] = event
        event = event_cache[event_uuid]

        event_type_uuid = event["type"] if isinstance(event["type"], str) else event["type"]["@uuid"]
        if not event_type_uuid in event_type_cache:
            event_type_cache[event_type_uuid] = event["type"]
        event_type = event_type_cache[event_type_uuid]

        match event_type["systemName"]:
            case "Deployment":
                # We handle these centrally so only cache for now
                deployment_id = event["@uuid"]
                if deployment_id in deployments:
                    deployments[deployment_id]["deployedSystems"].append(
                        {
                            "name": item_name,
                            "system": {
                                "href": f"/systems{item_id}"
                            }
                        }
                    )
                else:
                    new_deployment = {
                        "type": "Deployment",
                        "definition": "http://www.w3.org/ns/sosa/Deployment",
                        "id": str(event["id"]),
                        "uniqueId": f"urn:uuid:{event['@uuid']}",
                        "label": event['label'],
                        "validTime": [
                            event["startDate"],
                            event["endDate"]
                        ],
                        "deployedSystems": [
                            {
                                "name": item_name,
                                "system": {
                                    "href": f"/systems/{item_id}"
                                }
                            }
                        ]
                    }
                    if "description" in event and len(event["description"]) > 0:
                        new_deployment["description"] = event["description"]

                    if "latitude" in event:
                        new_deployment["location"] = {
                            "type": "Point",
                            "coordinates": [
                                event["latitude"],
                                event["longitude"]
                            ]
                        }
                        if "elevation" in event:
                            new_deployment["location"]["coordinates"].append(event["elevation"])
                    deployments[deployment_id] = new_deployment
            case _:
                new_event = {
                    "id": str(event["id"]),
                    "label": event["label"],
                    "identifiers": [
                        {
                            "label": "uuid",
                            "value": event["@uuid"]
                        }
                    ],
                    "documentation": [
                        {
                            "name": "source",
                            "link": {
                                "href": f"https://registry.o2a-data.de/rest/v2/items/{item_id}/events/{event['id']}"
                            }
                        }
                    ],
                    "time": [
                        event["startDate"],
                        event["endDate"]
                    ]
                }
                if "description" in event and len(event["description"]) > 0:
                    new_event["description"] = event["description"]
                history_events.append(new_event)
    return history_events


def parse_system_sml(item_id: str, parent_id: str = None) -> dict:
    item = get_item(item_id)

    new_system = {
        "type": "PhysicalSystem",
        "definition": "http://www.w3.org/ns/sosa/Platform",
        "id": str(item["id"]),
        "uniqueId": f"urn:uuid:{item['@uuid']}",
        "label": item["code"],
        "identifiers": [
            {
                "label": "shortName",
                "value": item["shortName"],
            },
            {
                "label": "longName",
                "value": item["longName"],
            }
        ],
        "history": parse_events(str(item_id), "awi__" + sanitize(item["shortName"])),
        # "contacts": get_contacts(id),

    }
    if "description" in item and len(item["description"]) > 0:
        new_system["description"] = item["description"]
    parsed_parameters = get_parameters(item_id)
    if len(parsed_parameters) > 0:
        new_system["parameters"] = parsed_parameters

    # DEBUG:
    # parse_events(str(item_id), f"awi:{item['shortName']}")

    if parent_id:
        path = f"systems/{parent_id}/subsystems"
    else:
        path = "systems"
    post(path, new_system, "application/sml+json")

    for child in item["children"]:
        parse_system_sml(child["id"], item_id)


def harvest():
    for vessel_id in ROOT_ITEMS:
        parse_system_sml(vessel_id)

        # Post deployments after-the-fact
        for deployment_id in deployments:
            post("deployments", deployments[deployment_id], "application/sml+json")


def sanitize(dirty: str):
    return dirty.replace(" ", "_")


if __name__ == "__main__":
    harvest()
