// requires  @hyperjump/json-schema
// npm install @hyperjump/json-schema

import { addMediaTypePlugin, addSchema, validate } from "@hyperjump/json-schema/draft-07";
import { bundle } from "@hyperjump/json-schema/bundle";
import { readdirSync, readFileSync, writeFile } from "fs";
import { VERBOSE } from "@hyperjump/json-schema/experimental";

const baseUrl = "https://opengeospatial.github.io/ogcapi-connected-systems/"

function parse_directory(name) {
    readdirSync(`${name}`, { withFileTypes: true })
        .filter((entry) => entry.isFile() && entry.name.endsWith(".schema"))
        .forEach((entry) => {
            const file = `${name}/${entry.name}`;
            let schemaText = readFileSync(file, "utf8")
            let schema = JSON.parse(schemaText)
            console.log(`added schema ${schema["$id"]}`)
            addSchema(schema)
        });
}

let example_system = {

    "type": "PhysicalSystem",
    "id": "123",
    "definition": "http://www.w3.org/ns/sosa/Sensor",
    "uniqueId": "urn:x-ogc:systems:001",
    "label": "Outdoor Thermometer 001",
    "description": "Digital thermometer located on first floor window 1",
    "typeOf": 

{

    "href": "https://data.example.org/api/procedures/TP60S?f=sml",
    "uid": "urn:x-myorg:datasheets:ThermoPro:TP60S:v001",
    "title": "ThermoPro TP60S",
    "type": "application/sml+json"

},
"identifiers": 
[

    {
        "definition": "http://sensorml.com/ont/swe/property/SerialNumber",
        "label": "Serial Number",
        "value": "0123456879"
    }

],
"contacts": 
[

    {
        "role": "http://sensorml.com/ont/swe/roles/Operator",
        "organisationName": "Field Maintenance Corp."
    }

],
"position": 
{

    "type": "Point",
    "coordinates": 

        [
            41.8781,
            -87.6298
        ]
    }

}

parse_directory(".")

let result = await validate("https://opengeospatial.github.io/ogcapi-connected-systems/api/part1/openapi/schemas/sensorml/system.json", example_system, VERBOSE)
console.log(result)