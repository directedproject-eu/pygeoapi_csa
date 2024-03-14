// =================================================================
// Copyright (C) 2024 by 52 North Spatial Information Research GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================

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