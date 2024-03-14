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

const baseUrl = "https://opengeospatial.github.io/ogcapi-connected-systems/"

let datastream_schema = {}

addMediaTypePlugin("application/json", {
    parse: async (response) => [JSON.parse(await response.text()), undefined],
    matcher: (path) => path.endsWith(".json")
});

function parse_directory(name) {
    readdirSync(`${name}`, { withFileTypes: true })
        .filter((entry) => entry.isFile() && entry.name.endsWith(".json"))
        .forEach((entry) => {
            const file = `${name}/${entry.name}`;
            let schemaText = readFileSync(file, "utf8")
            const re = /\$ref":\s?"([^"]+)"/g
            let refs = schemaText.matchAll(re)

            // Make all $ref links absolute
            for (const match of refs) {
                let ref = match[1]

                let replacement = ref
                // normalize $ref to use absolute urls
                if (ref.startsWith("#") || ref.startsWith("http")) {
                    // same document - nothing to do
                } else if (ref.startsWith(".")) {
                    // higher level
                    replacement = new URL(ref, baseUrl + name + "/").href
                } else if (ref.startsWith("sensormlDefs.json#/definitions/")) {
                    // special case because defs-collector class is optimized out during bundling
                    replacement =  baseUrl + "sensorml/schemas/json/" + ref.substring(31) + ".json"
                } else if (ref.startsWith("commonDefs.json#/definitions/")) {
                    // special case because defs-collector class is optimized out during bundling
                    if (name.startsWith("sensorml")) {
                        replacement = baseUrl + "sensorml/schemas/json/commonDefs.json#/definitions/" + ref.substring(29)
                    } else {
                        // fix inconsistent naming with upper & lowercase
                        replacement = baseUrl + "common/" + ref.substring(29, 30).toLowerCase() + ref.substring(30) + ".json"
                    }
                } else {
                    // Import by name
                    replacement = baseUrl + name + "/" + ref

                }
                schemaText = schemaText.replace(match[0], `$ref":"${replacement}"`)

                /*
                console.log(name)
                console.log(match[0])
                console.log(replacement)
                console.log()
                console.log()
                */
            }

            const suite = JSON.parse(schemaText);
            suite["$id"] = baseUrl + name + "/" + entry.name;

            if (entry.name.includes("DataStream")) {
                datastream_schema = suite
            }
            // console.log(`registered schema: ${suite["$id"]}`)
            addSchema(suite)
        });
}

async function get_bundle(uri, fileName) {
    const bundledSchema = await bundle(uri, {
        //bundleMode: 'full',
        //externalSchemas: ["https://geojson.org/schema/"]
    });

    // fix DataStream not being included due to no usage
    bundledSchema["definitions"][datastream_schema["$id"]] = datastream_schema

    writeFile(fileName, JSON.stringify(bundledSchema), err => {
        if (err) {
            console.error(err);
        }
    });
}

var base = 'ogcapi-connected-systems/'
parse_directory(base + 'common')
parse_directory(base + 'sensorml/schemas/json')
parse_directory(base + 'swecommon/schemas/json')
parse_directory(base + 'api/part1/openapi/schemas/common')
parse_directory(base + 'api/part1/openapi/schemas/sensorml')

await (get_bundle(baseUrl + 'api/part1/openapi/schemas/sensorml/system.json', "bundles/system.schema"))
await (get_bundle(baseUrl + 'api/part1/openapi/schemas/sensorml/deployment.json', "bundles/deployment.schema"))
await (get_bundle(baseUrl + 'api/part1/openapi/schemas/sensorml/procedure.json', "bundles/procedure.schema"))
await (get_bundle(baseUrl + 'api/part1/openapi/schemas/geojson/anySamplingFeature.json', "bundles/samplingFeature.schema"))
await (get_bundle(baseUrl + 'api/part1/openapi/schemas/sensorml/property.json', "bundles/property.schema"))