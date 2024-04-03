# =================================================================
# Copyright (C) 2024 by 52 North Spatial Information Research GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =================================================================
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from datetime import datetime

from pygeoapi.provider.base import ProviderInvalidQueryError


class BBoxParam:
    bbox: Optional[Dict] = None


class GeomParam:
    geom: Optional[str] = None


@dataclass(slots=True)
class CSAParams:
    f: str = None  # format
    id: List[str] = None
    q: Optional[List[str]] = None
    limit: int = 10
    offset: int = 0  # non-standard

    def parameters(self):
        return [name for name in dir(self) if not name.startswith('_')]


@dataclass(slots=True)
class CommonParams(CSAParams):
    datetime: Tuple[Optional[datetime], Optional[datetime]] = None
    foi: Optional[List[str]] = None
    observedProperty: Optional[List[str]] = None

    def datetime_start(self):
        return self.datetime[0] if self.datetime else None

    def datetime_end(self):
        return self.datetime[1] if self.datetime else None


@dataclass(slots=True)
class CollectionParams(CSAParams, BBoxParam, GeomParam):
    datetime: Tuple[Optional[datetime], Optional[datetime]] = None


@dataclass(slots=True)
class SystemsParams(CommonParams, BBoxParam, GeomParam):
    parent: Optional[List[str]] = None
    procedure: Optional[List[str]] = None
    controlledProperty: Optional[List[str]] = None


@dataclass(slots=True)
class DeploymentsParams(CommonParams, BBoxParam, GeomParam):
    system: Optional[List[str]] = None


@dataclass(slots=True)
class ProceduresParams(CommonParams):
    controlledProperty: Optional[List[str]] = None


@dataclass(slots=True)
class SamplingFeaturesParams(CommonParams, BBoxParam, GeomParam):
    controlledProperty: Optional[List[str]] = None
    system: Optional[List[str]] = None


@dataclass(slots=True)
class DatastreamsParams(CommonParams):
    phenomenonTime: Tuple[Optional[datetime], Optional[datetime]] = None
    resultTime: Tuple[Optional[datetime], Optional[datetime]] = None
    system: Optional[List[str]] = None
    schema: Optional[bool] = None


@dataclass(slots=True)
class ObservationsParams(CommonParams):
    phenomenonTime: Tuple[Optional[datetime], Optional[datetime]] = None
    resultTime: Tuple[Optional[datetime], Optional[datetime]] = None
    system: Optional[List[str]] = None
    datastream: Optional[str] = None


def parse_query_parameters(out_parameters: CSAParams, input_parameters: Dict):
    def _parse_list(identifier):
        setattr(out_parameters,
                identifier,
                [elem for elem in input_parameters.get(identifier).split(",")])

    def _verbatim(key):
        setattr(out_parameters, key, input_parameters.get(key))

    def _parse_time(key):
        val = input_parameters.get(key)
        if "/" in val:
            setattr(out_parameters, key, _parse_time_interval(key))
        else:
            # TODO: check if more edge cases/predefined variables exist
            if val == "now":
                date = datetime.utcnow()
            else:
                date = datetime.fromisoformat(val)
            setattr(out_parameters, key, (date, date))

    def _parse_bbox(key):
        split = input_parameters.get("bbox").split(',')
        if len(split) == 4:
            box = {
                "type": "2d",
                "x1": split[0],  # Lower left corner, coordinate axis 1
                "x2": split[1],  # Lower left corner, coordinate axis 2
                "y1": split[2],  # Upper right corner, coordinate axis 1
                "y2": split[3]  # Upper right corner, coordinate axis 2
            }
        elif len(split) == 6:
            box = {
                "type": "3d",
                "x1": split[0],  # Lower left corner, coordinate axis 1
                "x2": split[1],  # Lower left corner, coordinate axis 2
                "xalt": split[2],  # Minimum value, coordinate axis 3 (optional)
                "y1": split[3],  # Upper right corner, coordinate axis 1
                "y2": split[4],  # Upper right corner, coordinate axis 2
                "yalt": split[5]  # Maximum value, coordinate axis 3 (optional)
            }
        else:
            raise ProviderInvalidQueryError("invalid bbox")
        setattr(out_parameters, "bbox", box)

    def _parse_time_interval(key):
        raw = input_parameters.get(key)
        # TODO: Support 'latest' qualifier
        now = datetime.utcnow()
        start, end = None
        if "/" in raw:
            # time interval
            split = raw.split("/")
            startts = split[0]
            endts = split[1]
            if startts == "now":
                start = now
            elif startts == "..":
                start = None
            else:
                start = datetime.fromisoformat(startts)
            if endts == "now":
                end = now
            elif endts == "..":
                end = None
            else:
                end = datetime.fromisoformat(endts)
        else:
            if raw == "now":
                start = now
                end = now
            else:
                start = raw
                end = raw
        setattr(out_parameters, key, (start, end))

    parser = {
        "id": _parse_list,
        "system": _parse_list,
        "parent": _parse_list,
        "q": _verbatim,
        "observedProperty": _parse_list,
        "procedure": _parse_list,
        "controlledProperty": _parse_list,
        "foi": _parse_list,
        "format": _verbatim,
        "f": _verbatim,
        "limit": _verbatim,
        "offset": _verbatim,
        "bbox": _parse_bbox,
        "datetime": _parse_time,
        "geom": _verbatim,
        "datastream": _verbatim,
        "phenomenonTime": _parse_time_interval,
        "resultTime": _parse_time_interval,
    }

    #  TODO: There must be a way to make this more efficient/straightforward..
    # Iterate possible parameters
    for p in input_parameters:
        # Check if parameter is supplied as input
        if p in out_parameters.parameters():
            # Parse value with appropriate mapping function
            parser[p](p)
        else:
            raise ProviderInvalidQueryError(f"unrecognized query parameter: {p}")

    return out_parameters


CSAGetResponse = Tuple[List[Dict], List[Dict]] | None
CSACrudResponse = List[str]


class ConnectedSystemsBaseProvider:
    """Base provider for Providers implemented Connected Systems API"""

    def __init__(self, provider_def):
        pass

    def get_conformance(self) -> List[str]:
        """Returns the list of conformance classes that are implemented by this provider"""
        return []

    def query_collections(self, parameters: CollectionParams) -> CSAGetResponse:
        """
        implements queries on collections as specified in openapi-connectedsystems-1

        :returns: dict of formatted collections matching the query parameters
        """

        raise NotImplementedError()

    def query_collection_items(self, collection_id: str, parameters: CSAParams) -> CSAGetResponse:
        """
        implements queries on items in collections as specified in openapi-connectedsystems-1

        :returns: dict of formatted systems matching the query parameters
        """

        raise NotImplementedError()

    def query_systems(self, parameters: SystemsParams) -> CSAGetResponse:
        """
        implements queries on systems as specified in openapi-connectedsystems-1

        :returns: dict of formatted systems matching the query parameters
        """

        raise NotImplementedError()

    def query_deployments(self, parameters: DeploymentsParams) -> CSAGetResponse:
        """
        implements queries on deployments as specified in openapi-connectedsystems-1

        :returns: dict of formatted deployments matching the query parameters
        """

        raise NotImplementedError()

    def query_procedures(self, parameters: ProceduresParams) -> CSAGetResponse:
        """
        implements queries on procedures as specified in openapi-connectedsystems-1

        :returns: dict of formatted procedures matching the query parameters
        """

        raise NotImplementedError()

    def query_sampling_features(self, parameters: SamplingFeaturesParams) -> CSAGetResponse:
        """
        implements queries on samplingFeatures as specified in openapi-connectedsystems-1

        :returns: dict of formatted samplingFeatures matching the query parameters
        """

        raise NotImplementedError()

    def query_properties(self, parameters: CSAParams) -> CSAGetResponse:
        """
        implements queries on properties as specified in openapi-connectedsystems-1

        :returns: dict of formatted properties
        """

        raise NotImplementedError()

    def query_datastreams(self, parameters: DatastreamsParams) -> CSAGetResponse:
        """
        implements queries on datastreams as specified in openapi-connectedsystems-2

        :returns: dict of formatted properties
        """

        raise NotImplementedError()

    def query_observations(self, parameters: ObservationsParams) -> CSAGetResponse:
        """
        implements queries on observations as specified in openapi-connectedsystems-2

        :returns: dict of formatted properties
        """

        raise NotImplementedError()

    def create(self, type: str, item) -> CSACrudResponse:
        """
        Create a new item

        :param item: `dict` of new item

        :returns: identifier of created item
        """

        raise NotImplementedError()

    def update(self, identifier, item):
        """
        Updates an existing item

        :param identifier: feature id
        :param item: `dict` of partial or full item

        :returns: `bool` of update result
        """

        raise NotImplementedError()

    def delete(self, identifier):
        """
        Deletes an existing item

        :param identifier: item id

        :returns: `bool` of deletion result
        """

        raise NotImplementedError()
