# =================================================================
#
# Authors: Jan Speckamp <j.speckamp@52North.org>
#
# Copyright (c) 2023 Jan Speckamp
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================
from datetime import datetime, timezone
from typing import List, Optional, Dict, Type, Tuple

from pygeoapi.provider.base import ProviderInvalidQueryError


class CSAParams:
    f: str = None  # format
    id: List[str] = None
    q: Optional[List[str]] = None
    limit: int = 10
    offset: int = 0  # non-standard

    def parameters(self):
        return [name for name in dir(self) if not name.startswith('_')]


class CommonParams(CSAParams):
    datetimeStart: Optional[datetime] = None
    datetimeEnd: Optional[datetime] = None
    foi: Optional[List[str]] = None
    observedProperty: Optional[List[str]] = None

    def parameters(self):
        return super().parameters() + ["datetime"]


class CollectionParams(CSAParams):
    datetimeStart: Optional[datetime] = None
    datetimeEnd: Optional[datetime] = None
    bbox: Optional[Dict] = None
    geom: Optional[str] = None

    def parameters(self):
        return super().parameters() + ["datetime"]


class SystemsParams(CommonParams):
    bbox: Optional[Dict] = None
    geom: Optional[str] = None
    parent: Optional[List[str]] = None
    procedure: Optional[List[str]] = None
    controlledProperty: Optional[List[str]] = None


class DeploymentsParams(CommonParams):
    bbox: Optional[Dict] = None
    geom: Optional[str] = None
    system: Optional[List[str]] = None


class ProceduresParams(CommonParams):
    controlledProperty: Optional[List[str]] = None


class SamplingFeaturesParams(CommonParams):
    bbox: Optional[Dict] = None
    geom: Optional[str] = None
    controlledProperty: Optional[List[str]] = None
    system: Optional[List[str]] = None


class DatastreamsParams(CommonParams):
    phenomenonTimeStart: Optional[datetime] = None
    phenomenonTimeEnd: Optional[datetime] = None
    resultTimeStart: Optional[datetime] = None
    resultTimeEnd: Optional[datetime] = None
    system: Optional[List[str]] = None
    schema: Optional[bool] = None


class ObservationsParams(CommonParams):
    phenomenonTimeStart: Optional[datetime] = None
    phenomenonTimeEnd: Optional[datetime] = None
    resultTimeStart: Optional[datetime] = None
    resultTimeEnd: Optional[datetime] = None
    system: Optional[List[str]] = None
    datastream: Optional[str] = None

    def parameters(self):
        return super().parameters() + ["phenomenonTime", "resultTime"]


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
            setattr(out_parameters, key + "Start", date)
            setattr(out_parameters, key + "End", date)

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
        start = key + "Start"
        end = key + "End"
        # TODO: Support 'latest' qualifier
        now = datetime.utcnow()
        if "/" in raw:
            # time interval
            split = raw.split("/")
            startts = split[0]
            endts = split[1]
            if startts == "now":
                setattr(out_parameters, start, now)
            elif startts == "..":
                setattr(out_parameters, start, None)
            else:
                setattr(out_parameters, start, datetime.fromisoformat(startts))
            if endts == "now":
                setattr(out_parameters, end, now)
            elif endts == "..":
                setattr(out_parameters, end, None)
            else:
                setattr(out_parameters, end, datetime.fromisoformat(endts))
        else:
            if raw == "now":
                setattr(out_parameters, start, now)
                setattr(out_parameters, end, now)
            else:
                setattr(out_parameters, start, raw)
                setattr(out_parameters, end, raw)

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
