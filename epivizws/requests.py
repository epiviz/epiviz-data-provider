"""Collection of request classes"""

import epivizws.utils as utils
from flask import request

def create_request(action, request):
    """
        Create appropriate request class based on action

        Args:
            action : Type of request
            request : Other request parameters

        Returns:
            An instance of EpivizRequest class
    """

    req_manager = {
        "getSeqInfos": SeqInfoRequest,
        "getMeasurements": MeasurementRequest,
        "getData": DataRequest,
        "getRows": DataRequest,
        "getValues": DataRequest,
        "getSummaryByRegion": RegionSummaryRequest,
    }

    return req_manager[action](request)

class EpivizRequest(object):
    """
        Base class to process requests
    """

    def __init__(self, request):
        self.request = request
        self.params = None
        self.query = None

    def validate_params(self, request):
        """
            Validate parameters for requests
            
            Args:
                request: dict of params from request
        """
        raise Exception("NotImplementedException")

    def get_data(self):
        """
            Get Data for this request type

            Returns:
                result: JSON response for this request
                error: HTTP ERROR CODE
        """
        raise Exception("NotImplementedException")

class SeqInfoRequest(EpivizRequest):
    """
        SeqInfo requests class
    """

    def __init__(self, request):
        super(SeqInfoRequest, self).__init__(request)
        self.params = self.validate_params(request)
        self.query = "select * from genome"

    def validate_params(self, request):
        return None

    def get_data(self):
        result = utils.execute_query(self.query, self.params)

        builds = result.groupby("genome")
        genome = {}

        for name, group in builds:
            genome[name] = []
            for index, row in group.iterrows():
                genome[name].append([row["chr"], 1, row["seqlength"]])

        return genome["hg19"], None

class MeasurementRequest(EpivizRequest):
    """
        Measurement requests class
    """
    def __init__(self, request):
        super(MeasurementRequest, self).__init__(request)
        self.params = self.validate_params(request)
        self.query = "select * from measurements_index"

    def validate_params(self, request):
        return None

    def get_data(self):
        result = utils.execute_query(self.query, self.params)

        measurements = {
            "annotation": result["annotation"].values.tolist(),
            "datasourceGroup": result["location"].values.tolist(),
            "datasourceId": result["location"].values.tolist(),
            "defaultChartType": result["chart_type"].values.tolist(),
            "id": result["measurement_id"].values.tolist(),
            "maxValue": result["max_value"].values.tolist(),
            "minValue": result["min_value"].values.tolist(),
            "name": result["measurement_name"].values.tolist(),
            "type": result["type"].values.tolist(),
            "metadata": result["metadata"].values.tolist()
        }

        return measurements, None

class DataRequest(EpivizRequest):
    """
        Data requests class
    """
    def __init__(self, request):
        super(DataRequest, self).__init__(request)
        self.params = self.validate_params(request)
        self.query = "select %s from %s where chr=%s and start >= %s and end < %s order by chr, start"

    def validate_params(self, request):
        params_keys = ["datasource", "seqName", "genome", "start", "end", "metadata[]", "measurement"]
        params = {}

        for key in params_keys:
            if request.has_key(key):
                params[key] = request.get(key)
                if key == "start" and params[key] in [None, ""]:
                    params[key] = 1
                elif key == "end" and params[key] in [None, ""]:
                    params[key] = sys.maxint
                elif key == "metadata":
                    params[key] = eval(params[key])
            else:
                if key not in ["measurement", "genome", "metadata[]"]:
                    raise Exception("missing params in request")
        return params

    def get_data(self):

        # query_ms = ",".join(self.params.get("measurement"))
        if self.params.get("measurement") is None:
            query_ms = "chr, start, end "
        else:
            query_ms = "chr, start, end, " + self.params.get("measurement")

        if self.params.get("datasource") == "genes":
            query_params = [
                    "*",
                    str(self.params.get("datasource")),
                    "'" + str(self.params.get("seqName")) + "'",
                    int(self.params.get("start")),
                    int(self.params.get("end"))]

            result = utils.execute_query(self.query, query_params)
        else:
            query_params = [str(query_ms),
                    str(self.params.get("datasource")),
                    "'" + str(self.params.get("seqName")) + "'",
                    int(self.params.get("start")),
                    int(self.params.get("end"))]

            result = utils.execute_query(self.query, query_params)
            result = utils.bin_rows(result)

        globalStartIndex = None
        if len(result) > 0:
            globalStartIndex = result["start"].values.min()

        data = {
            "rows": {
                "globalStartIndex": globalStartIndex,
                "useOffset" : False,
                "values": {
                    "id": None,
                    "strand": None,
                    "metadata": {}
                }
            },
            "values": {
                "globalStartIndex": globalStartIndex,
                "values": {}
            }
        }

        if len(result) > 0:
            col_names = result.columns.values.tolist()
            row_names = ["chr", "start", "end", "strand", "id"]

            for col in col_names:
                if self.params.get("measurement") is not None and col in self.params.get("measurement"):
                    data["values"]["values"] = result[col].values.tolist()
                elif col in row_names:
                    data["rows"]["values"][col] = result[col].values.tolist()
                else:
                    data["rows"]["values"]["metadata"][col] = result[col].values.tolist()

        if self.request.get("action") == "getRows":
            return data["rows"], None
        elif self.request.get("action") == "getValues":
            return data["values"], None
        else:
            return data, None

class RegionSummaryRequest(EpivizRequest):
    """
        Region summary requests class
    """
    def __init__(self, request):
        super(RegionSummaryRequest, self).__init__(request)
        self.params = self.validate_params(request)

    def validate_params(self, request):
        raise Exception("NotImplementedException")

    def get_data(self):
        raise Exception("NotImplementedException")
