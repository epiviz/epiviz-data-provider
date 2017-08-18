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
        "getSeqInfo": SeqInfoRequest,
        "getMeasurements": MeasurementRequest,
        "getData": DataRequest,
        "getRows": DataRequest,
        "getValues": DataRequest,
        "getSummaryByegion": RegionSummaryRequest,
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
        return result.to_dict(), None

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
        return result.to_dict(), None

class DataRequest(EpivizRequest):
    """
        Data requests class
    """
    def __init__(self, request):
        super(DataRequest, self).__init__(request)
        self.params = self.validate_params(request)
        self.query = "select %s from %s where chr=%s and start >= %s and end < %s order by chr, start"

    def validate_params(self, request):
        params_keys = ["datasource", "seqName", "genome", "start", "end", "metadata", "measurement"]
        params = {}

        for key in params_keys:
            if request.has_key(key):
                params[key] = request.get(key)
                if key == "start" and params[key] is None:
                    params[key] = 0
                elif key == "end" and params[key] is None:
                    params[key] = sys.maxint
            else:
                raise Exception("missing params in request")
        return params

    def get_data(self):

        query_params = [str("chr, start, end, " + self.params.get("measurement")),
                        str(self.params.get("datasource")),
                        str(self.params.get("seqName")),
                        int(self.params.get("start")),
                        int(self.params.get("end"))]

        result = utils.execute_query(self.query, query_params)
        result = utils.bin_rows(result)

        return result.to_dict(), None

class RegionSummaryRequest(EpivizRequest):
    """
        Region summary requests class
    """
    def __init__(self, request):
        super(RegionSummaryRequest, self).__init__(request)
        self.validate_params(request)

    def validate_params(self, request):
        raise Exception("NotImplementedException")

    def get_data(self):
        raise Exception("NotImplementedException")
