"""Collection of request classes"""

import epivizws.utils as utils
from flask import request
import ujson
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os

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
        "getCombined": DataRequest,
        "getRows": DataRequest,
        "getValues": DataRequest,
        "getSummaryByRegion": RegionSummaryRequest,
        "getScreenshot": ScreenshotRequest
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

        return genome, None

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

        # metadata = result["metadata"].apply(lambda x: ujson.loads(x))
        annotation = []

        for anno in result["annotation"].values.tolist():
            if anno is not None:
                anno = ujson.loads(anno)
            annotation.append(anno)
            
        measurements = {
            "annotation": annotation,
            "datasourceGroup": result["location"].values.tolist(),
            "datasourceId": result["location"].values.tolist(),
            "defaultChartType": result["chart_type"].values.tolist(),
            # "id": result["measurement_id"].values.tolist(),
            "id": result["column_name"].values.tolist(),
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
        self.query = "select distinct %s from %s where chr=%s and end >= %s and start < %s order by chr, start"

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
                elif key == "metadata[]":
                    del params["metadata[]"]
                    params["metadata"] = request.getlist(key)
                elif key == "measurement":
                    # del params["measurement"]
                    params["measurement"] = params["measurement"].split(",")
            else:
                if key not in ["measurement", "genome", "metadata[]"]:
                    raise Exception("missing params in request")
        return params

    def get_data(self):

        if self.params.get("measurement") is None:
            query_ms = "id, chr, start, end "
        else:
            measurement = ", ".join(self.params.get("measurement"))
            query_ms = "id, chr, start, end, " + measurement
            # query_ms = "id, chr, start, end, " + self.params.get("measurement")

        if self.params.get("metadata"):
            metadata = ", ".join(self.params.get("metadata"))
            if metadata != "[]":
                query_ms = query_ms + ", " + metadata

        if self.params.get("datasource") == "genes":
            query_params = [
                str(query_ms) + ", strand",
                str(self.params.get("datasource")),
                "'" + str(self.params.get("seqName")) + "'",
                int(self.params.get("start")),
                int(self.params.get("end"))]

            result = utils.execute_query(self.query, query_params)
        else:
            query_params = [
                str(query_ms),
                str(self.params.get("datasource")),
                "'" + str(self.params.get("seqName")) + "'",
                int(self.params.get("start")),
                int(self.params.get("end"))]

            result = utils.execute_query(self.query, query_params)

        if self.request.get("action") == "getRows":
            data = utils.format_result(result, self.params)
            return data["rows"], None
        elif self.request.get("action") == "getValues":
            result = utils.bin_rows(result)
            data = utils.format_result(result, self.params)
            return data, None
        else:
            data = utils.format_result(result, self.params)
            return data, None

class RegionSummaryRequest(EpivizRequest):
    """
        Region summary requests class
    """
    def __init__(self, request):
        super(RegionSummaryRequest, self).__init__(request)
        self.params = self.validate_params(request)
        self.query = "select distinct %s from %s "
        # where chr=%s and start >= %s and end < %s order by chr, start"

    def validate_params(self, request):
        params_keys = ["datasource", "metadata[]", "measurement", "regions"]
        params = {}

        for key in params_keys:
            if request.has_key(key):
                params[key] = request.get(key)
                if key == "start" and params[key] in [None, ""]:
                    params[key] = 1
                elif key == "end" and params[key] in [None, ""]:
                    params[key] = sys.maxint
                elif key == "regions":
                    params[key] = eval(params[key])
                elif key == "metadata[]":
                    del params["metadata[]"]
                    params["metadata"] = request.getlist(key)
            else:
                if key not in ["measurement", "genome", "metadata[]"]:
                    raise Exception("missing params in request")
        return params

    def get_data(self):
        # query_ms = ",".join(self.params.get("measurement"))
        if self.params.get("measurement") is None:
            query_ms = "id, chr, start, end "
        else:
            query_ms = "id, chr, start, end, " + self.params.get("measurement")

        if self.params.get("metadata"):
            metadata = ", ".join(self.params.get("metadata"))
            query_ms = query_ms + ", " + metadata

        if self.params.get("regions"):
            regions = []
            query_reg = " (chr = %s AND start >= %s AND end < %s) "
            for reg in self.params.get("regions"):
                qreg = query_reg % ("'" + reg["seqName"] + "'", reg["start"], reg["end"])
                regions.append(qreg)

            query_regions = " OR ".join(regions)
            self.query = self.query + " WHERE " + query_regions

        self.query = self.query + " order by chr, start"

        print self.query

        globalStartIndex = None
        query_params = [
            str(query_ms),
            str(self.params.get("datasource"))
            ]

        result = utils.execute_query(self.query, query_params)
        total_length = len(result)

        data = utils.format_result(result, self.params)

        if self.request.get("action") == "getRows":
            return data["rows"], None
        elif self.request.get("action") == "getValues":
            return data["values"], None
        else:
            return data, None

class ScreenshotRequest(EpivizRequest):
    """
        Region summary requests class
    """
    def __init__(self, request):
        super(ScreenshotRequest, self).__init__(request)
        self.chromePath = ""
        self.params = request

    def get_data(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_driver = os.getcwd() + "/epivizws/chromedriver"

        driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)
        driver.implicitly_wait(500)
        driver.set_page_load_timeout(500)
        driver.get("https://epiviz.cbcb.umd.edu/4/?ws=" + self.params.get("workspaceId"))
        # driver.get("http://epiviz.cbcb.umd.edu/4/?ws=qg1K0lJ29cs&useCookie=true&seqName=chr11&start=1&end=135086622&settings=default&")
        respImage = driver.get_screenshot_as_png()

        return respImage, None
