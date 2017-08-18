"""
    API Router for epiviz web services
"""

from flask import request, jsonify, Response
from epivizws.requests import create_request
from epivizws import app

@app.route("/", methods=["POST", "OPTIONS", "GET"])
def process_request():
    """
        routes the request to the appropriate function based on the request `action` parameter.

        Returns:
            JSON result
    """

    if request.method == "OPTIONS":
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = '*'
        return res

    param_action = request.values.get("action")
    param_id = request.values.get("requestId")
    version = request.values.get("version")

    request = create_request(param_action, request.values)
    result, error = request.get_data()

    return Response(response=jsonify({"id": param_id,
                                      "error": error,
                                      "result": result}),
                    mimetype="application/json")
