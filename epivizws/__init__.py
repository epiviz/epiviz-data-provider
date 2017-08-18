"""
    Initialize the package
"""

from flask import Flask, request, jsonify, Response
from flask_sqlalchemy import SQLAlchemy
from epivizws.config import app_config

app = Flask(__name__)
app.config.from_object(app_config)
db = SQLAlchemy(app)

from epivizws.requests import create_request

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

    epiviz_request = create_request(param_action, request.values)
    result, error = epiviz_request.get_data()

    return jsonify(result)
    # return Response(response=jsonify({"id": param_id,
    #                                   "error": error,
    #                                   "result": result}),
    #                 mimetype="application/json")
    