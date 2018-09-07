"""
    Initialize the package
"""

from sanic import Sanic, Blueprint, response
from sanic.response import json
from sanic_cors import CORS, cross_origin
from io import BytesIO
import ujson

# import asyncio

app = Sanic()
CORS(app)
# loop = asyncio.get_event_loop()

from epivizws.requests import create_request

@app.route("/", methods=["POST", "OPTIONS", "GET"])
async def process_request(request):
    """
        routes the request to the appropriate function based on the request `action` parameter.

        Returns:
            JSON result
    """

    param_action = request.args.get("action")
    param_id = request.args.get("requestId")
    version = request.args.get("version")

    epiviz_request = create_request(param_action, request.args)
    result, error = await epiviz_request.get_data()

    return response.json({"requestId": int(param_id),
                            "type": "response",
                            "error": error,
                            "data": result,
                            "version": 5
                        },
                        status=200)
    