# -*- coding: utf-8 -*-

from chalice import Chalice
from s3pathlib.lbd import hello

app = Chalice(app_name="s3pathlib")


@app.lambda_function(name="hello")
def handler_hello(event, context):
    return hello.high_level_api(event, context)
