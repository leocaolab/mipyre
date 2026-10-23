"""App for tests/test_health.py::test_livez_answers_past_a_hook_that_refuses_the_rest. Its own
module: a worker serves one app per module."""

from mipyre import HealthProbes, Mipyre, Response

app = Mipyre()
HealthProbes(app)


@app.before_request
def refuse(req):
    return Response(body=b"no credentials", status_code=401)
