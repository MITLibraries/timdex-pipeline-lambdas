import ping


def test_ping():
    assert ping.lambda_handler({}, {}) == "pong"


def test_ping_always_pongs():
    assert ping.lambda_handler({'hallo': 'cheese'}, {}) == "pong"
