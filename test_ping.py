import unittest

from ping import lambda_handler


class TestFormatLambdaHandler(unittest.TestCase):
    def test_ping(self):
        self.assertEqual("pong", lambda_handler({}, {}))

    def test_ping_always_pongs(self):
        self.assertEqual("pong", lambda_handler({'hallo': 'cheese'}, {}))
