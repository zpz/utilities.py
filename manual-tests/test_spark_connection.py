import json
import os
import time

import requests

# This test is intended to be run both inside and outside of Docker
# to verify basic connection to the Livy server.

# To run this test on a Mac machine, make sure a Livy server
# is running at the machine specified by $LIVY_SERVER_URL

livy_server_url = os.environ['LIVY_SERVER_URL']

host = 'http://' + livy_server_url
headers = {'Content-Type': 'application/json'}
headers_csrf = {'Content-Type': 'application/json', 'X-Requested-By': 'ambari'}


def _wait(url, wait_for_state):
    while True:
        r = requests.get(url, headers=headers)
        print()
        print('in _wait')
        print('    r:    ', r)
        print('    __dict__:    ', r.__dict__)
        rr = r.json()
        print('    json:    ', rr)
        print('    state:    ', rr['state'])
        if rr['state'] == wait_for_state:
            return rr
        time.sleep(2)


def _post(url, data):
    r = requests.post(url, data=json.dumps(data), headers=headers_csrf)
    print()
    print('in _post')
    print('    headers:    ', r.headers)
    print('    __dict__:    ', r.__dict__)
    print('    location:    ', r.headers['location'])
    return r


def test_scala():
    data = {'kind': 'spark'}
    r = _post(host + '/sessions', data)
    location = r.headers['location']
    if location.startswith('null/'):
        location = location[4:]
    assert location.startswith('/')

    session_url = host + location
    _wait(session_url, 'idle')

    data = {'code': '1 + 1'}
    r = _post(session_url + '/statements', data)

    statements_url = host + r.headers['location']
    rr = _wait(statements_url, 'available')
    print()
    print('output:  ', rr['output'])
    assert rr['output']['status'] == 'ok'
    assert rr['output']['data']['text/plain'] == 'res0: Int = 2'

    # Close the session
    requests.delete(session_url, headers=headers_csrf)


if __name__ == '__main__':
    test_scala()