'''
Use a 'bot' to post message to a Slack channel.
This is useful for error alerts and scheduled notifications.
'''

import json
import threading
import urllib.request

import arrow

# Reference:
# search for 'incoming webhooks for slack'

def post(channel_webhook_url: str, subject: str, text: str) -> None:
    dt = arrow.utcnow().to('US/Pacific').format('YYYY-MM-DD HH:mm:ss') + ' Pacific'
    json_data = json.dumps({
        'text': '--- {} ---\n{}\n{}'.format(subject, dt, text)
        }).encode('ascii')
    req = urllib.request.Request(
        channel_webhook_url, data=json_data, 
        headers={'Content-type': 'application/json'})
    thr = threading.Thread(target=urllib.request.urlopen, args=(req, ))
    thr.start()
