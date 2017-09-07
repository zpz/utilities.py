from utilities.testing import config_test
from utilities.email import Mailer

config_test()

send_from = 'abc@my.com'
send_to = 'def@gmail.com'

def test_mailer():
    print('initiate Mailer from "{}" to "{}"'.format(send_from, send_to))
    ff = Mailer('abc', send_from, send_to)
    print('  sending email... ...')
    ff.send(subject="a test!", text="test more")

    print('  sending alert email... ...')
    try:
        raise Exception('artificial error for testing alert email')
    except Exception as e:
        ff.send_error(e)


def test_mail_on_the_fly():
    Mailer('abc').send(
        send_from=send_from,
        send_to=send_to,
        subject='a test on the fly',
        text='just a test',
    )
