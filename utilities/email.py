from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.encoders import encode_base64
import smtplib
import os
import threading


def _generate_msg(*, send_from, send_to, subject, text, files=None):
    """
    Args:
        send_to: comma-separated email addresses.
        files: either a list of full-path file names,
            or a list of tuples, each tuple containing
            file name (no path; intended to be meaningful to the email receiving party)
            and the file content as a binary blob.
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['Reply-to'] = send_from
    msg['To'] = send_to
    msg['Subject'] = subject
    msg.attach(MIMEText(text, 'plain'))

    if files is None:
        files = []
    assert isinstance(files, list)
    if files is None:
        files = []
    for f in files:
        part = MIMEBase('application', "octet-stream")
        if isinstance(f, str):
            part.set_payload( open(f,"rb").read() )
            encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        else:
            assert isinstance(f, tuple)
            part.set_payload( f[1] )
            encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="{}"'.format(f[0]))

        msg.attach(part)

    return msg


def _send_mail(server, msg):
    server.sendmail(msg['From'], msg['To'].split(','), msg.as_string())


class Mailer(object):
    def __init__(self, *, host, port, account, passwd):
        """
        Args:
            host: SMTP mail server.
            port: port number; an `int`, but can be a `str` (will be converted in the function).
            account: email account on the server.
            passwd: password for `account` on `host`.

        If you use a Gmail account to send emails, use
        'smtp.gmail.com' for `host` and 587 for `port`.
        """
        self._mail_server_args = dict(
            host=host, port=int(port), account=account, passwd=passwd)

    def _connect(self):
        """
        Todo:
            Should we establish connection upon object initiation?
            For how long will the connection be active?
        """
        # try:
        #     if 'accepting connections' in sh.Command('sudo')('/etc/init.d/sendmail', 'status', __tty_out=False):
        #         server = smtplib.SMTP('localhost', timeout=timeout)
        #         server.ehlo_or_helo_if_needed()
        #         return server
        #     raise Exception('no SMTP server on localhost')
        # except Exception as e:
        # Had trouble using 'sudo' with password passing in.
        server = smtplib.SMTP(self._mail_server_args['host'], self._mail_server_args['port'])
        #server.ehlo()
        server.starttls()
        #server.ehlo()
        server.login(self._mail_server_args['account'], self._mail_server_args['passwd'])
        return server

    def send(self, *, send_from, send_to, subject, text, files=None):
        """
        Args:
            send_from: an email address.
            send_to: a string of comma-separated email addresses.
            files (list): names of files to attach, with full path.

        If Gmail server is used, the 'From' field of the actually sent email will be
        the email address specified by ``self._mail_server_args['account']``,
        and not the ``send_from`` here,
        but we're filling 'Reply-to' with the value of ``send_from``.

        A common use case is to have fixed ``send_from`` and ``send_to`` but send multiple emails.
        In this case, do something like the following::

            from functools import partial

            mailer = Mailer(**server_args)
            my_mailer = partial(mailer.send, send_from='abc@gmail.com', send_to='def@myjob.com')

            my_mailer(subject='abc', text='test')
            my_mailer(subject='ERROR', text='see what you did!')

            # And pass the object 'my_mailer' around to send emails from within other code blocks.
        """
        msg = _generate_msg(send_from=send_from, send_to=send_to, subject=subject, text=text, files=files)
        server = self._connect()
        thr = threading.Thread(
            target=_send_mail,
            kwargs={'server': server, 'msg': msg},
            )
        thr.start()

        # TODO: take care of `server.quit()`.

