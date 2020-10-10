"""
Utility functions for running commands and shell scripts on a server without interactively logged in.
"""

from paramiko.client import SSHClient


class TeleShell(object):
    """This class is for running commands on servers."""

    def __init__(self, *, host, user, known_hosts_file, key_file):
        ssh = SSHClient()
        ssh.load_host_keys(filename=known_hosts_file)
        ssh.connect(hostname=host, username=user, key_filename=key_file)
        self._ssh = ssh

    def command(self, cmd):
        """
        Function to run single command.

        Args:
            cmd: Single command
        Returns:
            list: command result
            string: error message if command fails, None is succeeds
        """
        sshClient = self._ssh
        stdin, stdout, stderr = sshClient.exec_command(cmd)
        outputs = [each for each in stdout.readlines()]
        error = stderr.readlines()
        if len(error) != 0:
            error = error[0]
        else:
            error = None
        return outputs, error

    def commands(self, cmds):
        """
        Function to run multiple commands.

        Args:
            cmds: commands list
        Returns:
            list: commands result list
            string: error message is command fails, None is succeeds
        """
        cmd = '\n'.join(c for c in cmds)
        return self.command("bash -c '{str}'".format(str=cmd))

    def script(self, filename):
        """
        Function to run shell script file.

        Args:
            filename: name of script file
        Returns:
            list: commands result
            string: error message is command fails, None is succeeds
        """
        with open(filename, 'r') as f:
            comm = f.readlines()
        comms = ' '.join(each for each in comm)
        outputs, errors = self.command("bash -c '{str}'".format(str=comms))
        return outputs, errors

    def __call__(self, cmd):
        outputs, error = self.command(cmd)
        for line in outputs:
            print(line, end='')
        if error is not None:
            print(error)
