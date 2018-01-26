from ..teleshell import TeleShell
from utilities.config import get_config_file, read_ini_config

kwargs = read_ini_config('teleshell.cfg')['server']
kwargs['known_hosts_file'] = get_config_file(kwargs['known_hosts_file'])
kwargs['key_file'] = get_config_file(kwargs['key_file'])
teleShell = TeleShell(**kwargs)


def test_command():
    print("test_command()")
    print("Test with an executable command: ")
    cmd1 = "ls -l"
    outputs, errors = teleShell.command(cmd1)
    print("Printing outputs: ")
    print(outputs)
    print("Printing errors: ")
    print(errors)

    print("Test with an inexecutable command: ")
    cmd2 = "abc"
    outputs, errors = teleShell.command(cmd2)
    print("Printing outputs: ")
    print(outputs)
    print("Printing errors: ")
    print(errors)


def test_commands():
    print("test_commands()")
    cmds = ["date", "dir"]
    outputs, errors = teleShell.commands(cmds)
    print("Printing outputs: ")
    print(outputs)
    print("Printing errors: ")
    print(errors)
