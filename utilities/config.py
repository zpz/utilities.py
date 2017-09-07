"""
The main point of this module is that the location of config files
is a matter of system design, while child projects should not hard-code
fixed absolute paths for their config files.

Current design is that environment variable ``${CFGDIR}`` points to
the directory where config files reside. Below this directory there
can be sub-directories. Individual projects know the path (including file name)
of their config files **below** ``${CFGDIR}``.

A commonly used config file format is ``INI``. Here is an example file::

    [server]
    host = 1.2.3.4
    port = 28

    # This block concerns our secrete sauce
    speed =
    # fill in an integer greater than 1000000 above
    use-power
    ; leave this alone

    nocomment = this; that
    semicolon = ;

    [client settings]
    browser = chrome only

A few points about the format:

1. Feel free to use blank lines for content organization, even within a single 'section'.
2. A comment line starts with either `#` or `;`, *with no leading space*.
3. 'speed' have value '' (empty string).
4. 'use-power' has value of Python's ``None``.
5. Section name can contain space.
6. 'true', 'True', 'false', 'False', 'yes', 'Yes', 'no', 'No' are recognized in expected ways.

In light of the peculiarities of the syntax for comments, we recommend the following regarding comments:

- Do not use in-line comments.
- Use whole-line comments starting with '#', with no leading space.
- Use blank lines to help organization.

**Caution**: because ``configparser.ConfigParser`` ignores comments,
if you read in a config file using ``configparser.ConfigParser``, make changes
to the object, then save the changed object its ``write`` method,
comments in the original file will be lost.

Do not quote the values---quotation marks will be literal parts of the value.

For complex values, use JSON strings. Note, quotation marks in JSON strings should be *double quotes*.
"""

import configparser
import os


class ConfigFileNotFoundError(Exception):
    pass


def get_config_file(topic):
    """
    Obtain the full path of a config file given its short name.

    Args:
        topic (str): short name of the config file.

            The environment variable ``CFGDIR`` is guaranteed to exist and point
            to the directory where config files reside.
            ``topic`` is the config file's name along with its path under ``${CFGDIR}``,
            that is, ``topic`` is formatted like ``subdirectory/filename``, hence
            the full path of the config file is ``${CFGDIR}/topic``.

            If the named file is not found, ``ConfigFileNotFoundError`` is raised.

            Examples::

                python-common/mysql.cfg
                cross-device/d2d_logistic_model_a.cfg
                data-pipelines/luigi.cfg
    """
    topic = os.path.join(os.environ['CFGDIR'], topic)
    if os.path.isfile(topic):
        return topic
    raise ConfigFileNotFoundError('Configuration "' + topic + '" can not be found')


def read_ini_config(topic, allow_no_value=True):
    """
    This is a convenience function for getting the content
    of an INI-style config file (for read-only).

    The returned object works much like a ``dict`` thanks to the
    *mapping protocol access* capabilities in Python 3. For example,
    assuming the config file above is ``${CFGDIR}/myproject/test.cfg``::

        conf = read_ini_config('myproject/test.cfg')
        print(conf['server']['host'])   # print '1.2.3.4'

        server = conf['server']
        print(server['port'])    # print '28'

        # Now can pass 'server' on and use it largely as if it's a `dict`.
    """
    conf = configparser.ConfigParser(allow_no_value=allow_no_value)
    conf.read(get_config_file(topic))
    return conf

