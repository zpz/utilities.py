"""
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


def read_ini_config(file_name: str,
                    section_name: str = None,
                    allow_no_value: bool = True) -> dict:
    """
    This is a convenience function for getting the content of an INI-style config file
    (for reading only).

    The returned object works much like a ``dict`` thanks to the
    *mapping protocol access* capabilities in Python 3. For example,
    assuming the config file is '/myproject/test.cfg'::

        conf = read_ini_config('/myproject/test.cfg')
        print(conf['server']['host'])   # print '1.2.3.4'

        server = conf['server']
        print(server['port'])    # print '28'

        # Now can pass 'server' on and use it largely as if it's a `dict`.
    """
    conf = configparser.ConfigParser(allow_no_value=allow_no_value)
    conf.read(file_name)
    if section_name:
        return conf[section_name]
    return conf
