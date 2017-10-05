
## Use `utilities.config` to access config files

A example config file:

```
[server]
host = 1.2.3.4
port = 28    ; an integer

# This block concerns our secrete sauce
speed =
# fill in an integer greater than 1000000 above
use-power
; leave this alone

nocomment = this; that
semicolon = ;

[client settings]
browser = chrome only
```

A few points about the format:

1. Feel free to use blank lines for content organization, even within a single 'section'.
2. In-line comment can be introduced with a ` ;` (that is, space followed by semicolon) if the line has complete `key = value` content; otherwise semicolon is a literal part of the value. For example, `port` has value '28', `nocomment` has value 'this; that', and `semicolon` has value ';'.
3. A comment line starts with either `#` or `;`, *with no leading space*.
4. 'speed' have value '' (empty string).
5. 'use-power' has value of Python's `None`.
6. Section name can contain space; option name can not.
7. 'true', 'True', 'false', 'False', 'yes', 'Yes', 'no', 'No' are recognized by `Configger.getboolean` in expected ways.

In light of the peculiarities of comment syntax, we recommend the following regarding comments:

- Do not use in-line comments.
- Use whole-line comments starting with '#', with no leading space.
- Use blank lines to help organization.

**Caution**: if you use `Configger` to make changes to a config file (using its methods `set`, `remove_option`, `save`), all comments existing in the original file will be gone.

Do not quote the values---quotation marks will be literal parts of the value.

For complex values, use JSON strings. Note, quotation marks in JSON strings should be *double quotes*.

