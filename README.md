* [Recommended directory structure on local machine](#directory-structure-on-local-machine)
* [Configuration management and environment settings](#config-and-environ)
* [Use `utilities.config` to access config files](#utilities-config)
* [Recommend directory structure for a Python project](#directory-structure-for-python-project)
* [Documentation](#documentation)
* [Logging](#logging)
* [Testing](#testing)
* [A few more recommendations](#recommendations)

<a name="directory-structure-on-local-machine"></a>
## Recommended directory structure on local machine

Suppose your home directory is `/Users/username` (on Mac) or `/home/username` (on Ubuntu Linux), represented by the environment variable `HOME`, we recommend the following direcotry structure for work:

```
$HOME/work/
  |-- bin/
  |-- config-dev/
  |-- config-prod/
  |-- log/
  |-- src/
  |     |-- project_1/
  |     |-- project_2/
  |     |-- python-common/
  |     |-- and so on
  |-- tmp/
```

The directories in `$HOME/work/src/` are `git` repos and are source-controlled. Other thigns are not in source control and not stored in the cloud.

Space and non-ascii characters are better avoided in directory and file names, esp under `config*`, `log`, and `src`.


<a name="config-and-environ"></a>
## Configuration management and environment settings

Each source repo is expected to have a top-level directory `config` (or `conf`). The config files are "templates" in that they should not contain sensitive info like passwords; such fields should show a template like this:

```
host = ---fill this out---
password = ---fille this out---
```

Otherwise the config files should be maintained and contain all required sections and fields. If a field should take different values in different situations, mark it like so:

```
#timeout = 300
#  uncomment the above line in a production environment
#timeout = 30
#  uncomment the above line in a testing environment
```

**Please do not confuse _configuration_ with _constants_**. While *constants* should be defined within a package and maintained in version control (and used freely within the package), *configuration* should stay outside of packages, and mostly outside of version control, except for "templates" (or examples) of the configuration files. (c.f. [python directory structure](#directory-structure-for-python-project).)

Config files of a certain repo should be accessed only by code from that repo; if any configuration info is useful to other repos, the hosting repo should provide API instead of exposing raw config files. This way, code in one repo has full control and freedom with the file-names, sections, options, and everything about the config files of that repo.


<a name="utilities-config"></a>
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

 
<a name="directory-structure-for-python-project"></a>
## Recommended directory structure for a Python project

```
project/
  |-- config/
  |-- doc/
  |-- package-a/
  |     |-- __init__.py
  |     |-- test/
  |     |     |-- test_imports.py
  |     |     |-- test_other.py
  |-- package-b/
  |-- scripts/
  |-- README.md
```

Here 'project' is synonym to a git repo.

- Maintain a clean project file structure, and do not be afraid to adjust or refactor. [Some reference](http://as.ynchrono.us/2007/12/filesystem-structure-of-python-project_21.html); [some more](http://stackoverflow.com/questions/193161/what-is-the-best-project-structure-for-a-python-application).
- A typical situation is that a Python project contains a single Python package, with the same name as the project. However, it's fine to have multiple pakcages in one project.
- Every package (that is, a directory with an `__init__.py`) should contain a sub-directory `test` or `tests`.
- Directories `test`, `tests`, `scripts` should not contain `__init__.py` (that is, do not make these directories *packages*, unless you have really good reasons), but can contain files named like `*_test.py` or `test_*.py`.
- Ideally, content of `scripts` is mainly short launchers of functions in packages.


<a name="documentation"></a>
## Documentation

Do write documentation to help **yourself**, if not others. 

- Doc in source files: see [examples](http://sphinxcontrib-napoleon.readthedocs.org/en/latest/example_google.html).
- `README.md` in the top-level (project root) or second-level (package root) 
    directories: use Markdown format.
  - If you use Mac, a decent Markdown editor is `MacDown`.
- Doc files in `doc` directory under the project root directory: Markdown and plain text formats are preferred.
- Outside of source tree: for documentation aimed at non-developers or users from other groups that require different levels of access control


<a name="logging"></a>
## Logging

Prefer logging to `print` in most places.

- The [twelve factor app](http://12factor.net/logs) advocates treating log events as an event stream, always sending the stream to standard output, and leaving capture of the stream into files to the execution environment.
- `$HOME/work/log` is a reasonable place for log files. However, as the proceeding item suggests, capture of logging into files is better left to the execution environment.
- In all modules that need to do logging, always have this at the top of the module:

	```
   import logging
   logger = logging.getLogger(__name__)
	```
	
	then use `logger.info()` etc to create log messages.

	Do not create custom names for the logger. Always do it like above.

	The `__name__` mechanism will create a hierarchy of loggers following your
package structure, e.g. with loggers named

	```
   package1
   package1.module1
   package1.module1.submodule2
	```
	
	Log messages will pass to higher levels in this hierarchy.
Customizing the logger names will disrupt this useful message forwarding.

- In the `__init__.py` file in the top-level directory of your Python package,
include this:

	```
   # Set default logging handler to avoid "No handler found" warnings.
   import logging
   logging.getLogger(__name__).addHandler(logging.NullHandler())
	```
	
	Do not add any other handler in your package code.
- Do not do any format, handler (e.g. using a file handler), log level,
or other configuration in your package code. These are the package *user*'s concern.

- Use the old-style string formatting (`%`), not the new-style string formatting (`str.format`).
  The following example should serve most of your fomatting needs:
  
  ```python
  logger.info('Line %s (%s) has spent %.2f by hour %d', 'asd9123las', 'Huge Sale!', 28.97, 23) 
  ```
  
  [See here](https://pyformat.info) for more about formatting.
  

<a name="testing"></a>
## Testing


Write tests, and use `py.test` to do so.

`import utilities.testing` in your test script.


<a name="recommendations"></a>
## A few more recommendations

- Use 'snake-case' names; see [examples](https://google.github.io/styleguide/pyguide.html?showone=Naming#Naming).

- Use `PyCharm`.


