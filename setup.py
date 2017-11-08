#
# Setup file for the tutorial
#

#
# Required modules
#
import __main__
import os
import sys
import json

from textwrap import TextWrapper

#
# Some utilities
#
def write_section(section_name):
    """
    Writes section names for configuration files in the format
    [section_name]

    Args:
        section_name: a string representing the section name

    Returns:
        the string [section_name]\n
    """

    return '[{}]\n'.format(section_name)

def write_option(option_name, option_value, last=False):
    """
    Writes option and value pairs for configuration files in the
    format option=value

    Args:
        option_name: a string representing the option name
        option_value: a string representing the option value
        last: a boolean; if True, a new line is added at the end

    Returns:
        the string option_name=option_value if last is False or
        the string option_name=option_value\n if last is True
    """

    out = '{}={}\n'.format(option_name, option_value)

    if last:
        out += '\n'

    return out

def get_script_name():
    """
    Gets the name of the current script being executed

    This function returns the name of the current script being executed
    if the Python session is run in non-interactive mode.

    Returns:
        a string with the name of the script, if the script executed in
        non-interactive mode; otherwise, the function returns None
    """
    
    # check whether Python is in interactive mode
    interactive = not hasattr(__main__, '__file__')

    if interactive:
        return None
    else:
        script_name = os.path.basename(sys.argv[0])
        return script_name

max_chars_per_line = 100

# utility for pretty printing
text_wrapper = TextWrapper(replace_whitespace=True,
                           drop_whitespace=False,
                           width=max_chars_per_line)
def pretty_print(text, new_lines=0):
    print(text_wrapper.fill(text) + '\n' * new_lines)

#
# Setup
#
script_name = get_script_name()
pretty_print('=== This is {} ===\n'.format(script_name), 1)

# get this path
this_path = os.getcwd()

#
# Create luigi configuration file
#
pretty_print('Writing luigi configuration file...')

# create subdirectory for luigi stuff
if not os.path.exists(os.path.join(this_path, 'luigi')):
    os.system('mkdir luigi')

# path for luigi logging configuration file
luigi_logging_conf_path = os.path.join(this_path,
                                       'luigi',
                                       'luigi_logging.conf')

# luigi configuration file
luigi_conf_path = os.path.join(this_path,
                               'luigi',
                               'luigi.conf')
with open(luigi_conf_path, 'w') as luigi_conf_file:
    # [core]
    luigi_conf_file.write(write_section('core'))
    luigi_conf_file.write(write_option('logging_conf_file',
                                       luigi_logging_conf_path))

pretty_print('Done.')
pretty_print('luigi configuration file saved to {}'.format(luigi_conf_path), 1)

#
# Create luigi logging configuration file
#
pretty_print('Writing luigi logging configuration file...')

with open(luigi_logging_conf_path, 'w') as luigi_logging_conf_file:
    # [loggers]
    luigi_logging_conf_file.write(write_section('loggers'))
    luigi_logging_conf_file.write(write_option('keys',
                                               'root,luigi-interface',
                                               last=True))

    # [handlers]
    luigi_logging_conf_file.write(write_section('handlers'))
    luigi_logging_conf_file.write(
        write_option('keys', 'consoleHandler,fileHandler', last=True)
    )

    # [formatters]
    luigi_logging_conf_file.write(write_section('formatters'))
    luigi_logging_conf_file.write(
        write_option('keys', 'consoleFormatter,fileFormatter', last=True)
    )

    # [logger_root]
    luigi_logging_conf_file.write(write_section('logger_root'))
    luigi_logging_conf_file.write(write_option('level', 'INFO'))
    luigi_logging_conf_file.write(
        write_option('handlers', 'consoleHandler,fileHandler')
    )
    luigi_logging_conf_file.write(write_option('logThreads', '0'))
    luigi_logging_conf_file.write(
        write_option('raiseExceptions', '1', last=True)
    )

    # [logger_luigi-interface]
    luigi_logging_conf_file.write(write_section('logger_luigi-interface'))
    luigi_logging_conf_file.write(write_option('level', 'DEBUG'))
    luigi_logging_conf_file.write(
        write_option('handlers', 'consoleHandler,fileHandler')
    )
    luigi_logging_conf_file.write(write_option('qualname', 'luigi-interface'))
    luigi_logging_conf_file.write(write_option('propagate', '0'))
    luigi_logging_conf_file.write(write_option('logThreads', '0', last=True))

    # [handler_consoleHandler]
    luigi_logging_conf_file.write(write_section('handler_consoleHandler'))
    luigi_logging_conf_file.write(write_option('class', 'StreamHandler'))
    luigi_logging_conf_file.write(write_option('level', 'INFO'))
    luigi_logging_conf_file.write(write_option('formatter', 'consoleFormatter'))
    luigi_logging_conf_file.write(
        write_option('args', '(sys.stdout,)', last=True)
    )

    # [handler_fileHandler]
    luigi_logging_conf_file.write(write_section('handler_fileHandler'))
    luigi_logging_conf_file.write(write_option('class', 'FileHandler'))
    luigi_logging_conf_file.write(write_option('level', 'DEBUG'))
    luigi_logging_conf_file.write(write_option('formatter', 'fileFormatter'))
    luigi_logging_conf_file.write(
        write_option('args', "('luigi.log', 'a', 'utf-8')", last=True)
    )

    # [formatter_consoleFormatter]
    luigi_logging_conf_file.write(write_section('formatter_consoleFormatter'))
    luigi_logging_conf_file.write(
        write_option('format', '%(levelname)s - %(message)s')
    )
    luigi_logging_conf_file.write(write_option('datefmt', '', last=True))

    # [formatter_fileFormatter]
    luigi_logging_conf_file.write(write_section('formatter_fileFormatter'))
    luigi_logging_conf_file.write(
        write_option('format',
                     '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    luigi_logging_conf_file.write(write_option('datefmt', '', last=True))

pretty_print('Done.')
pretty_print('luigi logging configuration file saved to {}\n'.format(
    luigi_logging_conf_path), 1
)

pretty_print('Done.', 1)

pretty_print('=== All done with {} ==='.format(script_name), 1)

#
# Ask user to configure environment variables
#
pretty_print('#' * max_chars_per_line, 1)
pretty_print('Please copy and paste the following to your ~/.bash_profile:', 1)
pretty_print('export PYTHONPATH=$PYTHONPATH:{}'.format(this_path))
pretty_print('export LUIGI_CONFIG_PATH={}'.format(luigi_conf_path))
pretty_print('Then, save the updated ~/.bash_profile file and source it by '
             'executing:', 1)
pretty_print('source ~/.bash_profile', 1)
pretty_print('#' * max_chars_per_line)
