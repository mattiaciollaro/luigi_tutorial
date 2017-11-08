#
# Some utilities
#
import __main__
import os
import sys
from configparser import ConfigParser

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

def get_config(section, option):
    """
    Returns the value of an option stored in a section
    of a configuration file

    Args:
        section: a string corresponding to the name of a section of a
            configuration file
        option: a string corresponding to an option contained in a section
            of a configuration file

    Returns:
        a string, corresponding to the value of the target option in the
        target section
    """

    parser = ConfigParser()

    cf = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'local.conf'
    )

    parser.read(cf)

    return parser.get(section, option)