import sys
import argparse
import logging

from squelch import Squelch, __version__, PROGNAME, DEF_CONF_FILE

STATE_OPTS = ['set','pset']
NON_CONF_OPTS = STATE_OPTS

logging.basicConfig()
logger = logging.getLogger(PROGNAME)

def parse_cmdln():
    """
    Parse the command line

    :returns: An object containing the command line arguments and options
    :rtype: argparse.Namespace
    """

    epilog = """Database Connection URL

The database connection URL can either be passed on the command line, via the --url option, or specified in a JSON configuration file given by the --conf-file option.  The form of the JSON configuration file is as follows:

{
  "url": "<URL>"
}

From the SQLAlchemy documentation:

"The string form of the URL is dialect[+driver]://user:password@host/dbname[?key=value..], where dialect is a database name such as mysql, oracle, postgresql, etc., and driver the name of a DBAPI, such as psycopg2, pyodbc, cx_oracle, etc. Alternatively, the URL can be an instance of URL."
"""

    parser = argparse.ArgumentParser(description='Squelch is a Simple SQL REPL Command Handler.', epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter, prog=PROGNAME)
    parser.add_argument('-c', '--conf-file', help=f"The full path to a JSON configuration file.  It defaults to {DEF_CONF_FILE}.")
    parser.add_argument('-u', '--url', help='The database connection URL, as required by sqlalchemy.create_engine().')
    parser.add_argument('-S', '--set', help='Set state variable NAME to VALUE.', metavar='NAME=VALUE', nargs='*', action='extend')
    parser.add_argument('-P', '--pset', help='Set printing state variable NAME to VALUE.', metavar='NAME=VALUE', nargs='*', action='extend')
    parser.add_argument('-v', '--verbose', help='Turn verbose messaging on.  The effects of this option are incremental.', action='count', default=0)
    parser.add_argument('-V', '--version', action='version', version=f"%(prog)s {__version__}")

    args = parser.parse_args()

    return args

def configure_logging(args):
    """
    Configure logging based on the command line arguments

    :param args: The command line arguments
    :type args: argparse.Namespace
    """

    # Enable info messages in this library
    if args.verbose:
        logger.setLevel(logging.INFO)
        logging.getLogger(__package__).setLevel(logging.INFO)

        # Enable debug messages in this library
        if args.verbose > 1:
            logger.setLevel(logging.DEBUG)
            logging.getLogger(__package__).setLevel(logging.DEBUG)

        # Enable debug messages in this library and dependent libraries
        if args.verbose > 2:
            logging.getLogger().setLevel(logging.DEBUG)

def update_conf_from_cmdln(conf, args):
    """
    Update the configuration from command line arguments

    Options listed in NON_CONF_OPTS are not included in the configuration

    :param conf: The program's configuration
    :type conf: dict
    :param args: The parsed command line arguments object
    :type args: argparse.Namespace object
    :returns: The updated configuration
    :rtype: dict
    """

    opts = {}

    for k,v in vars(args).items():
        if k not in NON_CONF_OPTS:
            if v:
                opts[k] = v

    logger.debug(f"overriding configuration with options: {opts}")
    conf.update(opts)

    return conf

def set_state_from_cmdln(squelch, args, nv_sep='='):
    """
    Update the program's runtime state from command line arguments

    Options listed in STATE_OPTS are used to set the program's runtime state

    :param squelch: The instantiated Squelch object
    :type squelch: Squelch
    :param args: The parsed command line arguments object
    :type args: argparse.Namespace object
    :param nv_sep: The name/value separator in the option argument
    :type nv_sep: str
    """

    for k,v in vars(args).items():
        if k in STATE_OPTS:
            if v:
                # Multiple state options can be set hence this is a list
                for nv_pair in v:
                    try:
                        name, value = nv_pair.split(nv_sep, maxsplit=2)
                    except ValueError as e:
                        print(f"A state variable must be expressed as NAME=VALUE.  For example, --set AUTOCOMMIT=on, --pset pager=off.", file=sys.stderr)

                        if args.verbose > 1:
                            raise
                        else:
                            sys.exit(1)

                    # Construct command in form it would be issued in client
                    logger.debug(f"setting {name} to {value}")
                    cmd = fr"\{k} {name} {value}"
                    state_text = squelch.set_state(cmd)

                    if state_text:
                        logger.debug(state_text)

def consolidate_conf(squelch, args):
    """
    Consolidate the configuration from a conf file and command line arguments

    :param squelch: The instantiated Squelch object
    :type squelch: Squelch
    :param args: The parsed command line arguments object
    :type args: argparse.Namespace object
    :returns: The consolidated configuration
    :rtype: dict
    """

    if args.conf_file:
        squelch.get_conf(file=args.conf_file)

    squelch.conf = update_conf_from_cmdln(squelch.conf, args)

    # The verbosity level may have been set in the conf file so we reconfigure
    # the logging
    try:
        args.verbose = squelch.conf['verbose']
    except KeyError:
        pass

    configure_logging(args)

    set_state_from_cmdln(squelch, args)

    return squelch.conf

def connect(squelch, args):
    """
    Connect to the database

    The program exits if no valid database connection URL was specified

    :param squelch: The instantiated Squelch object
    :type squelch: Squelch
    :type args: argparse.Namespace object
    :returns: The updated configuration
    """

    try:
        url = squelch.conf['url']
    except KeyError:
        print(f"A database connection URL is required.  See the --help option for details.", file=sys.stderr)

        if args.verbose > 1:
            raise
        else:
            sys.exit(1)

    squelch.connect(url)

def main():
    """
    Main function
    """

    args = parse_cmdln()
    squelch = Squelch()
    configure_logging(args)
    consolidate_conf(squelch, args)

    connect(squelch, args)

    # Process queries on stdin if we were called as a one-shot, otherwise
    # we drop into the interactive REPL
    if not sys.stdin.isatty():
        for line in sys.stdin:
            squelch.process_input(squelch.clean_raw_input(line))
    else:
        squelch.repl()

if __name__ == '__main__':
    main()

