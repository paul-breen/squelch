__version__ = '0.1.0'

import sys
import logging
from pathlib import Path
import atexit
import traceback
import re
import json
import readline
import pydoc

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import DatabaseError
from tabulate import tabulate

PROGNAME = __name__

DEF_CONF_FILE = './squelch.json'
DEF_HISTORY_FILE = Path('~/.squelch_history').expanduser()
DEF_CONF = {}
DEF_STATE = {'pager': True, 'footer': True}

URL_CRED_PATTERN = r'://(.+)@'
URL_CRED_REPLACE = r'://***@'

SQL_COMPLETIONS = ['select', 'insert', 'update', 'delete', 'create', 'drop', 'from', 'where', 'and', 'or', 'not', 'like', 'order by', 'group by', 'into', 'values']

logger = logging.getLogger(__name__)

class Squelch(object):
    """
    Class providing a Simple SQL REPL Command Handler
    """

    DEFAULTS = {
        'conf_file': DEF_CONF_FILE,
        'history_file': DEF_HISTORY_FILE,
        'query_quoted_string_pattern': r"'[^']+'",
        'query_params_pattern': r':([a-z0-9_.]+)',
        'repl_commands': {
            'quit': [r'\q'],
            'state': [r'\set', r'\pset'],
            'help': [r'help', r'\?'],
            'dist': [r'\copyright']
        },
        'table_opts': {
            # Unfortunately, tabulate doesn't recognise a sqlalchemy result
            # as having keys(), so we can't set 'headers': 'keys' here
            'tablefmt': 'presto', 'showindex': False, 'disable_numparse': True
        }
    }
 
    def __init__(self, conf=DEF_CONF, state=DEF_STATE):
        """
        Constructor

        :param conf: Optional configuration
        :type conf: dict
        :param state: Optional REPL state
        :type state: dict
        """

        self.conf = conf
        self.state = state
        self.conn = None
        self.query = None
        self.params = {}
        self.result = None

    def get_conf(self, file=DEF_CONF_FILE):
        """
        Get the program's configuration from a JSON file

        The configuration is stored in self.conf.  As a minimum, the
        configuration must contain the database connection URL.

        The form of the minimum JSON configuration file is as follows:

        {
          "url": "<URL>"
        }

        :param file: The program's configuration file
        :type file: str
        :returns: The program's configuration
        :rtype: dict
        """

        self.conf = {}
        path = Path(file)

        if path.is_file():
            logger.info(f"reading configuration from file {file}")

            with path.open() as fp:
                self.conf = json.load(fp)

                # Make a copy of config so we can redact any credentials
                # in verbose logging
                if logger.isEnabledFor(logging.DEBUG):
                    tmp = self.conf.copy()

                    try:
                        tmp['url'] = re.sub(URL_CRED_PATTERN, URL_CRED_REPLACE, tmp['url'])
                    except KeyError:
                        pass

                    logger.debug(f"configuration read from file: {tmp}")

        return self.conf

    def get_conf_item(self, key):
        """
        Get the configuration item value for the given key

        The value for the key is returned from self.conf.  It uses the value
        from self.DEFAULTS as the fallback default

        :param key: The key of the configuration item
        :type key: str
        :returns: The configuration item's value
        :rtype: any
        :raises: KeyError
        """

        # If self.conf contains key but self.DEFAULTS does not, then using
        # self.DEFAULTS[key] as our default will raise KeyError, even though
        # there's a value available in self.conf.  In other words, we can't
        # do: self.conf.get(key, self.DEFAULTS[key])
        return self.conf[key] if key in self.conf else self.DEFAULTS[key]

    def set_state(self, cmd):
        """
        Set the progam's runtime state according to the given command

        The state variable in self.state is updated according to command

        :param cmd: The command to update the program's state
        :type cmd: str
        """

        if cmd.lower().startswith(r'\pset pager'):
            if cmd.lower() == r'\pset pager off':
                self.state['pager'] = False
            elif cmd.lower() == r'\pset pager on':
                self.state['pager'] = True
        elif cmd.lower().startswith(r'\pset footer'):
            if cmd.lower() == r'\pset footer off':
                self.state['footer'] = False
            elif cmd.lower() == r'\pset footer on':
                self.state['footer'] = True

    def get_welcome_text(self):
        """
        Get the welcome text

        * Shows program information
        * Signposts the user how to get help

        :returns: The welcome text
        :rtype: str
        """

        text = fr"""{PROGNAME} ({__version__})
Type "help" for help.
"""

        return text

    def get_help_summary_text(self):
        """
        Get the help summary text

        Tells the user how to get:

        * Distribution terms
        * Help with the REPL commands
        * How to quit

        :returns: The help summary text
        :rtype: str
        """

        text = fr"""You are using {PROGNAME}, a CLI to SQLAlchemy-supported database engines.
Type:  \copyright for distribution terms
       \? for help with {PROGNAME} commands
       \q to quit"""

        return text

    def get_help_repl_cmd_text(self):
        """
        Get the REPL command help text

        :returns: The REPL command help text
        :rtype: str
        """

        text = fr"""General
  \copyright             show {PROGNAME} usage and distribution terms
  \q                     quit {PROGNAME}

Help
  \?                     show help on backslash commands

Formatting
  \pset [NAME [VALUE]]   set table output option
                         (pager)
"""

        return text

    def get_help(self, cmd):
        r"""
        Get the progam's help text according to the given command

        * help: Get the help summary text
        * \?: Get the REPL command text

        :param cmd: The help command
        :type cmd: str
        :returns: The help text corresponding to the given command
        :rtype: str
        """

        text = ''

        if cmd.lower() == r'help':
            text = self.get_help_summary_text()
        elif cmd.lower() == r'\?':
            text = self.get_help_repl_cmd_text()

        return text

    def get_dist_terms_text(self):
        """
        Get the program's distribution terms text

        :returns: The program's distribution terms text
        :rtype: str
        """

        text = f"""{PROGNAME} ({__version__}) distributed under Apache-2.0 license: https://spdx.org/licenses/Apache-2.0.html"""

        return text

    def connect(self, url):
        """
        Connect to the database in the given connection URL

        :param url: The database connection URL
        :type url: str
        :returns: The database connection object
        :rtype: sqlalchemy.engine.Connection
        """

        engine = create_engine(url)
        self.conn = engine.connect()
        logger.info(f"connected to database {self.conn.engine.url.database}")

        return self.conn

    def exec_query(self, query, params):
        """
        Execute the given query and bind any given query parameters

        The query is executed in a database transaction and the query
        result set is stored in self.result

        :param query: The query to execute
        :type query: sqlalchemy.sql.text
        :param params: Optional query parameters to bind in the query
        :type params: dict
        :returns: The query result set
        :rtype: sqlalchemy.engine.CursorResult
        """

        self.result = None

        with self.conn.begin() as trans:
            try:
                self.result = self.conn.execute(query, params)
            except DatabaseError as e:
                if logger.isEnabledFor(logging.DEBUG):
                    traceback.print_exc(chain=False)
                else:
                    print(e, file=sys.stderr)

        return self.result

    def get_result_table_footer(self, table, table_opts):
        """
        Get the result table footer

        The footer shows the row count of the table data

        * Access to the row count is dependent on the DB engine supporting
          this.  It should be supported for UPDATE and DELETE, but may not
          be supported for INSERT or SELECT.
        * If row count isn't supported by the DB engine, then a fallback is
          to employ a heuristic to calculate the number of rows based on the
          number of newlines found in the table.  Whilst this works for the
          default `tablefmt` table option, it won't work for `tablefmt`
          styles that have more or less table grid lines.

        :param table: The result table text
        :type table: str
        :param table_opts: Options for rendering the tabulated result output
        :type table_opts: dict
        :returns: The result table footer text
        :rtype: str
        """

        footer = ''

        if self.result.supports_sane_rowcount and self.result.rowcount != -1:
            logger.debug(f"row count available in the result cursor")
            nrows = self.result.rowcount
        else:
            try:
                if table_opts['tablefmt'] == self.DEFAULTS['table_opts']['tablefmt']:
                    logger.debug(f"row count calculated from lines in the table")
                    nrows = table.count('\n') - 1
            except KeyError:
                nrows = -1

        if nrows != -1:
            row_text = 'row' if nrows == 1 else 'rows'
            footer = f'\n({nrows} {row_text})\n'
        else:
            logger.debug(f"row count not available")

        return footer

    def present_result(self, table_opts=None):
        """
        Present the result set of the latest executed query

        The query result set in self.result is presented as a table

        * If the state variable 'pager' is True, the output is paged using the
          system pager, otherwise the whole table is printed to the output
          stream

        :param table_opts: Options for rendering the tabulated result output
        :type table_opts: dict
        """

        table_opts = table_opts or self.get_conf_item('table_opts')
        logger.debug(f"table_opts: {table_opts}")

        if self.result and self.result.returns_rows:
            table = tabulate(self.result, headers=self.result.keys(), **table_opts)

            if table:
                if self.state['footer']:
                    table += self.get_result_table_footer(table, table_opts)
                else:
                    # No footer, add blank line to separate table from prompt
                    table += '\n'

                if self.state['pager']:
                    pydoc.pager(table)
                else:
                    print(table)

    def prompt_for_query_params(self, raw):
        """
        Prompt for any query parameters

        If the raw query contains query parameter placeholders, then the user
        is prompted to provide a value for each parameter.  These parameters
        are stored in self.params

        :param raw: The raw query that may contain query parameters
        :type raw: str
        :returns: Any query parameters
        :rtype: dict
        """

        self.params = {}
        clean = re.sub(self.get_conf_item('query_quoted_string_pattern'), '', raw)
        keys = re.findall(self.get_conf_item('query_params_pattern'), clean)
        logger.debug(f"parsed query parameter keys: {keys}")

        for key in keys:
            self.params[key] = input(f"{key}: ")

        return self.params

    def prompt_for_input(self, prompt='=> ', terminator=';'):
        """
        Prompt for input

        The user is prompted to input a query, or a command for the REPL, such
        as the quit command

        :param prompt: The text for the input prompt
        :type prompt: str
        :param terminator: A query terminator to be stripped from the query
        :type terminator: str
        :returns: The raw stripped query
        :rtype: str
        """

        raw = input(prompt)
        raw = raw.strip().rstrip(terminator)
        logger.debug(f"raw stripped query: '{raw}'")

        return raw

    def process_input(self, raw):
        """
        Process the given input

        * If the input is a REPL command, then the command is executed
        * If the input is a database query, then the raw query is converted
          to an sqlalchemy.sql.text prepared query and stored in self.query,
          any query parameters are prompted for and stored in self.params,
          and the query is then executed and the results presented
        * If the input is empty, the user is asked if they wish to quit

        :param raw: The raw stripped input (a query or REPL command)
        :type raw: str
        """

        self.query = None
        self.params = {}

        if raw:
            cmd = raw.split()[0]

            if cmd in self.get_conf_item('repl_commands')['quit']:
                logger.info('quitting')
                sys.exit(0)
            elif cmd in self.get_conf_item('repl_commands')['state']:
                self.set_state(raw)
            elif cmd in self.get_conf_item('repl_commands')['help']:
                print(self.get_help(raw))
            elif cmd in self.get_conf_item('repl_commands')['dist']:
                print(self.get_dist_terms_text())
            else:
                self.query = text(raw)
                self.params = self.prompt_for_query_params(raw)
                self.exec_query(self.query, self.params)
                self.present_result()
        else:
            q = input('no input, do you want to quit (yes/no)? ')

            if q.lower().startswith('y'):
                logger.info('no input, so quitting')
                sys.exit(0)

    def input_completions(self, text, state):
        """
        Readline completion callback function for the REPL

        The list of possible completions are held in SQL_COMPLETIONS

        :param text: The partial input text to be completed
        :type text: str
        :param state: The input completion state
        :type state: int
        :returns: A possible matching completion
        :rtype: str or None
        """

        if not text:
            matches = SQL_COMPLETIONS
        else:
            matches = [i for i in SQL_COMPLETIONS if i.startswith(text.lower())] + [None]

        return matches[state]

    def init_repl(self):
        """
        Initialise the REPL

        Any history from the configured history_file is read in.  The REPL
        input completions are initialised
        """

        history_file = self.get_conf_item('history_file')
        path = Path(history_file)

        if not path.is_file():
            readline.write_history_file(history_file)

        logger.info(f"reading history from file {history_file}")
        readline.read_history_file(history_file)

        logger.info(f"setting input completions")
        readline.parse_and_bind("tab: complete")
        readline.set_completer(self.input_completions)

        print(self.get_welcome_text())

    def complete_repl(self):
        """
        Complete the REPL

        The session history is written to the configured history_file
        """

        history_file = self.get_conf_item('history_file')
        logger.info(f"writing history to file {history_file}")
        readline.write_history_file(history_file)

    def repl(self):
        """
        Enter the REPL

        The REPL is initialised, an atexit handler is registered to complete
        the REPL on exit, and the REPL is then entered in an infinite loop.
        The user is prompted to run database queries or REPL commands,
        such as the quit command
        """

        prompt = f"{self.conn.engine.url.database} => "
        self.init_repl()
        atexit.register(self.complete_repl)

        while True:
            raw = self.prompt_for_input(prompt=prompt)
            self.process_input(raw)

