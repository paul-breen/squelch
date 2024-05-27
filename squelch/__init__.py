__version__ = '0.3.1'

import sys
import logging
from pathlib import Path
import atexit
import traceback
import re
import json
import readline
import pydoc
import shutil
import warnings

from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.sql import text
from sqlalchemy.exc import DatabaseError, NoSuchTableError
from tabulate import tabulate, simple_separated_format

PROGNAME = __name__

DEF_TABLE_FORMAT = 'presto'
DEF_CONF_FILE = './squelch.json'
DEF_HISTORY_FILE = Path('~/.squelch_history').expanduser()
DEF_CONF = {}
DEF_STATE = {'pager': True, 'footer': True, 'format': DEF_TABLE_FORMAT, 'AUTOCOMMIT': True}
DEF_MIN_FOOTER = '\n'             # Blank line to separate table from prompt

URL_CRED_PATTERN = r'://(.+)@'
URL_CRED_REPLACE = r'://***@'

SQL_COMPLETIONS = ['select', 'insert', 'update', 'delete', 'create', 'drop', 'from', 'where', 'and', 'or', 'not', 'like', 'order by', 'group by', 'into', 'values','begin', 'transaction', 'commit', 'rollback']

DEF_REL_TYPES = ['table', 'view', 'sequence']

TABLE_FORMAT_ALIASES = {'aligned': DEF_TABLE_FORMAT, 'unaligned': simple_separated_format('|'), 'csv': simple_separated_format(',')}
UNALIGNED_TABLE_FORMATS = ['unaligned', 'csv']

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
            'metadata': [r'\d', r'\dt', r'\dv', r'\ds', r'\di'],
            'help': [r'help', r'\?'],
            'dist': [r'\copyright']
        },
        'table_opts': {
            # Unfortunately, tabulate doesn't recognise a sqlalchemy result
            # as having keys(), so we can't set 'headers': 'keys' here
            'tablefmt': DEF_TABLE_FORMAT, 'showindex': False, 'disable_numparse': True
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
        self.completions = SQL_COMPLETIONS

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

    def set_table_opts(self, **opts):
        """
        Set the progam's table formatting options

        The table_opts are updated in self.conf or added if they don't exist

        :param opts: Keyword args that correspond to members of the table_opts
        dict
        :type opts: dict
        """

        # Ensure we update table_opts from self.conf and not self.DEFAULTS
        if 'table_opts' not in self.conf:
            self.conf['table_opts'] = self.DEFAULTS['table_opts'].copy()

        # Handle some specific table format cases
        if 'tablefmt' in opts:
            # For an unaligned table we have to set the alignment to None,
            # but for an aligned table we just remove any alignment option
            # so that it's done automatically
            if opts['tablefmt'] in UNALIGNED_TABLE_FORMATS:
                opts['stralign'] = None
            else:
                opts.pop('stralign', None)
                self.conf['table_opts'].pop('stralign', None)

            if opts['tablefmt'] in TABLE_FORMAT_ALIASES:
                opts['tablefmt'] = TABLE_FORMAT_ALIASES[opts['tablefmt']]

        self.conf['table_opts'].update(**opts)

    def set_state(self, cmd):
        """
        Set the progam's runtime state according to the given command

        The state variable in self.state is updated according to command

        :param cmd: The command to update the program's state
        :type cmd: str
        :returns: Text of the state update with which to notify the user
        :rtype: str
        """

        state_text = ''
        falsy = r'(off|false|0|no)'
        truthy = r'(on|true|1|yes)'

        # Irrespective of the actual case of the state variable, we always
        # compare case-insensitively for ease of the user.  No two state
        # variable names should only differ by case, so this is quite safe
        if cmd.lower().startswith(r'\pset pager'):
            if re.match(fr'\\pset\s+pager\s+{falsy}', cmd.lower()):
                self.state['pager'] = False
                state_text = 'Pager usage is off.'
            elif re.match(fr'\\pset\s+pager\s+{truthy}', cmd.lower()):
                self.state['pager'] = True
                state_text = 'Pager is used for long output.'
        elif cmd.lower().startswith(r'\pset footer'):
            if re.match(fr'\\pset\s+footer\s+{falsy}', cmd.lower()):
                self.state['footer'] = False
            elif re.match(fr'\\pset\s+footer\s+{truthy}', cmd.lower()):
                self.state['footer'] = True
        elif cmd.lower().startswith(r'\pset format'):
            m = re.match(fr'\\pset\s+format\s+(.+)', cmd.lower())

            if m:
                fmt = m.groups()[0]
                self.state['format'] = fmt
                self.set_table_opts(tablefmt=fmt)
        elif cmd.lower().startswith(r'\set autocommit'):
            if re.match(fr'\\set\s+autocommit\s+{falsy}', cmd.lower()):
                self.state['AUTOCOMMIT'] = False
            elif re.match(fr'\\set\s+autocommit\s+{truthy}', cmd.lower()):
                self.state['AUTOCOMMIT'] = True

        return state_text

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

Informational
  \d                     list tables, views, and sequences
  \d      NAME           describe table or view
  \di     [NAME]         list indexes
  \ds     [NAME]         list sequences
  \dt     [NAME]         list tables
  \dv     [NAME]         list views

Formatting
  \pset [NAME [VALUE]]   set table output option
                         (pager)

Variables
  \set [NAME [VALUE]]    set internal variable, or list all if no parameters
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

        * The query result set is stored in self.result.
        * If AUTOCOMMIT is on, the query is executed in a DB transaction
          and automatically committed on successful execution.

        :param query: The query to execute
        :type query: sqlalchemy.sql.text
        :param params: Optional query parameters to bind in the query
        :type params: dict
        :returns: The query result set
        :rtype: sqlalchemy.engine.CursorResult
        """

        self.result = None

        try:
            self.result = self.conn.execute(query, params)
            self.state.get('AUTOCOMMIT') and self.conn.commit()
        except DatabaseError as e:
            if logger.isEnabledFor(logging.DEBUG):
                traceback.print_exc(chain=False)
            else:
                print(e, file=sys.stderr)

        return self.result

    def use_pager(self, data, sep='\n', nsample=2):
        """
        Determine whether we should use the pager or not

        We sample the first few lines of data.  Table grid lines are usually
        the longest lines and so we try to use this to determine if the
        output is wider than the terminal.  To turn this off, set sample=-1,
        which will mean we just check the length of the first line

        :param data: The data to be output
        :type data: str
        :param sep: The separator string of the lines in data
        :type sep: str
        :param nsample: The number of lines to sample from data to find the
        longest line length
        :type nsample: int
        :returns: A flag indicating whether to use the pager or not
        :rtype: bool
        """

        use_pager = False

        if self.state.get('pager'):
            ts = shutil.get_terminal_size()
            nlines = data.count(sep)

            if nsample != -1 and nlines >= nsample:
                sample_lines = data.split(sep, maxsplit=nsample)
                ncolumns = max([len(i) for i in sample_lines[0:nsample]])
            else:
                ncolumns = data.find(sep)

            if nlines < ts.lines and ncolumns < ts.columns:
                use_pager = False
            else:
                use_pager = True

        return use_pager

    def print_data(self, data):
        """
        Print the given data to stdout

        The output is paged if the 'pager' state variable is set and the data
        would overflow the terminal

        :param data: The data to be output
        :type data: str
        """

        if self.use_pager(data):
            pydoc.pager(data)
        else:
            print(data)

    def get_table_footer_text(self, nrows):
        """
        Get the text for a table footer

        The footer text shows the row count of the table data

        * If the state variable 'footer' is False, the only the minimum footer
          is returned

        :param nrows: The number of rows in the table.  If nrows == -1, then
        the footer text only consists of the minimum footer text
        :type nrows: int
        :returns: The table footer text
        :rtype: str
        """

        footer = DEF_MIN_FOOTER

        if self.state.get('footer'):
            if nrows != -1:
                row_text = 'row' if nrows == 1 else 'rows'
                footer = f'\n({nrows} {row_text})\n'

        return footer

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

        nrows = -1

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

        if nrows == -1:
            logger.debug(f"row count not available")

        return self.get_table_footer_text(nrows)

    def get_command_response(self):
        """
        Get the non-query command response

        The response shows the command name and the affected row count

        * Access to the row count is dependent on the DB engine supporting
          this.  It should be supported for UPDATE and DELETE, but may not
          be supported for INSERT or SELECT.

        :returns: The command response text
        :rtype: str
        """

        response = ''

        if self.query.text:
            response = self.query.text.split()[0].upper()

        if self.result.supports_sane_rowcount and self.result.rowcount != -1:
            logger.debug(f"row count available in the result cursor")
            response = f'{response} {self.result.rowcount}'

        return response

    def present_result(self, table_opts=None):
        """
        Present the result set of the latest executed query

        The query result set in self.result is presented as a table

        * If the state variable 'pager' is True, the output is paged using the
          system pager, otherwise the whole table is printed to the output
          stream.
        * If the state variable 'footer' is True, then a footer is appended to
          the result table.

        :param table_opts: Options for rendering the tabulated result output
        :type table_opts: dict
        """

        table_opts = table_opts or self.get_conf_item('table_opts')
        logger.debug(f"table_opts: {table_opts}")

        if self.result:
            if self.result.returns_rows:
                table = tabulate(self.result, headers=self.result.keys(), **table_opts)

                if table:
                    table += self.get_result_table_footer(table, table_opts)
                    self.print_data(table)
            else:
                print(self.get_command_response())

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

    def clean_raw_input(self, raw, terminator=';'):
        """
        Clean the raw input query

        :param raw: The raw query
        :type raw: str
        :param terminator: A query terminator to be stripped from the query
        :type terminator: str
        :returns: The raw stripped query
        :rtype: str
        """

        raw = raw.strip().rstrip(terminator)
        logger.debug(f"raw stripped query: '{raw}'")

        return raw

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

        return self.clean_raw_input(input(prompt), terminator=terminator)

    def handle_state_command(self, raw):
        """
        Process the given state change command

        :param raw: The raw stripped input (a REPL state command)
        :type raw: str
        """

        if len(raw.split()) > 1:
            state_text = self.set_state(raw)

            if state_text:
                print(state_text)
        else:
            print(self.state)

    def _get_relation_type_names(self, func, type_name):
        """
        Get a list of relation names of the given type

        :param func: The function to provide the list of relation names
        :type func: function reference
        :param type_name: The name of the relation type
        :type type_name: str
        :returns: A list of relation names
        :rtype: list
        """

        rel_names = []

        try:
            rel_names = func()
        except NotImplementedError as e:
            if logger.isEnabledFor(logging.INFO):
                warnings.warn(f"Engine does not provide a list of {type_name} names")

        return rel_names

    def get_relation_names(self, types=DEF_REL_TYPES):
        """
        Get a list of relation names for the given relation types

        :param types: Include types in the list of relations
        :type types: list
        :returns: A list of relation names for the given relation types
        :rtype: list
        """

        # Using the Inspector for introspection is much faster than using
        # MetaData.  Hence, even though it means pulling in more functionality
        # from SQLAlchemy (we have to use MetaData for table metadata), it is
        # worth it
        insp = inspect(self.conn.engine)
        rel_names = []

        for type_name in types:
            rel_names += [i for i in self._get_relation_type_names(getattr(insp, f"get_{type_name}_names"), type_name)]

        rel_names.sort()

        return rel_names

    def get_metadata_for_relation(self, name):
        """
        Get the metadata object for the given relation

        :param name: The name of the relation
        :type name: str
        :returns: The relation metadata object or None if the relation
        doesn't exist
        :rtype: sqlalchemy.schema.Table or None
        """

        rel_md = None
        md = MetaData()
        md.reflect(bind=self.conn.engine)

        try:
            rel_md = Table(name, md, autoload_with=self.conn.engine)
        except NoSuchTableError as e:
            msg = f'Did not find any relation named "{e}".'

            if logger.isEnabledFor(logging.DEBUG):
                traceback.print_exc(chain=False)
            else:
                print(msg, file=sys.stderr)

        return rel_md

    def get_metadata_table_for_relation_types(self, types=DEF_REL_TYPES):
        """
        Get the table showing metadata for the given relation types

        :param types: Include types in the list of relations
        :type types: list
        :returns: The table showing metadata for the given relation types
        :rtype: str
        """

        table_opts = self.get_conf_item('table_opts')
        insp = inspect(self.conn.engine)
        headers = ['Name','Type']
        rel_names = []

        for type_name in types:
            if type_name.lower() == 'index':
                res = self._get_relation_type_names(insp.get_multi_indexes, type_name)
                rel_names += [[v['name'], type_name, k[1]] for k in res for v in res[k]]
                headers = ['Name','Type','Table']
            else:
                rel_names += [[i, type_name] for i in self._get_relation_type_names(getattr(insp, f"get_{type_name}_names"), type_name)]

        rel_names.sort()
        table = 'List of relations\n'
        table += tabulate(rel_names, headers=headers, **table_opts)
        table += self.get_table_footer_text(len(rel_names))

        return table

    def get_metadata_table_for_relation(self, name):
        """
        Get the table showing metadata for the given relation

        :param name: The name of the relation
        :type name: str
        :returns: The table showing metadata for the given relation or None
        if the relation doesn't exist
        :rtype: str or None
        """

        table = None
        table_opts = self.get_conf_item('table_opts')
        rel_md = self.get_metadata_for_relation(name)

        if rel_md is not None:
            cols = [[c.name,c.type,c.nullable,c.default,c.primary_key] for c in rel_md.columns]
            idxs = [f'    "{i.name}", {",".join([c.name for c in i.expressions])}' for i in rel_md.indexes]
            table = f'Relation "{rel_md.name}"\n'
            table += tabulate(cols, headers=['Column','Type','Nullable','Default','Primary Key'], **table_opts)

            if len(idxs) > 0:
                table += f'\nIndexes:\n'
                table += '\n'.join(idxs)

            table += self.get_table_footer_text(-1)

        return table

    def handle_metadata_command(self, raw):
        """
        Process the given metadata introspection command

        :param raw: The raw stripped input (a REPL state command)
        :type raw: str
        :returns: The metadata text corresponding to the given command
        :rtype: str
        """

        table = ''

        if raw.lower() == r'\d':
            table = self.get_metadata_table_for_relation_types()
        elif raw.lower() == r'\dt':
            table = self.get_metadata_table_for_relation_types(types=['table'])
        elif raw.lower() == r'\dv':
            table = self.get_metadata_table_for_relation_types(types=['view'])
        elif raw.lower() == r'\ds':
            table = self.get_metadata_table_for_relation_types(types=['sequence'])
        elif raw.lower() == r'\di':
            table = self.get_metadata_table_for_relation_types(types=['index'])
        elif raw.lower().startswith(r'\d'):
            args = raw.split()

            if len(args) > 1:
                name = args[1]
                table = self.get_metadata_table_for_relation(name)

        if table:
            self.print_data(table)

    def handle_query(self, raw):
        """
        Process the given query

        :param raw: The raw stripped input (a query)
        :type raw: str
        """

        cmd = raw.split()[0].lower()

        # Disable autocommit during an explicit transaction
        if cmd == 'begin':
            self.state['AUTOCOMMIT'] = False

        self.query = text(raw)
        self.params = self.prompt_for_query_params(raw)
        self.exec_query(self.query, self.params)
        self.present_result()

        if cmd == 'rollback':
            self.state['AUTOCOMMIT'] = True
        elif cmd == 'commit':
            self.state['AUTOCOMMIT'] = True

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
                self.handle_state_command(raw)
            elif cmd in self.get_conf_item('repl_commands')['metadata']:
                self.handle_metadata_command(raw)
            elif cmd in self.get_conf_item('repl_commands')['help']:
                print(self.get_help(raw))
            elif cmd in self.get_conf_item('repl_commands')['dist']:
                print(self.get_dist_terms_text())
            else:
                self.handle_query(raw)
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
            matches = [i for i in self.completions if i.startswith(text.lower())] + [None]

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
        self.completions = SQL_COMPLETIONS + self.get_relation_names()
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

        prompt = f"{self.conn.engine.url.database}=> "
        self.init_repl()
        atexit.register(self.complete_repl)

        while True:
            raw = self.prompt_for_input(prompt=prompt)
            self.process_input(raw)

