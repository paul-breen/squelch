# squelch

Squelch is a package providing a Simple SQL REPL Command Handler.  Squelch uses SQLAlchemy for database access and so can support any database engine that SQLAlchemy supports, thereby providing a common database client experience for any of those database engines.  Squelch is modelled on a simplified `psql`, the PostgreSQL command line client.  The Squelch CLI supports readline history and basic SQL statement tab completions.

## Install

The package can be installed from PyPI:

```bash
$ pip install squelch
```

## From the command line

The package comes with a functional CLI called `squelch`, which just calls the package *main*, hence the following two invocations are equivalent:

```bash
$ python3 -m squelch
```

```bash
$ squelch
```

The only required argument is a database connection URL.  This can either be passed on the command line, via the `--url` option, or specified in a JSON configuration file given by the `--conf-file` option.  The form of the JSON configuration file is as follows:

```json
{
  "url": "<URL>"
}
```

where the `<URL>` follows the [SQLAlchemy database connection URL syntax](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).  An advantage of using a configuration file is that it avoids providing database login credentials in plain text on the command line.

### Command line usage

```
usage: squelch [-h] [-c CONF_FILE] [-u URL] [-v] [-V]

Squelch is a Simple SQL REPL Command Handler.

optional arguments:
  -h, --help            show this help message and exit
  -c CONF_FILE, --conf-file CONF_FILE
                        The full path to a JSON configuration file. It
                        defaults to ./squelch.json.
  -u URL, --url URL     The database connection URL, as required by
                        sqlalchemy.create_engine().
  -v, --verbose         Turn verbose messaging on. The effects of this option
                        are incremental.
  -V, --version         show program's version number and exit

Database Connection URL

The database connection URL can either be passed on the command line, via the --url option, or specified in a JSON configuration file given by the --conf-file option.  The form of the JSON configuration file is as follows:

{
  "url": "<URL>"
}

From the SQLAlchemy documentation:

"The string form of the URL is dialect[+driver]://user:password@host/dbname[?key=value..], where dialect is a database name such as mysql, oracle, postgresql, etc., and driver the name of a DBAPI, such as psycopg2, pyodbc, cx_oracle, etc. Alternatively, the URL can be an instance of URL."
```

