# squelch

Squelch is a package providing a Simple [SQL](https://en.wikipedia.org/wiki/SQL) [REPL](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) Command Handler.  Squelch uses [SQLAlchemy](https://www.sqlalchemy.org/) for database access and so can support any database engine that SQLAlchemy supports, thereby providing a common database client experience for any of those database engines.  Squelch is modelled on a simplified `psql`, the [PostgreSQL](https://www.postgresql.org/) command line client.  The Squelch [CLI](https://en.wikipedia.org/wiki/Command-line_interface) supports readline history and basic SQL statement tab completions.

## Install

The package can be installed from [PyPI](https://pypi.org/):

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

The only required argument is a database connection URL.  This can either be passed on the command line, via the `--url` option, or specified in a [JSON](https://en.wikipedia.org/wiki/JSON) configuration file given by the `--conf-file` option.  The form of the JSON configuration file is as follows:

```json
{
  "url": "<URL>"
}
```

where the `<URL>` follows the [SQLAlchemy database connection URL syntax](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).  An advantage of using a configuration file is that it avoids providing database login credentials in plain text on the command line.

### Running queries

When running the CLI in a terminal, the user is dropped into an interactive REPL.  From here, the user is prompted for input, which can be an SQL statement to be sent to the database engine, or a CLI command (backslash command) such as `\q` to quit the CLI:

```
$ python -m squelch -c tests/data/test.json 
squelch (0.3.0)
Type "help" for help.

tests/data/test.db => select * from data;
 id   | name   | status   | key
------+--------+----------+-----------
 1    | pmb    | 0        | 0000-0000
 2    | abc    | 0        | 0000-0001
 3    | def    | 0        | 0000-0002
 4    | ghi    | 1        | 0000-0003
(4 rows)

tests/data/test.db => \q
```

Alternatively, the CLI can be called as a *one-shot* by providing a query on `stdin`, thereby allowing it to be called in scripts.

For example, using `echo` to pipe a query to the CLI:

```bash
$ echo "select * from data" | python -m squelch -c tests/data/test.json
 id   | name   | status   | key
------+--------+----------+-----------
 1    | pmb    | 0        | 0000-0000
 2    | abc    | 0        | 0000-0001
 3    | def    | 0        | 0000-0002
 4    | ghi    | 1        | 0000-0003
(4 rows)

```

Or redirecting from a file.  Given the following queries in a file:

```bash
$ cat tests/data/queries.sql
select * from data;
select * from data where id = 1;
select * from status where status = 1;
```

the result would be:

```bash
$ python -m squelch -c tests/data/test.json < tests/data/queries.sql
 id   | name   | status   | key
------+--------+----------+-----------
 1    | pmb    | 0        | 0000-0000
 2    | abc    | 0        | 0000-0001
 3    | def    | 0        | 0000-0002
 4    | ghi    | 1        | 0000-0003
(4 rows)

 id   | name   | status   | key
------+--------+----------+-----------
 1    | pmb    | 0        | 0000-0000
(1 row)

 name   | status
--------+----------
 ghi    | 1
(1 row)

```

#### Machine-readable data in scripts

It's likely that when calling the CLI from a script, the user is less interested in the data being laid out in a human-readable table, rather, they probably want it as machine-readable data.  The table format can be set (using the `--pset` option) to `csv` so that the table is printed as [CSV](https://en.wikipedia.org/wiki/Comma-separated_values).  Additionally, the table footer can be turned off (again using `--pset`) so that the result is just a simple CSV table.  Taking our example from earlier, the result would be:

```bash
$ echo "select * from data;" | python -m squelch -c tests/data/test.json --pset format=csv --pset footer=off
id,name,status,key
1,pmb,0,0000-0000
2,abc,0,0000-0001
3,def,0,0000-0002
4,ghi,1,0000-0003

```

### Command line usage

```
usage: squelch [-h] [-c CONF_FILE] [-u URL] [-S [NAME=VALUE [NAME=VALUE ...]]]
               [-P [NAME=VALUE [NAME=VALUE ...]]] [-v] [-V]

Squelch is a Simple SQL REPL Command Handler.

optional arguments:
  -h, --help            show this help message and exit
  -c CONF_FILE, --conf-file CONF_FILE
                        The full path to a JSON configuration file. It
                        defaults to ./squelch.json.
  -u URL, --url URL     The database connection URL, as required by
                        sqlalchemy.create_engine().
  -S [NAME=VALUE [NAME=VALUE ...]], --set [NAME=VALUE [NAME=VALUE ...]]
                        Set state variable NAME to VALUE.
  -P [NAME=VALUE [NAME=VALUE ...]], --pset [NAME=VALUE [NAME=VALUE ...]]
                        Set printing state variable NAME to VALUE.
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

