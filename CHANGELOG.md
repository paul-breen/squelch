# Changelog

## [v0.4.0] - 2025-06-15

### Added

- Add support for configuration name

### Changed

- Update the package documentation

## [v0.3.1] - 2024-05-27

### Changed

- Minor typos

## [v0.3.0] - 2024-05-06

### Added

- Add support for setting different table formats via pset
- Add support for running the program as a one-shot, with queries supplied on stdin
- Add support for setting state variables on the command line
- Add support for getting index metadata
- Add functionality to determine if pager is required for the size of output
- Add support for database relations metadata introspection commands
- Add documentation for getting machine-readable output from queries
- Add documentation on how to run queries in the CLI, either interactively or as a one-shot on stdin

### Changed

- Ensure we can set multiple state variables (via --set or --pset) in a single invocation
- Separate prompting for input from cleaning raw input to allow the latter to be used elsewhere
- Improve metadata introspection and add relation names to readline completions
- Ensure the metadata tables also honour the footer state variable setting

## [v0.2.0] - 2024-04-20

### Added

- Add support for reporting response for non-query commands
- Add support for explicit transactions as well as implicit transactions
- Add support for a table footer (showing row count)
- Add interactive help commands

### Changed

- Make the minimum footer more explicit
- Improve the handling and reporting of runtime state changes
- Ensure only those results that return rows are tabulated
- Disable tabulate's parsing of numbers.  DB results often contain numbers that aren't required to be formatted as numbers (e.g. version numbers)

## [v0.1.0] - 2024-04-17

### Added

- Initial commit
- Ensure all REPL command strings are set as raw r'' strings, to avoid the backslash character raising the `DeprecationWarning: invalid escape sequence` warning
- dict.get() can't short circuit, so we have to check if conf has key before check DEFAULTS, and only then raise KeyError if key isn't present

