import os
import sys
import argparse

import pytest

import squelch
import squelch.__main__ as m

base = os.path.dirname(__file__)

@pytest.fixture
def init_squelch():
    def _init_squelch(conf):
        f = squelch.Squelch(conf=conf)

        return f

    return _init_squelch
  
@pytest.fixture
def unconfigured_squelch(init_squelch):
    conf = {}
    f = init_squelch(conf)

    return f

@pytest.mark.parametrize(['argv','expected'], [
(['main'], {}),
(['main', '-u', 'd://u:p@h/db'], {'url': 'd://u:p@h/db'}),
(['main', '-v'], {'verbose': 1}),
(['main', '-vv'], {'verbose': 2}),
(['main', '-c', '/path/to/conf.json'], {'conf_file': '/path/to/conf.json'}),
(['main', '-u', 'd://u:p@h/db', '-c', '/path/to/conf.json', '-vv'], {'url': 'd://u:p@h/db', 'conf_file': '/path/to/conf.json', 'verbose': 2}),
])
def test_update_conf_from_cmdln(unconfigured_squelch, argv, expected):
    f = unconfigured_squelch
    sys.argv = argv
    args = m.parse_cmdln()
    m.update_conf_from_cmdln(f.conf, args)
    assert f.conf == expected

@pytest.mark.parametrize(['argv','expected'], [
(['main'], {}),
(['main', '-u', 'd://u:p@h/db'], {'url': 'd://u:p@h/db'}),
(['main', '-v'], {'verbose': 1}),
(['main', '-vv'], {'verbose': 2}),
(['main', '-c', base + '/data/min.json'], {'conf_file': base + '/data/min.json', 'url': 'dialect[+driver]://user:password@host/dbname'}),
(['main', '-c', base + '/data/min.json', '-v'], {'conf_file': base + '/data/min.json', 'url': 'dialect[+driver]://user:password@host/dbname', 'verbose': 1}),
# URL on command line will override one from conf file
(['main', '-u', 'd://u:p@h/db', '-c', base + '/data/min.json', '-v'], {'conf_file': base + '/data/min.json', 'url': 'd://u:p@h/db', 'verbose': 1}),
(['main', '-c', base + '/data/min.json', '-u', 'd://u:p@h/db', '-v'], {'conf_file': base + '/data/min.json', 'url': 'd://u:p@h/db', 'verbose': 1}),
# Non-existent conf file
(['main', '-c', '/non-existent.json'], {'conf_file': '/non-existent.json'}),
(['main', '-c', '/non-existent.json', '-u', 'd://u:p@h/db', '-v'], {'conf_file': '/non-existent.json', 'url': 'd://u:p@h/db', 'verbose': 1}),
(['main', '-c', '/non-existent.json', '-v'], {'conf_file': '/non-existent.json', 'verbose': 1}),
])
def test_consolidate_conf(unconfigured_squelch, argv, expected):
    f = unconfigured_squelch
    sys.argv = argv
    args = m.parse_cmdln()
    m.consolidate_conf(f, args)
    assert f.conf == expected

@pytest.mark.parametrize(['argv','expected'], [
(['main', '-u', 'd://u:p@h/db'], {'url': 'd://u:p@h/db'}),
(['main', '-c', base + '/data/min.json'], {'conf_file': base + '/data/min.json', 'url': 'dialect[+driver]://user:password@host/dbname'}),
(['main', '-c', base + '/data/min.json', '-v'], {'conf_file': base + '/data/min.json', 'url': 'dialect[+driver]://user:password@host/dbname', 'verbose': 1}),
(['main', '-u', 'd://u:p@h/db', '-c', base + '/data/min.json', '-v'], {'conf_file': base + '/data/min.json', 'url': 'd://u:p@h/db', 'verbose': 1}),
(['main', '-c', base + '/data/min.json', '-u', 'd://u:p@h/db', '-v'], {'conf_file': base + '/data/min.json', 'url': 'd://u:p@h/db', 'verbose': 1}),
(['main', '-c', '/non-existent.json', '-u', 'd://u:p@h/db', '-v'], {'conf_file': '/non-existent.json', 'url': 'd://u:p@h/db', 'verbose': 1}),
])
def test_connect(unconfigured_squelch, argv, expected, mocker):
    f = unconfigured_squelch
    sys.argv = argv
    args = m.parse_cmdln()
    m.consolidate_conf(f, args)
    mocker.patch('squelch.Squelch.connect')
    m.connect(f, args)
    assert f.conf == expected

@pytest.mark.parametrize(['argv','expected'], [
(['main'], {}),
(['main', '-v'], {'verbose': 1}),
(['main', '-vv'], {'verbose': 2}),
(['main', '-c', '/non-existent.json'], {'conf_file': '/non-existent.json'}),
(['main', '-c', '/non-existent.json', '-v'], {'conf_file': '/non-existent.json', 'verbose': 1}),
])
def test_connect_error(unconfigured_squelch, argv, expected, mocker):
    f = unconfigured_squelch
    sys.argv = argv
    args = m.parse_cmdln()
    m.consolidate_conf(f, args)
    mocker.patch('squelch.Squelch.connect')

    # In DEBUG mode, we raise the exception for the full stack trace,
    # otherwise we just show a focussed error message and exit with non-zero
    if args.verbose > 1:
        with pytest.raises(KeyError) as e:
            m.connect(f, args)

        assert f.conf == expected
    else:
        with pytest.raises(SystemExit) as e:
            m.connect(f, args)

        assert e.type == SystemExit
        assert e.value.code == 1
        assert f.conf == expected

