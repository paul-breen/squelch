import os
import logging

import pytest
from sqlalchemy.sql import text
from sqlalchemy.exc import DatabaseError

import squelch

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

def test_version():
    assert squelch.__version__ == '0.1.0'

@pytest.mark.parametrize(['file','expected'], [
('non-existent.json', {}),
(base + '/data/min.json', {'url': 'dialect[+driver]://user:password@host/dbname'}),
(base + '/data/extras.json', {'url': 'dialect[+driver]://user:password@host/dbname', 'verbose': 2}),
])
def test_get_conf(unconfigured_squelch, file, expected):
    f = unconfigured_squelch
    f.get_conf(file=file)
    assert f.conf == expected

@pytest.mark.parametrize(['file','expected'], [
(base + '/data/min.json', {'url': 'dialect[+driver]://user:password@host/dbname'}),
])
def test_get_conf_debug(unconfigured_squelch, file, expected, caplog):
    caplog.set_level(logging.DEBUG)
    f = unconfigured_squelch
    f.get_conf(file=file)
    assert f.conf == expected
    assert squelch.URL_CRED_REPLACE in caplog.text

@pytest.mark.parametrize(['conf','key','expected'], [
({'url': 'd://u:p@h/db'}, 'url', 'd://u:p@h/db'),
({'url': 'd://u:p@h/db', 'verbose': 2}, 'verbose', 2),
({}, 'repl_commands', squelch.Squelch.DEFAULTS['repl_commands']),
])
def test_get_conf_item(unconfigured_squelch, conf, key, expected):
    f = unconfigured_squelch
    f.conf = conf
    actual = f.get_conf_item(key)
    assert actual == expected

@pytest.mark.parametrize(['conf','key','expected'], [
({}, 'non-existent-key', ''),
])
def test_get_conf_item_error(unconfigured_squelch, conf, key, expected):
    f = unconfigured_squelch
    f.conf = conf

    with pytest.raises(KeyError) as e:
        actual = f.get_conf_item(key)

@pytest.mark.parametrize(['state','key','cmd','expected'], [
({'pager': True}, 'pager', r'\pset pager off', False),
({'pager': False}, 'pager', r'\pset pager off', False),
({'pager': True}, 'pager', r'\pset pager on', True),
({'pager': False}, 'pager', r'\pset pager on', True),
({'pager': True}, 'pager', r'\PSET PAGER OFF', False),
({'pager': True}, 'pager', r'\PSET PAGER ON', True),
])
def test_set_state(unconfigured_squelch, state, key, cmd, expected):
    f = unconfigured_squelch
    f.state = state
    f.set_state(cmd)
    actual = f.state[key]
    assert actual == expected

@pytest.mark.parametrize(['query','params'], [
(text('select * from data'), {}),
])
def test_exec_query(unconfigured_squelch, query, params, mocker, capsys):
    f = unconfigured_squelch
    f.conn = mocker.patch('sqlalchemy.engine.base.Connection')
    mocker.patch.object(f.conn, 'begin')
    mocker.patch.object(f.conn, 'execute', side_effect=DatabaseError('sentinel text', {}, Exception))
    f.exec_query(query, params)
    captured = capsys.readouterr()
    assert 'sentinel text' in captured.err

@pytest.mark.parametrize(['query','params'], [
(text('select * from data'), {}),
])
def test_exec_query_debug(unconfigured_squelch, query, params, mocker, capsys, caplog):
    caplog.set_level(logging.DEBUG)
    f = unconfigured_squelch
    f.conn = mocker.patch('sqlalchemy.engine.base.Connection')
    mocker.patch.object(f.conn, 'begin')
    mocker.patch.object(f.conn, 'execute', side_effect=DatabaseError('sentinel text', {}, Exception))
    f.exec_query(query, params)
    captured = capsys.readouterr()
    assert 'sentinel text' in captured.err
    assert 'Traceback (most recent call last)' in captured.err

@pytest.mark.parametrize(['result','headers','state','table_opts','expected'], [
({}, [], {'pager': True}, None, ''),
(None, [], {'pager': True}, None, ''),
(None, [], {'pager': False}, None, ''),
(None, ['id','title'], {'pager': False}, None, 'id'),
(None, ['id','title'], {'pager': False}, {'tablefmt': 'plain', 'showindex': False}, 'id'),
])
def test_present_result(unconfigured_squelch, result, headers, state, table_opts, expected, mocker, capsys):
    f = unconfigured_squelch

    if state['pager']:
        pager = mocker.patch('pydoc.pager')

    if result is None:
        result = mocker.patch('sqlalchemy.engine.cursor.CursorResult')

        if headers:
            mocker.patch.object(result, 'keys', return_value=headers)

    f.result = result
    f.state = state
    f.present_result(table_opts=table_opts)

    if result:
        if state['pager']:
            pager.assert_called_once()
        else:
            captured = capsys.readouterr()
            assert expected in captured.out

@pytest.mark.parametrize(['raw','values','expected'], [
("select * from data", [], {}),
("select * from data where id = :id", ['1'], {'id': '1'}),
("select * from data where name = :name and status = :status", ['primary','0'], {'name': 'primary', 'status': '0'}),
# Our code filters parameter-like text within strings (the clean step).
# Interestingly though, sqlalchemy itself will baulk on this
("select * from data where name = :name and status = :status and key = ':key'", ['primary','0'], {'name': 'primary', 'status': '0'}),
])
def test_prompt_for_query_params(unconfigured_squelch, raw, values, expected, mocker):
    f = unconfigured_squelch
    mocker.patch('squelch.input', side_effect=values)
    f.prompt_for_query_params(raw)
    assert f.params == expected

@pytest.mark.parametrize(['value','expected','terminator'], [
("select * from data", "select * from data", ';'),
("select * from data where id = :id", "select * from data where id = :id", ';'),
("select * from data  ", "select * from data", ';'),
("  select * from data", "select * from data", ';'),
("  select * from data  ", "select * from data", ';'),
("select * from data;", "select * from data", ';'),
("  select * from data;  ", "select * from data", ';'),
# N.B.: Spaces before a terminator won't be stripped
("  select * from data  ;", "select * from data  ", ';'),
("select * from data where id = :id;", "select * from data where id = :id", ';'),
("select * from data where id = :id;  ", "select * from data where id = :id", ';'),
# Alternative terminators
("select * from data where id = :id/", "select * from data where id = :id", '/'),
(r"select * from data where id = :id\n/", "select * from data where id = :id", r'\n/'),
(r"select * from data where id = :id\\", "select * from data where id = :id", r'\\'),
("select * from data where id = :id%", "select * from data where id = :id", '%'),
])
def test_prompt_for_input(unconfigured_squelch, value, expected, terminator, mocker):
    f = unconfigured_squelch
    mocker.patch('squelch.input', return_value=value)
    actual = f.prompt_for_input(terminator=terminator)
    assert actual == expected

@pytest.mark.parametrize(['raw','query','params'], [
("select * from data", text("select * from data"), {}),
("select * from data where id = :id", text("select * from data where id = :id"), {'id': '1'}),
# Anything that's not a REPL command or blank is treated as a query
("0", text("0"), {}),
])
def test_process_input_query(unconfigured_squelch, raw, query, params, mocker):
    f = unconfigured_squelch
    mocker.patch.object(f, 'prompt_for_query_params', return_value=params)
    mocker.patch.object(f, 'exec_query')
    mocker.patch.object(f, 'present_result')
    f.process_input(raw)
    assert f.query.compare(query)
    assert f.params == params

@pytest.mark.parametrize('raw', [
(r"\q"),
])
def test_process_input_cmd_quit(unconfigured_squelch, raw):
    f = unconfigured_squelch

    with pytest.raises(SystemExit) as e:
        f.process_input(raw)

    assert e.type == SystemExit
    assert e.value.code == 0

@pytest.mark.parametrize(['state','key','raw','expected'], [
({'pager': True}, 'pager', r'\pset pager off', False),
({'pager': False}, 'pager', r'\pset pager off', False),
({'pager': True}, 'pager', r'\pset pager on', True),
({'pager': False}, 'pager', r'\pset pager on', True),
])
def test_process_input_cmd_state(unconfigured_squelch, state, key, raw, expected):
    f = unconfigured_squelch
    f.state = state
    f.process_input(raw)
    actual = f.state[key]
    assert actual == expected

@pytest.mark.parametrize(['raw','q'], [
('', 'y'),
('', 'yes'),
('', 'Y'),
('', 'YES'),
('', 'yay'),
('', 'n'),
('', 'no'),
('', 'N'),
('', 'NO'),
('', 'nay'),
])
def test_process_input_empty(unconfigured_squelch, raw, q, mocker):
    f = unconfigured_squelch
    mocker.patch('squelch.input', return_value=q)

    if q.lower().startswith('y'):
        with pytest.raises(SystemExit) as e:
            f.process_input(raw)

        assert e.type == SystemExit
        assert e.value.code == 0
    else:
        f.process_input(raw)

@pytest.mark.parametrize(['text','state','expected'], [
('', 0, squelch.SQL_COMPLETIONS[0]),
('sel', 0, 'select'),
('cr', 0, 'create'),
('d', 0, 'delete'),
('d', 1, 'drop'),
('non-existent', 0, None),
])
def test_input_completions(unconfigured_squelch, text, state, expected):
    f = unconfigured_squelch
    actual = f.input_completions(text, state)
    assert actual == expected

@pytest.mark.parametrize(['history_file','exists'], [
('non-existent-file_squelch_history', False),
(base + '/data/.squelch_history', True),
])
def test_init_repl(unconfigured_squelch, history_file, exists, mocker):
    f = unconfigured_squelch
    f.conf['history_file'] = history_file
    wh = mocker.patch('readline.write_history_file')
    rh = mocker.patch('readline.read_history_file')
    pb = mocker.patch('readline.parse_and_bind')
    sc = mocker.patch('readline.set_completer')
    f.init_repl()

    if not exists:
        wh.assert_called_once_with(history_file)

    rh.assert_called_once_with(history_file)
    pb.assert_called_once_with("tab: complete")
    sc.assert_called_once_with(f.input_completions)

@pytest.mark.parametrize('history_file', [
('non-existent-file_squelch_history'),
(base + '/data/.squelch_history'),
])
def test_complete_repl(unconfigured_squelch, history_file, mocker):
    f = unconfigured_squelch
    f.conf['history_file'] = history_file
    wh = mocker.patch('readline.write_history_file')
    f.complete_repl()
    wh.assert_called_once_with(history_file)

