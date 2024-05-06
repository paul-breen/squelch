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
    assert squelch.__version__ == '0.3.0'

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

@pytest.mark.parametrize(['opts','changes'], [
({}, {}),
({'tablefmt': squelch.DEF_TABLE_FORMAT}, {}),
({'tablefmt': 'html'}, {'tablefmt': 'html'}),
({'tablefmt': 'aligned'}, {'tablefmt': squelch.TABLE_FORMAT_ALIASES['aligned']}),
({'tablefmt': 'unaligned'}, {'tablefmt': squelch.TABLE_FORMAT_ALIASES['unaligned'], 'stralign': None}),
({'tablefmt': 'csv'}, {'tablefmt': squelch.TABLE_FORMAT_ALIASES['csv'], 'stralign': None}),
({'showindex': True}, {'showindex': True}),
({'disable_numparse': True}, {'disable_numparse': True}),
])
def test_set_table_opts(unconfigured_squelch, opts, changes):
    f = unconfigured_squelch
    expected = f.DEFAULTS['table_opts'].copy()
    expected.update(changes)
    f.set_table_opts(**opts)
    actual = f.conf['table_opts']
    assert actual == expected

@pytest.mark.parametrize(['state','key','cmd','expected'], [
({'pager': True}, 'pager', r'\pset pager off', False),
({'pager': False}, 'pager', r'\pset pager off', False),
({'pager': True}, 'pager', r'\pset pager on', True),
({'pager': False}, 'pager', r'\pset pager on', True),
({'pager': True}, 'pager', r'\PSET PAGER OFF', False),
({'pager': True}, 'pager', r'\PSET PAGER ON', True),
({'pager': True}, 'pager', r'\pset pager false', False),
({'pager': True}, 'pager', r'\pset pager 0', False),
({'pager': False}, 'pager', r'\pset pager true', True),
({'pager': False}, 'pager', r'\pset pager 1', True),
({'AUTOCOMMIT': True}, 'AUTOCOMMIT', r'\set AUTOCOMMIT on', True),
({'AUTOCOMMIT': True}, 'AUTOCOMMIT', r'\set AUTOCOMMIT off', False),
({'AUTOCOMMIT': False}, 'AUTOCOMMIT', r'\set AUTOCOMMIT on', True),
({'AUTOCOMMIT': True}, 'AUTOCOMMIT', r'\set autocommit off', False),
])
def test_set_state(unconfigured_squelch, state, key, cmd, expected):
    f = unconfigured_squelch
    f.state = state
    f.set_state(cmd)
    actual = f.state[key]
    assert actual == expected

@pytest.mark.parametrize(['cmd','func'], [
('help', 'get_help_summary_text'),
(r'\?', 'get_help_repl_cmd_text'),
])
def test_get_help(unconfigured_squelch, cmd, func):
    f = unconfigured_squelch
    expected = getattr(f, func)()
    actual = f.get_help(cmd)
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

@pytest.mark.parametrize(['data','state','kwargs','term_size','expected'], [
('', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((80,24)), False),
('short\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((80,24)), False),
('short\nshort\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((80,24)), False),
('short\nshort\nshort\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((80,24)), False),
('short\nshort\nshort\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((80,4)), False),
('short\nshort\nshort\nshort\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((80,4)), True),
('short\nshort\nshort\nshort\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((5,24)), True),
('short\nshort\nshort\nshort\n', {'pager': False}, {'sep': '\n', 'nsample': 2}, os.terminal_size((5,24)), False),
('short\nshort\nshort\nreally long but wont be sampled\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((10,24)), False),
('short\nreally long and will be sampled\nshort\nshort\n', {'pager': True}, {'sep': '\n', 'nsample': 2}, os.terminal_size((10,24)), True),
('short\nshort\nshort\nreally long and will be sampled\n', {'pager': True}, {'sep': '\n', 'nsample': 5}, os.terminal_size((10,24)), False),
])
def test_use_pager(unconfigured_squelch, data, state, kwargs, term_size, expected, mocker):
    f = unconfigured_squelch
    f.state = state
    mocker.patch('squelch.shutil.get_terminal_size', return_value=term_size)
    actual = f.use_pager(data, **kwargs)
    assert actual == expected

@pytest.mark.parametrize(['nrows','expected'], [
(2, '\n(2 rows)\n'),
(1, '\n(1 row)\n'),
(0, '\n(0 rows)\n'),
(-1, '\n'),
(-2, '\n(-2 rows)\n'),
])
def test_get_table_footer_text(unconfigured_squelch, nrows, expected):
    f = unconfigured_squelch
    actual = f.get_table_footer_text(nrows)
    assert actual == expected

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
        use_pager = mocker.patch.object(f, 'use_pager', return_value=state['pager'])

    if result is None:
        result = mocker.patch('sqlalchemy.engine.cursor.CursorResult')
        result.returns_rows = True
        mocker.patch.object(result, 'values', return_value=[[11,21]])

        if headers:
            mocker.patch.object(result, 'keys', return_value=headers)

    f.result = result
    f.state = state
    f.present_result(table_opts=table_opts)

    if result:
        if state['pager']:
            use_pager.assert_called_once()
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

# These parameters are shared across multiple tests
raw_input_params = pytest.mark.parametrize(['raw','expected','terminator'], [
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

@raw_input_params
def test_clean_raw_input(unconfigured_squelch, raw, expected, terminator):
    f = unconfigured_squelch
    actual = f.clean_raw_input(raw, terminator=terminator)
    assert actual == expected

@raw_input_params
def test_prompt_for_input(unconfigured_squelch, raw, expected, terminator, mocker):
    f = unconfigured_squelch
    mocker.patch('squelch.input', return_value=raw)
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

@pytest.mark.parametrize(['raw','expected'], [
("select * from data", ''),
("0", ''),
(r"\pset", None),
(r"\pset pager on", 'Pager is used for long output.'),
(r"\pset pager off", 'Pager usage is off.'),
])
def test_handle_state_command(unconfigured_squelch, raw, expected, capsys):
    f = unconfigured_squelch

    if expected is None:
        expected = str(f.state)

    f.handle_state_command(raw)
    captured = capsys.readouterr()
    assert expected in captured.out

@pytest.mark.parametrize(['raw','expected'], [
(r"\d", 'all'),
(r"\dt", 'all'),
(r"\dv", 'all'),
(r"\ds", 'all'),
(r"\ddummy", ''),
(r"\d data", 'data'),
(r"\dt data", 'data'),
(r"\dv data", 'data'),
(r"\ds data", 'data'),
(r"\ddummy data", ''),
])
def test_handle_metadata_command(unconfigured_squelch, raw, expected, mocker, capsys):
    f = unconfigured_squelch
    far = mocker.patch.object(f, 'get_metadata_table_for_relation_types', return_value=expected)
    fr = mocker.patch.object(f, 'get_metadata_table_for_relation', return_value=expected)
    f.handle_metadata_command(raw)
    captured = capsys.readouterr()
    assert expected in captured.out

    if expected != '':
        if len(raw.split()) > 1:
            fr.assert_called_once()
        else:
            far.assert_called_once()

@pytest.mark.parametrize(['raw','query','params','autocommit'], [
("begin", text('begin'), {}, False),
("rollback", text('rollback'), {}, True),
("commit", text('commit'), {}, True),
("select * from data", text('select * from data'), {}, True),
("select * from data where id = :id", text('select * from data where id = :id'), {'id': '1'}, True),
])
def test_handle_query(unconfigured_squelch, raw, query, params, autocommit, mocker):
    f = unconfigured_squelch
    pqp = mocker.patch.object(f, 'prompt_for_query_params', return_value=params)
    eq = mocker.patch.object(f, 'exec_query')
    pr = mocker.patch.object(f, 'present_result')
    f.handle_query(raw)
    assert f.query.compare(query)
    assert f.params == params
    assert f.state['AUTOCOMMIT'] == autocommit
    pqp.assert_called_once_with(raw)
    # text(query) is not directly comparable, hence we use the actual
    eq.assert_called_once_with(f.query, f.params)
    pr.assert_called_once()

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
    mocker.patch.object(f, 'get_relation_names', return_value=[])
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

