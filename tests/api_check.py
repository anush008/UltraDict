#
# Behavior check of the full public API of UltraDict
#
# Every step prints one line with the step name and either the result or the
# type of the exception that was raised. Run this script against two versions
# of UltraDict and compare the output to find any change in behavior.
#
# Usage: python tests/api_check.py [path that contains the UltraDict package]
#

import sys, os

if len(sys.argv) > 1:
    sys.path.insert(0, sys.argv[1])
else:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

import collections, collections.abc, copy, io, json, marshal, multiprocessing, pickle, contextlib

from UltraDict import UltraDict

# Disable logging
if hasattr(UltraDict.log, 'disable'):
    UltraDict.log.disable(UltraDict.log.CRITICAL)

PREFIX = f'apichk{os.getpid() % 100000}_'
counter = [0]

def new_name():
    counter[0] += 1
    return f'{PREFIX}{counter[0]}'

def cleanup_name(name):
    for suffix in ('', '_memory', '_full', '_register', '_register_memory'):
        UltraDict.unlink_by_name(name + suffix, ignore_errors=True)

def fmt(value):
    """ Stable text for a result, without memory addresses or random names """
    if isinstance(value, UltraDict):
        return f'UltraDict({fmt(dict(value.data))})'
    if isinstance(value, multiprocessing.shared_memory.SharedMemory):
        return 'SharedMemory'
    if isinstance(value, dict):
        return '{' + ', '.join(f'{fmt(k)}: {fmt(v)}' for k, v in value.items()) + '}'
    if isinstance(value, (list, tuple)):
        inner = ', '.join(fmt(v) for v in value)
        return f'[{inner}]' if isinstance(value, list) else f'({inner})'
    if isinstance(value, (collections.abc.KeysView, collections.abc.ValuesView, collections.abc.ItemsView)):
        return f'{type(value).__name__}({fmt(list(value))})'
    if isinstance(value, (set, frozenset)):
        return f'{type(value).__name__}({sorted(map(fmt, value))})'
    return f'{type(value).__name__}:{value!r}'

def step(label, fn):
    try:
        result = fn()
        print(f'{label}: {fmt(result)}')
    except Exception as e: # pylint: disable=broad-except
        print(f'{label}: raises {type(e).__name__}')

def section(title):
    print(f'\n## {title}')

class JsonSerializer:
    """ Custom serializer with the loads() and dumps() interface, only string keys """
    @staticmethod
    def dumps(obj):
        return json.dumps(obj).encode()
    @staticmethod
    def loads(data):
        loaded = json.loads(bytes(data).decode())
        # JSON turns tuples into lists, UltraDict expects a tuple for stream records
        return tuple(loaded) if isinstance(loaded, list) else loaded

class Custom:
    def __init__(self, v):
        self.v = v
    def __eq__(self, other):
        return isinstance(other, Custom) and other.v == self.v
    def __repr__(self):
        return f'Custom({self.v!r})'

def child_write(d, key, value):
    d[key] = value

def child_write_by_name(name, key, value):
    d = UltraDict(name=name)
    d[key] = value

def child_shared_counter(name, count):
    d = UltraDict(name=name, shared_lock=True)
    for _ in range(count):
        with d.lock:
            d['counter'] += 1

def main():
    # pylint: disable=too-many-statements, unnecessary-lambda, unnecessary-dunder-call

    section('Class attributes')
    step('Exceptions', lambda: sorted(n for n in dir(UltraDict.Exceptions) if not n.startswith('_') and n[0].isupper()))
    step('log', lambda: hasattr(UltraDict, 'log'))
    step('SharedLock', lambda: UltraDict.SharedLock.__name__)
    step('RLock', lambda: UltraDict.RLock.__name__)
    step('isinstance', lambda: [isinstance(UltraDict(), t) for t in (dict, collections.UserDict, collections.abc.Mapping, collections.abc.MutableMapping)])

    section('Constructor')
    step('empty', lambda: UltraDict())
    step('mapping', lambda: UltraDict({1: 1, 'a': [1, 2]}))
    step('pairs', lambda: UltraDict([(1, 2), (3, 4)]))
    step('kwargs', lambda: UltraDict(a=1, b='x'))
    step('mapping and kwargs', lambda: UltraDict({1: 1}, some_key='some_value'))
    step('name type', lambda: type(UltraDict().name).__name__)

    name = new_name()
    u = UltraDict({'init': 1}, name=name)
    step('named name', lambda: u.name == name)
    step('attach by name', lambda: UltraDict(name=name))
    step('attach with initial data', lambda: UltraDict({'more': 2}, name=name))
    step('creator sees initial data of attacher', lambda: u)
    step('create=True existing', lambda: UltraDict(name=name, create=True))
    step('create=False existing', lambda: UltraDict(name=name, create=False))
    missing = new_name()
    step('create=False missing', lambda: UltraDict(name=missing, create=False))
    created = new_name()
    step('create=True new', lambda: UltraDict({'x': 1}, name=created, create=True))
    cleanup_name(created)
    u.unlink()

    section('buffer_size')
    small = UltraDict(buffer_size=200)
    step('small buffer_size', lambda: small.buffer_size)
    for i in range(50):
        small[i] = i * 2
    other = UltraDict(name=small.name)
    step('small buffer full dumps happened', lambda: small.full_dump_counter > 1)
    step('small buffer other sees all', lambda: dict(other.items()) == {i: i * 2 for i in range(50)})
    step('small buffer value bigger than buffer', lambda: (small.__setitem__('big', 'x' * 1000), len(other['big']))[1])
    step('buffer_size of attacher', lambda: other.buffer_size)
    big = UltraDict(buffer_size=1_000_000)
    step('big buffer_size', lambda: big.buffer_size)

    section('serializer')
    m = UltraDict(serializer=marshal)
    m['a'] = [1, 2, (3, 4)]
    m[5] = {'x': b'y'}
    mo = UltraDict(name=m.name, serializer=marshal)
    step('marshal serializer', lambda: dict(mo.items()))
    m.dump()
    step('marshal serializer after dump', lambda: dict(UltraDict(name=m.name, serializer=marshal).items()))
    j = UltraDict(serializer=JsonSerializer)
    j['a'] = [1, 2]
    j['b'] = {'c': None}
    jo = UltraDict(name=j.name, serializer=JsonSerializer)
    step('custom serializer', lambda: dict(jo.items()))
    step('custom serializer delete', lambda: j.__delitem__('a'))
    step('custom serializer after delete', lambda: dict(jo.items()))
    j.dump()
    step('custom serializer after dump', lambda: dict(UltraDict(name=j.name, serializer=JsonSerializer).items()))
    p = UltraDict(serializer=pickle)
    p['c'] = Custom(1)
    step('explicit pickle serializer', lambda: UltraDict(name=p.name)['c'])

    section('Values and keys')
    v = UltraDict()
    vo = UltraDict(name=v.name)
    values = [0, -1, 2**70, 1.5, float('inf'), 'str', 'ä€😀', '\udcff', b'\x00\xff', bytearray(b'ba'), None, True, False,
              (1, 'a'), [1, [2]], {'n': {'m': 1}}, {1, 2}, frozenset({3}), Custom(5), 1+2j, range(3)]
    for i, value in enumerate(values):
        v[i] = value
    step('values roundtrip', lambda: [vo[i] for i in range(len(values))])
    step('value types', lambda: [type(vo[i]).__name__ for i in range(len(values))])
    keys = [0, 1.5, 'k', b'k', None, (1, 2), frozenset({1}), Custom.__name__, -5, 2**65]
    for key in keys:
        v[key] = 'value'
    step('keys roundtrip', lambda: [key in vo for key in keys])
    step('key order', lambda: list(vo.keys()))
    v[1] = 'one'
    v[True] = 'true'
    step('True and 1 same key', lambda: (vo[1], [type(k).__name__ for k in vo.keys() if k == 1]))
    v[1.0] = 'float one'
    step('1.0 same key', lambda: vo[1])
    step('unhashable get', lambda: vo[[1]])
    step('unhashable set', lambda: v.__setitem__([1], 1))
    step('unhashable contains', lambda: [1] in vo)
    step('unpicklable value', lambda: v.__setitem__('f', lambda: 1))
    step('unpicklable value not stored', lambda: 'f' in v)
    step('unpicklable value not stored remote', lambda: 'f' in vo)

    section('Mapping API')
    d = UltraDict({'a': 1, 'b': 2})
    o = UltraDict(name=d.name)
    step('getitem', lambda: o['a'])
    step('getitem missing', lambda: o['zzz'])
    step('getitem missing message', lambda: (lambda: o['zzz']) and str(next(iter([e for e in [_catch(lambda: o['zzz'])]]))))
    step('get', lambda: o.get('a'))
    step('get missing', lambda: o.get('zzz'))
    step('get default', lambda: o.get('zzz', 5))
    step('get default kwarg', lambda: o.get('zzz', default=6))
    step('contains', lambda: ('a' in o, 'zzz' in o))
    step('len', lambda: len(o))
    step('bool', lambda: (bool(o), bool(UltraDict())))
    step('iter', lambda: list(iter(o)))
    step('list', lambda: list(o))
    step('keys', lambda: o.keys())
    step('values', lambda: o.values())
    step('items', lambda: o.items())
    step('keys type', lambda: type(o.keys()).__name__)
    step('items equal', lambda: d.items() == o.items())
    step('reversed', lambda: list(reversed(o)))
    step('sorted', lambda: sorted(o))
    step('dict()', lambda: dict(o))
    step('json.dumps', lambda: json.dumps(o))
    step('repr', lambda: repr(o))
    step('str', lambda: str(o))
    step('format', lambda: f'{o}')
    step('eq dict', lambda: o == {'a': 1, 'b': 2})
    step('eq UltraDict', lambda: o == d)
    step('ne', lambda: o != {'a': 1})
    step('eq non mapping', lambda: o == 5)
    step('hash', lambda: hash(o))
    step('setitem', lambda: (d.__setitem__('c', 3), o['c'])[1])
    step('setitem overwrite', lambda: (d.__setitem__('c', 4), o['c'])[1])
    step('delitem', lambda: (d.__delitem__('c'), 'c' in o)[1])
    step('delitem missing', lambda: d.__delitem__('zzz'))
    step('del statement remote', lambda: (o.__delitem__('b'), dict(d.items()))[1])
    step('pop', lambda: (d.__setitem__('p', 9), o.pop('p'), 'p' in d)[1:])
    step('pop missing', lambda: o.pop('zzz'))
    step('pop default', lambda: o.pop('zzz', 'dflt'))
    step('setdefault existing', lambda: o.setdefault('a', 100))
    step('setdefault new', lambda: (o.setdefault('s', 7), d['s'])[1])
    step('setdefault None', lambda: (o.setdefault('n'), 'n' in d)[1])
    step('popitem', lambda: (d.popitem(), dict(o.items()))[1])
    step('update mapping', lambda: (d.update({'u1': 1}), o['u1'])[1])
    step('update pairs', lambda: (d.update([('u2', 2)]), o['u2'])[1])
    step('update kwargs', lambda: (d.update(u3=3), o['u3'])[1])
    step('update mapping and kwargs', lambda: (d.update({'u4': 4}, u5=5), o['u4'], o['u5'])[1:])
    step('update UltraDict', lambda: (d.update(UltraDict({'u6': 6})), o['u6'])[1])
    step('update nothing', lambda: d.update())
    step('update bad', lambda: d.update(5))
    step('copy', lambda: fmt(d.copy()))
    step('copy type', lambda: type(d.copy()).__name__)
    step('copy.copy', lambda: fmt(copy.copy(d)))
    step('copy.deepcopy', lambda: fmt(copy.deepcopy(d)))
    step('or dict', lambda: fmt(d | {'or': 1}))
    step('or type', lambda: type(d | {'or': 1}).__name__)
    step('ror dict', lambda: fmt({'ror': 1} | d))
    step('ior', lambda: (_ior(d, {'ior': 1}), 'ior' in d, 'ior' in o)[1:])
    step('fromkeys', lambda: fmt(UltraDict.fromkeys(['x', 'y'], 0)))
    step('fromkeys type', lambda: type(UltraDict.fromkeys(['x'])).__name__)
    step('clear', lambda: (o.clear(), len(d), dict(d.items()))[1:])
    step('popitem empty', lambda: d.popitem())
    step('data attribute', lambda: (d.__setitem__('z', 1), d.data, type(d.data).__name__)[1:])
    step('data of other before access', lambda: dict(o.data))
    step('data of other after access', lambda: (len(o), dict(o.data))[1])

    section('Pickle and copy across instances')
    pk = UltraDict({'p': 1})
    step('pickle roundtrip', lambda: fmt(pickle.loads(pickle.dumps(pk))))
    step('pickle roundtrip same name', lambda: pickle.loads(pickle.dumps(pk)).name == pk.name)
    step('reduce', lambda: type(pk.__reduce__()).__name__)

    section('Control methods')
    c = UltraDict({'a': 1})
    co = UltraDict(name=c.name)
    step('dump returns', lambda: c.dump())
    step('full_dump_counter after dump', lambda: c.full_dump_counter)
    step('update_stream_position after dump', lambda: c.update_stream_position)
    step('other loads dump', lambda: (co['a'], co.full_dump_counter))
    step('load without new data', lambda: co.load())
    step('load force', lambda: (co.load(force=True), co.full_dump_counter)[1])
    step('apply_update', lambda: co.apply_update())
    step('append_update', lambda: (c.append_update('appended', 5), co['appended'])[1])
    step('append_update local data unchanged', lambda: 'appended' in c.data)
    step('append_update delete', lambda: (c.append_update('appended', b'', delete=True), 'appended' in co)[1])
    step('status keys', lambda: sorted(c.status().keys()))
    step('status values', lambda: {k: c.status()[k] for k in ('update_stream_position', 'update_stream_position_remote', 'full_dump_counter', 'full_dump_counter_remote', 'lock_pid_remote', 'lock_remote', 'shared_lock_remote', 'recurse_remote', 'buffer_size', 'recurse', 'closed', 'auto_unlink', 'full_dump_size', 'shared_lock')})
    out = io.StringIO()
    step('print_status', lambda: (_redirect(out, c.print_status), out.getvalue().count('\n') > 5)[1])
    step('print_status stderr', lambda: (_redirect_err(io.StringIO(), lambda: c.print_status(stderr=True)), True)[1])
    step('get_memory attach', lambda: UltraDict.get_memory(create=False, name=c.name))
    step('get_memory missing', lambda: UltraDict.get_memory(create=False, name=new_name()))
    gm = new_name()
    step('get_memory create', lambda: (UltraDict.get_memory(create=True, name=gm, size=100).size >= 100))
    step('get_memory create existing', lambda: UltraDict.get_memory(create=True, name=gm, size=100))
    step('unlink_by_name', lambda: UltraDict.unlink_by_name(gm))
    step('unlink_by_name missing', lambda: UltraDict.unlink_by_name(gm))
    step('unlink_by_name missing ignore', lambda: UltraDict.unlink_by_name(gm, ignore_errors=True))

    section('full_dump_size')
    f = UltraDict(buffer_size=200, full_dump_size=10_000)
    fo = UltraDict(name=f.name)
    for i in range(100):
        f[i] = i
    step('static full dump', lambda: (fo.full_dump_size, dict(fo.items()) == {i: i for i in range(100)}))
    step('static full dump too small', lambda: f.__setitem__('big', 'x' * 20_000))
    tiny = UltraDict(buffer_size=100, full_dump_size=100)
    step('static full dump memory full', lambda: [tiny.__setitem__(i, 'x' * 30) for i in range(10)])

    section('shared_lock')
    s = UltraDict({'a': 1}, shared_lock=True)
    step('shared_lock type', lambda: type(s.lock).__name__)
    step('shared_lock attach inherit', lambda: type(UltraDict(name=s.name).lock).__name__)
    step('shared_lock attach True', lambda: type(UltraDict(name=s.name, shared_lock=True).lock).__name__)
    step('shared_lock attach False', lambda: UltraDict(name=s.name, shared_lock=False))
    r = UltraDict()
    step('rlock attach True', lambda: UltraDict(name=r.name, shared_lock=True))
    step('rlock type', lambda: type(r.lock).__name__)
    step('rlock with', lambda: _with(r.lock, lambda: r.__setitem__('w', 1)))
    step('rlock acquire release', lambda: (r.lock.acquire(), r.lock.release()))
    step('rlock nested', lambda: _with(r.lock, lambda: _with(r.lock, lambda: r.__setitem__('n', 1))))
    lock = s.lock
    step('lock acquire', lambda: lock.acquire())
    step('lock has_lock', lambda: lock.has_lock)
    step('lock acquire nested', lambda: (lock.acquire(), lock.has_lock))
    step('lock remote state', lambda: (lock.get_remote_lock(), lock.get_remote_pid() == os.getpid()))
    step('lock release', lambda: (lock.release(), lock.has_lock))
    step('lock release last', lambda: (lock.release(), lock.has_lock, lock.get_remote_lock(), lock.get_remote_pid()))
    step('lock release not held', lambda: lock.release())
    step('lock with', lambda: _with(lock, lambda: (lock.has_lock, s.__setitem__('w', 1))))
    step('lock with call', lambda: _with(lock(timeout=1, sleep_time=0), lambda: lock.has_lock))
    step('lock call returns', lambda: lock(block=False) is lock)
    step('lock next_acquire_parameters', lambda: (lock.reset_acquire_parameters(), lock.next_acquire_parameters)[1])
    step('lock status', lambda: {k: v for k, v in lock.status().items() if k != 'pid'})
    step('lock print_status', lambda: _redirect(io.StringIO(), lock.print_status))
    step('lock repr', lambda: repr(lock).split(' @')[0])
    step('lock test_and_inc', lambda: (lock.test_and_inc(), lock.get_remote_lock()))
    step('lock test_and_inc held', lambda: lock.test_and_inc())
    step('lock test_and_dec', lambda: (lock.test_and_dec(), lock.get_remote_lock()))
    step('lock test_and_dec not held', lambda: lock.test_and_dec())
    # Simulate another process that holds the lock
    fake_pid = 2**22 - 3
    def hold_fake():
        lock.lock_remote[0] = 1
        lock.pid_remote[:] = fake_pid.to_bytes(4, 'little')
    def free_fake():
        lock.pid_remote[:] = b'\x00\x00\x00\x00'
        lock.lock_remote[0] = 0
    hold_fake()
    step('acquire non blocking held', lambda: lock.acquire(block=False))
    step('acquire non blocking blocking_pid', lambda: _catch(lambda: lock.acquire(block=False)).blocking_pid == fake_pid)
    step('acquire timeout', lambda: lock.acquire(timeout=0.05))
    step('acquire timeout type', lambda: type(_catch(lambda: lock.acquire(timeout=0.05))).__name__)
    step('with timeout', lambda: _with(lock(timeout=0.05), lambda: 1))
    step('setitem while held by other non blocking', lambda: _with(lock(block=False), lambda: s.__setitem__('x', 1)))
    step('steal wrong pid', lambda: lock.steal(from_pid=12345))
    step('steal_from_dead', lambda: (lock.steal_from_dead(from_pid=fake_pid), lock.has_lock, lock.get_remote_pid() == os.getpid()))
    step('release after steal', lambda: (lock.release(), lock.get_remote_lock(), lock.get_remote_pid()))
    step('steal not locked', lambda: lock.steal(from_pid=fake_pid))
    hold_fake()
    step('steal_after_timeout', lambda: (lock.acquire(timeout=0.05, steal_after_timeout=True), lock.has_lock, lock.get_remote_pid() == os.getpid()))
    step('release after steal_after_timeout', lambda: (lock.release(), lock.get_remote_lock()))
    hold_fake()
    step('with steal_after_timeout', lambda: _with(lock(timeout=0.05, steal_after_timeout=True), lambda: (s.__setitem__('stolen', 1), lock.has_lock)[1]))
    step('after with steal', lambda: (lock.has_lock, lock.get_remote_lock(), s['stolen']))
    hold_fake()
    step('steal with release', lambda: (lock.steal(from_pid=fake_pid, release=True), lock.has_lock, lock.get_remote_lock()))
    step('steal while holding', lambda: (lock.acquire(), _catch(lambda: lock.steal(from_pid=1)).__class__.__name__, lock.release())[1])
    step('steal_from_dead alive', lambda: (hold_fake(), lock.pid_remote.__setitem__(slice(None), os.getppid().to_bytes(4, 'little')), lock.steal_from_dead(from_pid=os.getppid()))[2])
    free_fake()
    step('reset', lambda: lock.reset())
    step('after reset', lambda: (lock.has_lock, lock.get_remote_lock(), lock.get_remote_pid()))
    step('shared lock still works', lambda: (s.__setitem__('after', 1), UltraDict(name=s.name)['after'])[1])
    step('stolen lock release', lambda: (lock.acquire(), lock.pid_remote.__setitem__(slice(None), fake_pid.to_bytes(4, 'little')), _catch(lambda: lock.release()).__class__.__name__)[2])
    lock.has_lock = 0
    free_fake()
    step('shared lock works after stolen', lambda: (s.__setitem__('again', 1), s['again'])[1])

    section('recurse')
    rc = UltraDict(recurse=True)
    rc['nested'] = {'counter': 0, 'deeper': {'x': 1}}
    rco = UltraDict(name=rc.name)
    step('nested type', lambda: type(rc['nested']).__name__)
    step('deeper type', lambda: type(rc['nested']['deeper']).__name__)
    step('attach recurse inherit', lambda: rco.recurse)
    step('nested other type', lambda: type(rco['nested']).__name__)
    rco['nested']['counter'] += 1
    step('nested update visible', lambda: rc['nested']['counter'])
    rc['nested']['deeper']['x'] = 2
    step('deeper update visible', lambda: rco['nested']['deeper']['x'])
    step('nested repr', lambda: repr(rco))
    step('recurse register', lambda: type(rc.recurse_register).__name__)
    step('recurse register size', lambda: len(rc.recurse_register))
    step('recurse attach False', lambda: UltraDict(name=rc.name, recurse=False))
    step('non recurse attach True', lambda: UltraDict(name=r.name, recurse=True))
    step('recurse non pickle serializer', lambda: UltraDict(recurse=True, serializer=marshal))
    step('recurse_register str', lambda: type(UltraDict(recurse=True, recurse_register=rc.recurse_register.name).recurse_register).__name__)
    step('recurse_register instance', lambda: UltraDict(recurse=True, recurse_register=rc.recurse_register).recurse_register is rc.recurse_register)
    step('recurse_register bad type', lambda: UltraDict(recurse=True, recurse_register=5))
    step('recurse non dict values', lambda: (rc.__setitem__('list', [1, {'a': 1}]), type(rco['list'][1]).__name__)[1])
    step('recurse shared_lock', lambda: (lambda x: (x.__setitem__('n', {'a': 1}), type(x['n'].lock).__name__)[1])(UltraDict(recurse=True, shared_lock=True)))
    step('recurse kwargs init', lambda: type(UltraDict({'a': {'b': 1}}, recurse=True)['a']).__name__)
    child_names = list(rc.recurse_register.keys())
    import gc; gc.collect()
    step('recurse children alive before creator close', lambda: [_exists(n) for n in child_names])
    step('recurse register alive before creator close', lambda: _exists(rc.recurse_register.name))
    step('recurse nested unlink', lambda: (rc['nested']['deeper'].unlink(), _exists(rc.recurse_register.name), _exists(rc['nested'].name))[1:])
    step('recurse attacher close', lambda: rco.close())
    step('recurse creator close', lambda: rc.close())
    step('recurse children unlinked', lambda: [UltraDict.unlink_by_name(n, ignore_errors=True) for n in child_names])

    section('auto_unlink and close')
    a = UltraDict({'a': 1})
    step('auto_unlink creator default', lambda: a.auto_unlink)
    ao = UltraDict(name=a.name)
    step('auto_unlink attacher default', lambda: ao.auto_unlink)
    step('close returns data', lambda: a.close())
    step('close twice', lambda: a.close())
    step('closed', lambda: a.closed)
    step('attach after creator closed', lambda: UltraDict(name=a.name, create=False))
    step('attacher still works', lambda: ao['a'])
    ao.close()
    na = new_name()
    b = UltraDict({'b': 1}, name=na, auto_unlink=False)
    step('auto_unlink False', lambda: b.auto_unlink)
    b.close()
    step('attach after auto_unlink False closed', lambda: UltraDict(name=na))
    UltraDict(name=na).unlink()
    step('attach after unlink', lambda: UltraDict(name=na, create=False))
    t = UltraDict({'t': 1}, auto_unlink=True)
    to = UltraDict(name=t.name, auto_unlink=True)
    to.close()
    step('attacher auto_unlink True close', lambda: UltraDict(name=t.name, create=False)['t'])
    x = UltraDict({'x': 1})
    xo = UltraDict(name=x.name)
    xo.unlink()
    step('unlink by attacher', lambda: UltraDict(name=x.name, create=False))
    step('after close getitem', lambda: xo['x'])
    step('after close get', lambda: xo.get('x'))
    step('after close contains', lambda: 'x' in xo)
    step('after close len', lambda: len(xo))
    step('after close iter', lambda: list(xo))
    step('after close keys', lambda: xo.keys())
    step('after close items', lambda: xo.items())
    step('after close repr', lambda: repr(xo))
    step('after close data', lambda: xo.data)
    step('after close setitem', lambda: xo.__setitem__('y', 1))
    step('after close delitem', lambda: xo.__delitem__('x'))
    step('after close dump', lambda: xo.dump())
    step('after close apply_update', lambda: xo.apply_update())
    step('after close status', lambda: sorted(xo.status().keys()))
    sl = UltraDict({'s': 1}, shared_lock=True)
    sl.close()
    step('shared after close setitem', lambda: sl.__setitem__('y', 1))
    step('shared after close getitem', lambda: sl['s'])
    step('del keyword', lambda: _del_check())

    section('Processes')
    ctx_spawn = multiprocessing.get_context('spawn')
    sp = UltraDict({'a': 1})
    proc = ctx_spawn.Process(target=child_write, args=(sp, 'spawn', 'yes'))
    proc.start(); proc.join()
    step('spawn child with pickled UltraDict', lambda: (proc.exitcode, sp.get('spawn')))
    proc = ctx_spawn.Process(target=child_write_by_name, args=(sp.name, 'by_name', 'yes'))
    proc.start(); proc.join()
    step('spawn child by name', lambda: (proc.exitcode, sp.get('by_name')))
    if sys.platform != 'win32':
        ctx_fork = multiprocessing.get_context('fork')
        proc = ctx_fork.Process(target=child_write, args=(sp, 'fork', 'yes'))
        proc.start(); proc.join()
        step('fork child', lambda: (proc.exitcode, sp.get('fork')))
    sc = UltraDict({'counter': 0}, shared_lock=True)
    procs = [ctx_spawn.Process(target=child_shared_counter, args=(sc.name, 500)) for _ in range(3)]
    for pr in procs: pr.start()
    for pr in procs: pr.join()
    step('spawn shared lock counter', lambda: ([pr.exitcode for pr in procs], sc['counter']))

def _exists(name):
    try:
        UltraDict.get_memory(create=False, name=name).close()
        return True
    except UltraDict.Exceptions.CannotAttachSharedMemory:
        return False

def _catch(fn):
    try:
        fn()
    except Exception as e: # pylint: disable=broad-except
        return e
    return None

def _with(cm, fn):
    with cm:
        return fn()

def _ior(d, other):
    d |= other
    return d

def _redirect(out, fn):
    with contextlib.redirect_stdout(out):
        return fn()

def _redirect_err(out, fn):
    with contextlib.redirect_stderr(out):
        return fn()

def _del_check():
    tmp = UltraDict({'d': 1})
    name = tmp.name
    del tmp
    return UltraDict.unlink_by_name(name, ignore_errors=True)

if __name__ == '__main__':
    main()
