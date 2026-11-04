#
# Stress test for concurrent readers and writers
#
# Three writer processes change simple values (marshal records), complex values
# (pickle records) and delete keys, while a small buffer forces many full dumps.
# A lock-free reader process checks that values never go backwards.
#
# With spawn, the writers alternate between pure Python and the C extension.
#
# Usage: python tests/stress.py, set STRESS_N for more operations per writer
import os, sys, multiprocessing, time
sys.path.insert(0, '..')
from UltraDict import UltraDict

N = int(os.environ.get('STRESS_N', 5_000))

class Box:
    def __init__(self, v): self.v = v
    def __eq__(self, o): return isinstance(o, Box) and o.v == self.v

def writer(name, wid, shared_lock, inherited=None):
    # The default RLock is only shared with children that inherit the object through fork
    d = inherited if inherited is not None else UltraDict(name=name, shared_lock=shared_lock)
    for i in range(N):
        with d.lock:
            d['counter'] += 1
        d[f'w{wid}'] = i                      # marshal record
        d[('t', wid)] = [i, Box(i)]           # pickle record
        if i % 7 == 0:
            d[f'tmp{wid}'] = 1.5
            del d[f'tmp{wid}']

def reader(name, shared_lock, q):
    d = UltraDict(name=name, shared_lock=shared_lock)
    last = {}
    errors = 0
    deadline = time.time() + 120
    while time.time() < deadline:
        for wid in range(3):
            v = d.get(f'w{wid}', -1)
            if v < last.get(wid, -1):
                errors += 1
            last[wid] = v
        if d.get('done'):
            break
    q.put((errors, d['counter']))

def run(ctx_name, shared_lock):
    ctx = multiprocessing.get_context(ctx_name)
    u = UltraDict(buffer_size=4096, shared_lock=shared_lock, full_dump_size=None)
    u['counter'] = 0
    q = ctx.Queue()
    r = ctx.Process(target=reader, args=(u.name, shared_lock, q)); r.start()
    ws = [ctx.Process(target=writer, args=(u.name, w, shared_lock, u if not shared_lock else None)) for w in range(3)]
    # With spawn, each child starts a new interpreter: mix processes with and without the C extension
    pure = os.environ.get('ULTRADICT_PURE_PYTHON')
    for i, w in enumerate(ws):
        if ctx_name == 'spawn' and i % 2 == 0:
            os.environ['ULTRADICT_PURE_PYTHON'] = '1'
        w.start()
        if pure is None:
            os.environ.pop('ULTRADICT_PURE_PYTHON', None)
        else:
            os.environ['ULTRADICT_PURE_PYTHON'] = pure
    for w in ws: w.join()
    u['done'] = True
    errors, reader_counter = q.get(timeout=120); r.join()
    ok = (u['counter'] == 3 * N and errors == 0 and all(u[f'w{w}'] == N - 1 for w in range(3))
          and all(u[('t', w)] == [N - 1, Box(N - 1)] for w in range(3)) and not any(f'tmp{w}' in u for w in range(3)))
    print(f"{ctx_name:5s} shared_lock={shared_lock!s:5s} counter={u['counter']} reader_errors={errors} full_dumps={u.full_dump_counter} -> {'OK' if ok else 'FAIL'}")
    u.unlink()
    return ok

if __name__ == '__main__':
    # Fork is not available on Windows, and dynamic full dump memory is not reliable there
    if sys.platform == 'win32':
        print('Skipped on Windows')
        sys.exit(0)
    results = [run('fork', False), run('spawn', True), run('fork', True)]
    sys.exit(0 if all(results) else 1)
