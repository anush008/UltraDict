import unittest
import subprocess
import sys
import os
import time

sys.path.insert(0, '..')
from UltraDict import UltraDict

# Disable logging
if hasattr(UltraDict.log, 'disable'):
    UltraDict.log.disable(UltraDict.log.CRITICAL)
else:
    UltraDict.log.set_level(UltraDict.log.Levels.error)

class UltraDictTests(unittest.TestCase):

    def setUp(self):
        pass

    def exec(self, filepath):
        ret = subprocess.run([sys.executable, filepath],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        #print(ret.stdout.decode())
        ret.stdout = ret.stdout.replace(b'\r\n', b'\n');
        self.assertEqual(ret.returncode, 0, f"Running '{filepath}' returned exit code '{ret.returncode}' but expected exit code is '0'")
        return ret

    def exec_show_output(self, ret):
        return f"\n\n{ret}\n\nOutput:\n{ret.stdout.decode()}\n"

    def assertReturnCode(self, ret, target=0):
        return self.assertEqual(ret.returncode, target, self.exec_show_output(ret))

    def test_count(self):
        ultra = UltraDict()
        other = UltraDict(name=ultra.name)

        count = 100
        for i in range(count//2):
            ultra[i] = i

        for i in range(count//2, count):
            other[i] = i

        self.assertEqual(len(ultra), len(other))

    def test_huge_value(self):
        ultra = UltraDict()

        # One megabyte string
        self.assertEqual(ultra.full_dump_counter, 0)

        length = 1_000_000

        ultra['huge'] = ' ' * length

        self.assertEqual(ultra.full_dump_counter, 1)
        self.assertEqual(len(ultra.data['huge']), length)

        other = UltraDict(name=ultra.name)

        self.assertEqual(len(other.data['huge']), length)

    def test_parameter_passing(self):
        ultra = UltraDict(shared_lock=True, buffer_size=4096*8, full_dump_size=4096*8)
        # Connect `other` dict to `ultra` dict via `name`
        other = UltraDict(name=ultra.name)

        self.assertIsInstance(ultra.lock, ultra.SharedLock)
        self.assertIsInstance(other.lock, other.SharedLock)

        self.assertEqual(ultra.buffer_size, other.buffer_size)

    def test_iter(self):
        ultra = UltraDict()
        # Connect `other` dict to `ultra` dict via `name`
        other = UltraDict(name=ultra.name)

        ultra[1] = 1
        ultra[2] = 2

        counter = 0
        for i in other.items():
            counter += 1

        self.assertEqual(counter, 2)

        self.assertEqual(ultra.items(), other.items())

    def test_delete(self):
        import random
        import string
        letters = string.ascii_lowercase
        rand_str =   ''.join(random.choice(letters) for i in range(1000))
        my_dict = UltraDict(buffer_size=10_000_000)
        for i in range(100_000):
            my_dict[i] = rand_str
        for i in list(my_dict.keys()):
            del my_dict[i]
        self.assertEqual(len(my_dict), 0)

    def test_already_exists(self):
        name = 'ultra_test'
        # Ensure we have a clean state before the test
        UltraDict.unlink_by_name(name, ignore_errors=True)
        UltraDict.unlink_by_name(name + '_memory', ignore_errors=True)
        # Create should be possible now
        u1 = UltraDict(name=name, create=True)
        with self.assertRaises(UltraDict.Exceptions.AlreadyExists):
            u2 = UltraDict(name=name, create=True)

    def test_not_already_exists(self):
        name = 'ultra_test'
        # Ensure we have a clean state before the test
        UltraDict.unlink_by_name(name, ignore_errors=True)
        UltraDict.unlink_by_name(name + '_memory', ignore_errors=True)

        with self.assertRaises(UltraDict.Exceptions.CannotAttachSharedMemory):
            ultra = UltraDict(name=name, create=False)

    def test_simple_and_complex_values(self):
        # Simple keys and values use marshal records, all others use pickle records
        ultra = UltraDict()
        other = UltraDict(name=ultra.name)

        values = {
            1: 1, 'int': -2**40, 'float': 1.5, 'str': 'ä', 'bytes': b'\x00\xff', 'none': None,
            'bool': True, 1.25: 'float key', b'k': 'bytes key',
            ('tuple', 'key'): [1, {'nested': (2, 3)}], 'set': {1, 2}, 'big': 2**100,
        }
        for key, value in values.items():
            ultra[key] = value

        self.assertEqual(dict(other.items()), values)
        for key, value in values.items():
            self.assertEqual(other[key], value)
            self.assertIs(type(other[key]), type(value))

        # True and 1 are the same dict key, the value must keep its type
        other[1] = True
        self.assertIs(ultra[1], True)

        del other[('tuple', 'key')]
        del other['str']
        self.assertNotIn(('tuple', 'key'), ultra)
        self.assertNotIn('str', ultra)
        self.assertEqual(len(ultra), len(values) - 2)

    def test_simple_value_encoding(self):
        # Edge cases of the direct marshal encoding of simple types, as values and as keys
        import math
        values = [
            0, 1, -1, 2**31 - 1, -2**31, 2**31, -2**31 - 1, 2**63, -2**63 - 1, 2**200,
            0.0, -0.0, 1.5, -1e308, 5e-324, float('inf'), float('-inf'), float('nan'),
            '', 'a', 'x' * 255, 'x' * 256, 'x' * 100_000, 'ä', '€' * 300, '😀', '\udcff', 'a\x00b',
            b'', b'\x00', b'x' * 300, True, False, None,
        ]
        by_index = UltraDict(buffer_size=1_000_000)
        by_index_other = UltraDict(name=by_index.name)
        for i, value in enumerate(values):
            by_index[i] = value
        for i, value in enumerate(values):
            got = by_index_other[i]
            self.assertIs(type(got), type(value), (i, value))
            if isinstance(value, float) and math.isnan(value):
                self.assertTrue(math.isnan(got))
            else:
                self.assertEqual(got, value)
                if isinstance(value, float):
                    self.assertEqual(math.copysign(1, got), math.copysign(1, value))

        # Keys that are equal in a dict (0, 0.0, -0.0, False) share one entry, NaN never matches
        keys = [value for value in values if not (isinstance(value, float) and math.isnan(value))]
        by_key = UltraDict(buffer_size=1_000_000)
        by_key_other = UltraDict(name=by_key.name)
        expected = {}
        for i, key in enumerate(keys):
            by_key[key] = i
            expected[key] = i
        self.assertEqual(dict(by_key_other.items()), expected)
        self.assertEqual([type(k) for k in by_key_other.keys()], [type(k) for k in expected.keys()])
        for key in keys:
            if key in expected:
                del by_key[key]
                del expected[key]
                self.assertNotIn(key, by_key_other)
        self.assertEqual(len(by_key_other), 0)

    def test_record_visible_later(self):
        # On CPUs with weak memory order, a reader can see the new stream position before the
        # record. Simulate it: publish the position first and write the record a moment later.
        import threading, pickle, zlib
        ultra = UltraDict(buffer_size=10_000)
        other = UltraDict(name=ultra.name)
        ultra['before'] = 1
        self.assertEqual(other['before'], 1)

        module = sys.modules['UltraDict.UltraDict']
        body = pickle.dumps((True, 'late', 2))
        start = ultra.update_stream_position_remote[0]
        end = start + module.HEADER_SIZE + len(body)
        ultra.buffer.buf[start:end] = bytes(end - start)
        ultra.update_stream_position = end
        ultra.update_stream_position_remote[0] = end

        def write_record():
            time.sleep(0.02)
            # The body first, then the header, like a writer that reorders its stores
            ultra.buffer.buf[start + module.HEADER_SIZE:end] = body
            checksum = zlib.crc32(body, ultra.full_dump_counter)
            module.HEADER.pack_into(ultra.buffer.buf, start, 0xFF, len(body), checksum, 0xFF)

        writer = threading.Thread(target=write_record)
        writer.start()
        try:
            self.assertEqual(other['late'], 2)
        finally:
            writer.join()

    def test_full_dump_visible_later(self):
        # On CPUs with weak memory order, a reader can see the new full dump counter before the
        # name of the new full dump. Simulate it: publish the counter first and the name later.
        import threading, pickle, zlib
        module = sys.modules['UltraDict.UltraDict']
        ultra = UltraDict({'a': 1})
        other = UltraDict(name=ultra.name)
        ultra.dump()
        self.assertEqual(other['a'], 1)

        new_counter = ultra.full_dump_counter_remote[0] + 1
        body = pickle.dumps({'a': 2})
        memory = UltraDict.get_memory(create=True, size=module.FULL_DUMP_HEADER_SIZE + len(body))
        memory.buf[module.FULL_DUMP_HEADER_SIZE:module.FULL_DUMP_HEADER_SIZE + len(body)] = body
        module.FULL_DUMP_HEADER.pack_into(memory.buf, 0, 0xFF, len(body), zlib.crc32(body, new_counter), 0xFF)
        ultra.update_stream_position = 0
        ultra.update_stream_position_remote[0] = 0
        ultra.full_dump_counter = new_counter
        ultra.full_dump_counter_remote[0] = new_counter

        def publish_name():
            time.sleep(0.02)
            ultra.full_dump_memory_name_remote[:] = memory.name.encode('utf-8').ljust(255)

        writer = threading.Thread(target=publish_name)
        writer.start()
        try:
            self.assertEqual(other['a'], 2)
        finally:
            writer.join()
            memory.close()

    def test_append_update_after_remote_full_dump(self):
        # A writer that has not loaded the last full dump of another process yet
        ultra = UltraDict(buffer_size=1000)
        other = UltraDict(name=ultra.name)
        other.dump()
        ultra.append_update('k', 1)
        self.assertEqual(other['k'], 1)
        self.assertEqual(UltraDict(name=ultra.name)['k'], 1)

    def test_repeated_delete_is_harmless(self):
        # Two threads that read through the same UltraDict can apply the same delete twice
        ultra = UltraDict()
        other = UltraDict(name=ultra.name)
        ultra['a'] = 1
        self.assertEqual(other['a'], 1)
        position = other.update_stream_position
        del ultra['a']
        self.assertNotIn('a', other)
        # Replay the delete again, like a second thread that started at the same position
        other.update_stream_position = position
        other.apply_update()
        self.assertNotIn('a', other)

    def test_shared_lock_is_freed(self):
        # The fork handler of a SharedLock must not keep it alive
        import gc, weakref
        ultra = UltraDict(shared_lock=True)
        lock = weakref.ref(ultra.lock)
        ultra.close()
        del ultra
        gc.collect()
        self.assertIsNone(lock())

    def run_crash_case(self, case):
        ret = subprocess.run([sys.executable, 'tests/crash_cases.py', case],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = ret.stdout.decode(errors='replace')
        self.assertEqual(ret.returncode, 0, output)
        self.assertIn('ok:', output, output)

    def test_close_during_replay(self):
        self.run_crash_case('close_during_replay')

    def test_bad_record_length(self):
        self.run_crash_case('bad_record_length')

    def test_exchange_after_close(self):
        self.run_crash_case('exchange_after_close')

    def test_threads(self):
        # The default lock also protects against other threads in the same process
        import threading
        ultra = UltraDict({'counter': 0}, buffer_size=2000)
        other = UltraDict(name=ultra.name)
        errors = []

        def increment():
            try:
                for _ in range(2000):
                    with ultra.lock:
                        ultra['counter'] += 1
            except Exception as e: # pylint: disable=broad-except
                errors.append(e)

        def write(thread_id):
            try:
                for i in range(2000):
                    ultra[f't{thread_id}'] = i
                    ultra[('complex', thread_id)] = [i]
            except Exception as e: # pylint: disable=broad-except
                errors.append(e)

        def read():
            try:
                for _ in range(4000):
                    other.get('t0')
                    len(other)
            except Exception as e: # pylint: disable=broad-except
                errors.append(e)

        threads = [threading.Thread(target=increment) for _ in range(4)]
        threads += [threading.Thread(target=write, args=(i,)) for i in range(2)]
        threads += [threading.Thread(target=read)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(other['counter'], 8000)
        self.assertEqual(other['t0'], 1999)
        self.assertEqual(other[('complex', 1)], [1999])

    def test_shared_lock_reset(self):
        ultra = UltraDict(shared_lock=True)
        lock = ultra.lock
        lock.acquire()
        lock.reset()
        self.assertEqual((lock.has_lock, lock.get_remote_lock(), lock.get_remote_pid()), (0, 0, 0))
        # The lock works again after a reset
        with lock:
            ultra['a'] = 1
        self.assertEqual(lock.get_remote_lock(), 0)

    def test_custom_serializer_delete(self):
        # A serializer that cannot write bytes, delete records must work anyway
        import json
        class JsonSerializer:
            @staticmethod
            def dumps(obj):
                return json.dumps(obj).encode()
            @staticmethod
            def loads(data):
                # Stream records are tuples, JSON turns them into lists. Full dumps are dicts.
                loaded = json.loads(data)
                return tuple(loaded) if isinstance(loaded, list) else loaded
        ultra = UltraDict(serializer=JsonSerializer)
        other = UltraDict(name=ultra.name, serializer=JsonSerializer)
        ultra['a'] = 1
        ultra['b'] = 2
        del ultra['a']
        self.assertEqual(dict(other.items()), {'b': 2})
        ultra.dump()
        self.assertEqual(dict(UltraDict(name=ultra.name, serializer=JsonSerializer).items()), {'b': 2})

    def test_recurse_register_owner(self):
        import gc
        def exists(name):
            try:
                UltraDict.get_memory(create=False, name=name).close()
                return True
            except UltraDict.Exceptions.CannotAttachSharedMemory:
                return False

        owner = UltraDict(recurse=True)
        owner['nested'] = {'deeper': {'x': 1}}
        register = owner.recurse_register.name
        children = list(owner.recurse_register.keys())
        self.assertEqual(len(children), 2)

        # An UltraDict that only uses the register of `owner` must not unlink the tree
        for shared in (UltraDict(recurse=True, recurse_register=register),
                       UltraDict(recurse=True, recurse_register=owner.recurse_register)):
            shared['own'] = {'y': 1}
            shared.close()
        gc.collect()
        self.assertTrue(exists(register))
        self.assertTrue(all(exists(name) for name in children))

        # A nested UltraDict only unlinks itself
        deeper = owner['nested']['deeper']
        deeper_name = deeper.name
        deeper.unlink()
        self.assertFalse(exists(deeper_name))
        self.assertTrue(exists(register))
        self.assertTrue(exists(owner['nested'].name))

        # The owner unlinks the whole tree, even if a nested UltraDict is gone already
        owner.close()
        self.assertFalse(exists(register))
        self.assertFalse(any(exists(name) for name in children))

    def test_get_and_contains(self):
        ultra = UltraDict()
        other = UltraDict(name=ultra.name)
        ultra['a'] = 1
        self.assertEqual(other.get('a'), 1)
        self.assertEqual(other.get('b', 2), 2)
        self.assertIsNone(other.get('b'))
        self.assertIn('a', other)
        self.assertNotIn('b', other)

    def test_unserializable_value(self):
        # A change that cannot be serialized changes nothing, neither locally nor in other processes
        ultra = UltraDict()
        ultra['a'] = 1
        with self.assertRaises(Exception):
            ultra['b'] = lambda: None
        self.assertNotIn('b', ultra)
        self.assertEqual(UltraDict(name=ultra.name).data, {'a': 1})
        with self.assertRaises(Exception):
            ultra['a'] = lambda: None
        self.assertEqual(ultra['a'], 1)
        # Errors of the local change come first, like in the original implementation
        with self.assertRaises(TypeError):
            ultra[[1]] = lambda: None
        with self.assertRaises(KeyError):
            del ultra[lambda: None]

    def test_access_after_close(self):
        ultra = UltraDict()
        ultra['a'] = 1
        ultra.close()
        with self.assertRaises(UltraDict.Exceptions.AlreadyClosed):
            ultra['a']
        with self.assertRaises(UltraDict.Exceptions.AlreadyClosed):
            ultra.get('a')
        with self.assertRaises(UltraDict.Exceptions.AlreadyClosed):
            'a' in ultra
        # Same as the original implementation: writes fail because the lock is gone
        with self.assertRaises(AttributeError):
            ultra['a'] = 2
        with self.assertRaises(AttributeError):
            del ultra['a']
        with self.assertRaises(AttributeError):
            ultra.data

    @unittest.skipIf(UltraDict.accelerated, "the C extension does not use the atomics package for locking")
    def test_shared_lock_public_api_fallback(self):
        # The shared lock must also work with the public API of the atomics package
        ultra = UltraDict(shared_lock=True)
        lock = ultra.lock
        lock.exchange = lock.exchange_public_api
        for i in range(100):
            with lock:
                ultra[i] = i
        self.assertEqual(len(ultra), 100)
        self.assertEqual(lock.get_remote_lock(), 0)
        self.assertEqual(lock.get_remote_pid(), 0)

    def test_shared_lock_release(self):
        ultra = UltraDict(shared_lock=True)
        lock = ultra.lock
        with lock:
            self.assertEqual(lock.get_remote_lock(), 1)
            self.assertEqual(lock.get_remote_pid(), lock.pid)
            with lock:
                ultra['a'] = 1
            self.assertEqual(lock.get_remote_lock(), 1)
        self.assertEqual(lock.get_remote_lock(), 0)
        self.assertEqual(lock.get_remote_pid(), 0)
        with self.assertRaises(UltraDict.Exceptions.CannotAcquireLock):
            lock.lock_remote[0] = 1
            try:
                lock.acquire(block=False)
            finally:
                lock.lock_remote[0] = 0

    def test_full_dump_resets_stream(self):
        ultra = UltraDict(buffer_size=1000)
        other = UltraDict(name=ultra.name)
        # The real buffer can be bigger, e.g. 16 KB pages on macOS arm64. Each record is at least 10 bytes.
        for i in range(ultra.buffer_size // 10 * 3):
            ultra[i] = i
            self.assertEqual(other[i], i)
        self.assertGreater(ultra.full_dump_counter, 1)
        self.assertEqual(dict(other.items()), dict(ultra.items()))

    @unittest.skipIf(sys.platform == 'win32', "requires fork and dynamic full dumps")
    def test_stress_concurrent_readers(self):
        filename = "tests/stress.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertNotIn(b'FAIL', ret.stdout, self.exec_show_output(ret))

    def modes(self):
        """ Environments for the modes to test: always pure Python, and the C extension if it is built """
        pure = dict(os.environ, ULTRADICT_PURE_PYTHON='1')
        if UltraDict.accelerated:
            accelerated = dict(os.environ)
            accelerated.pop('ULTRADICT_PURE_PYTHON', None)
            return {'pure': pure, 'accelerated': accelerated}
        return {'pure': pure}

    @unittest.skipUnless(sys.platform.startswith("linux"), "expected output was recorded on Linux")
    def test_api_behavior_unchanged(self):
        # tests/api_check_expected.txt is the output of tests/api_check.py with the original
        # implementation of UltraDict, plus the bug fixes listed in readme.md. Every other step
        # of the API must still behave the same.
        # Pickle on Python 3.14 raises PicklingError for a lambda, earlier versions AttributeError.
        # PyPy frees objects only when its garbage collector runs and has a pure Python json encoder.
        if sys.implementation.name == 'pypy':
            expected_file = 'tests/api_check_expected_pypy.txt'
        elif sys.version_info >= (3, 14):
            expected_file = 'tests/api_check_expected_py314.txt'
        else:
            expected_file = 'tests/api_check_expected.txt'
        with open(expected_file, 'rb') as f:
            expected = f.read().splitlines()
        for mode, env in self.modes().items():
            with self.subTest(mode=mode):
                ret = subprocess.run([sys.executable, 'tests/api_check.py'], env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                self.assertEqual(ret.returncode, 0)
                actual = ret.stdout.replace(b'\r\n', b'\n').splitlines()
                diff = [f'{a!r} != {b!r}' for a, b in zip(expected, actual) if a != b]
                self.assertEqual(len(actual), len(expected))
                self.assertEqual(diff, [], '\n'.join(diff))

    def test_mixed_modes(self):
        # A process with the C extension and a process without it share the same UltraDict
        # On Windows, a full dump disappears when the process that created it ends, see readme.md
        full_dump_size = 1_000_000 if sys.platform == 'win32' else None
        ultra = UltraDict({'counter': 0}, shared_lock=True, buffer_size=1000, full_dump_size=full_dump_size)
        for i in range(100):
            ultra[i] = i
            ultra[('t', i)] = [i]
        child = (
            "import sys; sys.path.insert(0, '..')\n"
            "from UltraDict import UltraDict\n"
            f"d = UltraDict(name={ultra.name!r})\n"
            "assert all(d[i] == i and d[('t', i)] == [i] for i in range(100))\n"
            "for i in range(100, 200):\n"
            "    d[i] = i\n"
            "    d[('t', i)] = [i]\n"
            "for i in range(500):\n"
            "    with d.lock:\n"
            "        d['counter'] += 1\n"
            "del d[0]\n"
            "print(UltraDict.accelerated)\n"
        )
        for mode, env in self.modes().items():
            with self.subTest(mode=mode):
                ret = subprocess.run([sys.executable, '-c', child], env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                self.assertEqual(ret.returncode, 0, ret.stdout.decode())
                self.assertEqual(ret.stdout.strip(), str(mode == 'accelerated').encode())
                self.assertEqual({i: ultra[i] for i in range(1, 200)}, {i: i for i in range(1, 200)})
                self.assertEqual([ultra[('t', i)] for i in range(200)], [[i] for i in range(200)])
                self.assertNotIn(0, ultra)
                ultra[0] = 0
        self.assertEqual(ultra['counter'], 500 * len(self.modes()))

    @unittest.skipIf(sys.platform == 'win32', "requires fork and dynamic full dumps")
    def test_stress_concurrent_readers_all_modes(self):
        for mode, env in self.modes().items():
            with self.subTest(mode=mode):
                ret = subprocess.run([sys.executable, 'tests/stress.py'], env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                self.assertEqual(ret.returncode, 0, ret.stdout.decode())
                self.assertNotIn(b'FAIL', ret.stdout, ret.stdout.decode())

    def test_lock_blocking(self):
        pass

    def test_full_dump(self):
        # TODO
        pass

    # Turns out MacOS can only do 24 characters in total
    #def test_longest_name(self):
    #    for i in range(5, 50):
    #        print('Loop ', i)
    #        ultra = UltraDict(name='x' * i)

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux")
    def test_cleanup(self):
        import psutil
        p = psutil.Process()

        # Count only the files that UltraDict opens. Other files can be open already, e.g. stdout
        # redirected to a file, or the Python executable that the atomics package opens on Python 3.14.
        baseline = {f.path for f in p.open_files()}
        def count():
            return len([f for f in p.open_files() if f.path not in baseline])

        self.assertEqual(count(), 0, "file handle count before before tests should be 0")
        # PyPy frees objects only when its garbage collector runs
        import gc
        ultra = UltraDict(nested={ 1: 1})
        self.assertEqual(count(), 4, "file handle count with one simple UltraDict should be 4")
        del ultra
        gc.collect()
        self.assertEqual(count(), 0, "file handle count after deleting the UltraDict should be 0 again")
        ultra = UltraDict(nested={ 1: 1}, recurse=True)
        self.assertEqual(count(), 12, "nested file handle count should be 12")
        del ultra
        gc.collect()
        self.assertEqual(count(), 0, "nested file handle count after deleting UltraDict should be 0 again")

    def test_example_simple(self):
        filename = "examples/simple.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b"Length:  100000  ==  100000  ==  100000", self.exec_show_output(ret))

    def test_example_parallel(self):
        filename = "examples/parallel.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b'Counter:  100000  ==  100000', self.exec_show_output(ret))

    def test_example_nested(self):
        filename = "examples/nested.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b"{'nested': {'deeper': {0: 2}}}  ==  {'nested': {'deeper': {0: 2}}}", self.exec_show_output(ret))

    def test_example_recover_from_stale_lock(self):
        filename = "examples/recover_from_stale_lock.py"
        ret = self.exec(filename)
        self.assertReturnCode(ret)
        self.assertEqual(ret.stdout.splitlines()[-1], b"Counter: 100 == 100", self.exec_show_output(ret))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        for i in range(0, len(sys.argv)):
            if sys.argv[i].startswith('-'):
                continue
            if '.' not in sys.argv[i]:
                sys.argv[i] = f"UltraDictTests.{sys.argv[i]}"
    unittest.main()
