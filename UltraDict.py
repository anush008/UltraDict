#
# UltraDict
#
# A synchronized, streaming Python dictionary that uses shared memory as a backend
#
# Copyright [2022] [Ronny Rentner] [ultradict.code@ronny-rentner.de]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = ['UltraDict']

import multiprocessing, multiprocessing.shared_memory, multiprocessing.synchronize
import collections, marshal, os, pickle, struct, sys, time, weakref, zlib
import importlib.util, importlib.machinery

try:
    # Needed for the shared locked
    import atomics
except ModuleNotFoundError:
    pass

try:
    import ultraimport
    Exceptions = ultraimport('__dir__/Exceptions.py')
    try:
        log = ultraimport('__dir__/utils/log.py', 'log', package=1)
        log.log_targets = [ sys.stderr ]
    except ultraimport.ResolveImportError:
        import logging as log
except ModuleNotFoundError:
    from . import Exceptions
    try:
        from .utils import log
        log.log_targets = [ sys.stderr ]
    except ModuleNotFoundError:
        import logging as log

def remove_shm_from_resource_tracker():
    """
    Monkey-patch multiprocessing.resource_tracker so SharedMemory won't be tracked
    More details at: https://bugs.python.org/issue38119
    """
    # pylint: disable=protected-access, import-outside-toplevel
    # Ignore linting errors in this bug workaround hack
    from multiprocessing import resource_tracker
    def fix_register(name, rtype):
        if rtype == "shared_memory":
            return None
        return resource_tracker._resource_tracker.register(name, rtype)
    resource_tracker.register = fix_register
    def fix_unregister(name, rtype):
        if rtype == "shared_memory":
            return None
        return resource_tracker._resource_tracker.unregister(name, rtype)
    resource_tracker.unregister = fix_unregister
    if "shared_memory" in resource_tracker._CLEANUP_FUNCS:
        del resource_tracker._CLEANUP_FUNCS["shared_memory"]

#More details at: https://bugs.python.org/issue38119
remove_shm_from_resource_tracker()

# Header of each record in the update stream: one marker byte, 4 bytes little endian length
# of the body, 4 bytes little endian CRC32 of the body and one FF byte.
# The CRC32 is `zlib.crc32(body, full_dump_counter)`. Readers check it before they parse a record,
# so they never parse a record that is not complete yet, or one from before the last full dump.
HEADER = struct.Struct('<BIIB')
HEADER_SIZE = HEADER.size

# Header of a full dump: FF byte, 4 bytes little endian length of the body, 4 bytes little endian
# CRC32 of the body and one FF byte. The CRC32 is `zlib.crc32(body, full_dump_counter)` with the
# full dump counter that the full dump belongs to. A reader that sees the new counter before the
# new full dump, e.g. on a CPU with weak memory order, finds a wrong CRC32 and tries again.
FULL_DUMP_HEADER = struct.Struct('<BIIB')
FULL_DUMP_HEADER_SIZE = FULL_DUMP_HEADER.size

class FullDumpMismatch(Exception):
    """ The full dump does not belong to the full dump counter, see FULL_DUMP_HEADER """

# The first byte of a record header tells how its body is serialized.
# Full dumps always use MARKER_SERIALIZER.
MARKER_SERIALIZER = 0xFF  # The serializer of the UltraDict, by default pickle
MARKER_MARSHAL    = 0xFE  # marshal, only used for simple types instead of pickle

# With the default pickle serializer, updates with keys and values of exactly these
# types use marshal, which is several times faster than pickle and gives the same result.
MARSHAL_TYPES = frozenset((int, float, str, bytes, bool, type(None)))
# Fixed marshal format version, so processes with different Python versions can read it
MARSHAL_VERSION = 4

# Value of a delete record in the update stream. Readers ignore it.
# None works with every serializer, e.g. also with JSON.
DELETED = None

# Optional C implementation of the hot paths, see _fast.pyx.
# Set the environment variable ULTRADICT_PURE_PYTHON=1 to disable it.
ACCELERATED = False
if not os.environ.get('ULTRADICT_PURE_PYTHON'):
    try:
        from . import _fast
        if getattr(_fast, 'VERSION', None) == 3:
            ACCELERATED = True
    except ImportError:
        pass

if ACCELERATED:
    FastCore = _fast.FastCore
    SharedLockCore = _fast.SharedLockCore
else:
    FastCore = dict
    class SharedLockCore():
        __slots__ = ()

# Readers do not lock. On CPUs that can reorder memory accesses, e.g. arm64, a reader can see the
# new stream position or full dump counter before the data it points to, if the writer does not
# use the C extension. The CRC32 in each header detects this, see HEADER and FULL_DUMP_HEADER,
# and the reader tries again after these pauses in seconds.
RECORD_RETRY_DELAYS = (0, 0.0001, 0.001, 0.01, 0.1)

# Errors that a bad record in the update stream raises.
# EOFError and ValueError come from marshal on a damaged record.
RECORD_ERRORS = (AssertionError, struct.error, pickle.UnpicklingError, EOFError, ValueError)

if ACCELERATED:
    # Full memory barrier, used around full dumps
    memory_fence = _fast.fence
else:
    def memory_fence():
        """ Pure Python has no memory barrier, see RECORD_RETRY_DELAYS """

def make_fast_exchange(atomic):
    """
    Return a function `exchange(desired) -> old` that atomically swaps a single byte
    in shared memory and returns the old value. `desired` must be 0 or 1.

    Only used without the C extension module `_fast`.

    The public API of the `atomics` package allocates two cffi buffers on every call,
    which costs several microseconds. This function calls the same underlying C function
    of `atomics` directly with preallocated arguments instead.

    Returns None if the internals of `atomics` do not match, so the caller can fall back
    to the public API.
    """
    # pylint: disable=protected-access
    try:
        import ctypes, cffi, threading
        from atomics._impl.enums import OpType, MemoryOrder
        fp_exchange = atomic._core.get_op_func(OpType.EXCHANGE)
        if fp_exchange is None or atomic._core.width != 1:
            return None
        ffi = cffi.FFI()
        c_exchange = ffi.cast("void(*)(void *, const void *, int, void *)", ctypes.cast(fp_exchange, ctypes.c_void_p).value)
        address = ffi.cast("void *", atomic._core.address)
        values = (ffi.new("unsigned char[1]", [0]), ffi.new("unsigned char[1]", [1]))
        order = int(MemoryOrder.SEQ_CST)
    except Exception: # pylint: disable=broad-except
        return None

    # The C call releases the GIL, so each thread needs its own result buffer
    local = threading.local()
    # The address must never be used after the shared memory is released, see `close()`
    open_state = [True]

    def exchange(desired):
        if not open_state[0]:
            raise Exceptions.AlreadyClosed('The lock of this UltraDict is already closed')
        try:
            result = local.result
        except AttributeError:
            result = local.result = ffi.new("unsigned char[1]")
        c_exchange(address, values[desired], order, result)
        return result[0]

    def close():
        open_state[0] = False

    exchange.close = close
    return exchange

def python_attrs(attrs, core_attrs):
    """ The attributes that need a slot in a Python class, without the ones in the C base class """
    return tuple(attr for attr in attrs if not (ACCELERATED and attr in core_attrs))

class UltraDict(collections.UserDict, FastCore):

    Exceptions = Exceptions
    log = log

    class RLock(multiprocessing.synchronize.RLock):
        """ Not yet used """
        pass

    class SharedLock(SharedLockCore):
        """
        Lock stored in shared_memory to provide an additional layer of protection,
        e.g. when using spawned processes.

        Internally uses atomics package of patomics for atomic locking.

        This is needed if you write to the shared memory with independent processes.
        """

        # With the C extension, `has_lock`, `pid` and `next_acquire_parameters` live in SharedLockCore
        __slots__ = python_attrs(('parent', 'has_lock',  'ctx', 'lock_atomic', 'lock_remote', \
            'pid', 'pid_bytes', 'pid_remote', 'pid_remote_ctx', 'pid_remote_atomic', \
            'next_acquire_parameters', 'exchange', '__weakref__'), ('has_lock', 'pid', 'next_acquire_parameters', 'exchange'))

        def __init__(self, parent, lock_name, pid_name):
            self.has_lock = 0
            self.next_acquire_parameters = ()

            # `lock_name` contains the name of the attribute that the parent uses
            # to store the memory view on the remote lock, so `self.lock_remote` is
            # referring to a memory view
            self.lock_remote = getattr(parent, lock_name)
            self.pid_remote = getattr(parent, pid_name)

            self.init_pid()

            if ACCELERATED:
                # The C base class works on the lock byte and the PID bytes directly,
                # it does not need the atomics package
                self._bind(self.lock_remote, self.pid_remote)
            else:
                load_error = None
                try:
                    self.ctx = atomics.atomicview(buffer=self.lock_remote[0:1], atype=atomics.BYTES)
                    self.pid_remote_ctx = atomics.atomicview(buffer=self.pid_remote[0:4], atype=atomics.BYTES)
                except NameError as e:
                    self.cleanup()
                    raise e
                except OSError as e:
                    # The native library of atomics cannot be loaded, e.g. the atomics 1.0.3
                    # wheel for macOS arm64 contains a library for x86_64
                    load_error = str(e)
                if load_error is not None:
                    # Raised outside of the except block, so the traceback of the OSError and the
                    # half created atomics objects with their exports of the shared memory are freed
                    self.cleanup()
                    raise Exceptions.MissingDependency(
                        f"The `atomics` package cannot load its native library ({load_error}). "
                        "Build the UltraDict C extension, or install atomics from source with "
                        "`pip install --force-reinstall --no-binary atomics atomics`.")
                self.lock_atomic = self.ctx.__enter__()
                self.pid_remote_atomic = self.pid_remote_ctx.__enter__()
                self.exchange = make_fast_exchange(self.lock_atomic) or self.exchange_public_api

            # Python cannot remove a fork handler, so it only holds a weak reference to the lock
            weak_self = weakref.ref(self)

            def after_fork():
                lock = weak_self()
                if lock is None or not hasattr(lock, 'pid_bytes'):
                    # Gone or closed
                    return
                if lock.has_lock:
                    raise Exception("Release the SharedLock before you fork the process")

                # After forking, we got a new pid
                lock.init_pid()

            if sys.platform != 'win32':
                os.register_at_fork(after_in_child=after_fork)

        def init_pid(self):
            self.pid = multiprocessing.current_process().pid
            self.pid_bytes = self.pid.to_bytes(4, 'little')

        def acquire_with_timeout(self, block=True, sleep_time=0.000001, timeout=1.0, steal_after_timeout=False):
            # The block parameter will be ignored
            time_start = None
            blocking_pid = None
            while True:
                try:
                    return self.acquire(block=False, sleep_time=sleep_time)
                except Exceptions.CannotAcquireLock as e:
                    if not time_start:
                        time_start = e.timestamp
                        blocking_pid = e.blocking_pid

                    # We should not be the blocking pid
                    assert blocking_pid != self.pid

                    time_passed = time.monotonic() - time_start

                    if time_passed >= timeout:
                        if steal_after_timeout:
                            # If the blocking pid has changed meanwhile, someone else took or stole the lock
                            if blocking_pid == e.blocking_pid:
                                self.steal_from_dead(from_pid=blocking_pid, release=True)
                            time_start = None
                            blocking_pid = None
                            continue
                        raise Exceptions.CannotAcquireLockTimeout(blocking_pid = e.blocking_pid, timestamp=time_start) from None


        #@profile
        def acquire(self, block=True, sleep_time=0.000001, timeout=None, steal_after_timeout=False):
            # If we already own the lock, just increment our counter
            if self.has_lock:
                self.has_lock += 1
                return True

            if timeout:
                return self.acquire_with_timeout(sleep_time=sleep_time, timeout=timeout, steal_after_timeout=steal_after_timeout)

            exchange = self.exchange

            while True:
                # We need both, the shared lock to be False and the lock_pid to be 0
                # Inlined version of `test_and_inc()`
                if exchange(1) == 0:

                    assert self.has_lock == 0
                    self.has_lock = 1

                    # If nobody had owned the lock, so the remote pid should be zero
                    assert self.pid_remote == b'\x00\x00\x00\x00'

                    self.pid_remote[:] = self.pid_bytes
                    return True

                # If set to 0, we practically have a busy wait
                if sleep_time:
                    time.sleep(sleep_time)

                if not block:
                    raise Exceptions.CannotAcquireLock(blocking_pid=self.get_remote_pid())

        def exchange_public_api(self, desired):
            """ Slow fallback for `exchange()` if `make_fast_exchange()` cannot be used """
            return self.lock_atomic.exchange(b'\x01' if desired else b'\x00')[0]

        if not ACCELERATED:
            # With the C extension, SharedLockCore provides these methods

            #@profile
            def test_and_inc(self):
                if self.exchange(1) != 0:
                    # Oops, someone else was faster than us
                    return False
                return True

            #@profile
            def test_and_dec(self):
                if self.exchange(0) != 1:
                    raise Exception("Failed to release lock")
                return True

            #@profile
            def release(self, *args):
                #log.debug("Release lock, lock={}", self.has_lock)
                if self.has_lock > 0:
                    if self.pid_remote != self.pid_bytes:
                        owner = int.from_bytes(self.pid_remote, 'little')
                        raise Exception(f"Our lock for pid {self.pid} was stolen by pid {owner}")
                    self.has_lock -= 1
                    # Last local lock released, release shared lock
                    if not self.has_lock:
                        self.pid_remote[:] = b'\x00\x00\x00\x00'
                        self.test_and_dec()
                    #log.debug("Relased lock, lock={} pid_remote={}", self.has_lock, int.from_bytes(self.pid_remote, 'little'))
                    return True

                return False

            def __enter__(self):
                if self.next_acquire_parameters:
                    self.acquire(*self.next_acquire_parameters)
                    self.reset_acquire_parameters()
                else:
                    self.acquire()
                return self

            def __exit__(self, type, value, traceback):
                self.release()
                # Make sure exceptions are not ignored
                return False

        def reset(self):
            # Risky, releases the lock even if another process holds it.
            # Clear the PID first, so a process that takes the lock next finds no stale PID.
            self.pid_remote[:] = b'\x00\x00\x00\x00'
            # The lock is the first byte of `lock_remote`
            self.lock_remote[0:1] = b'\x00'
            self.has_lock = 0

        def reset_acquire_parameters(self):
            self.next_acquire_parameters = ()

        def steal(self, from_pid=0, release=False):
            if self.has_lock:
                raise Exception("Cannot steal the lock because we have already acquired it. Use release() to release the lock.")

            #log.debug(f'Stealing from_pid={from_pid}, remote_pid={self.get_remote_pid()}')

            # It's not locked, so nothing to steal from
            if not self.get_remote_lock():
                return False

            # Someone else has stolen the lock
            if from_pid != self.get_remote_pid():
                return False

            # Stealing the lock means actually just putting our pid into the shared memory overwriting the other pid.
            # This can go wrong if the lock owner is actually still alive and working.
            if ACCELERATED:
                success = self.compare_exchange_pid(from_pid.to_bytes(4, 'little'), self.pid_bytes)
            else:
                success = self.pid_remote_atomic.cmpxchg_strong(expected=from_pid.to_bytes(4, 'little'), desired=self.pid_bytes).success
            if success:
                self.has_lock = 1
                if release:
                    self.release()
            return success

        def steal_from_dead(self, from_pid=0, release=False):
            """ Check if from_pid is actually a dead process and if yes, steal the lock from it.
                Optionally, the lock can be directly released after stealing it.
            """

            try:
                import psutil
            except ModuleNotFoundError:
                raise Exceptions.MissingDependency("Install `psutil` Python package to use shared_lock=True") from None
            # No process must exist anymore with the from_pid or it must at least be dead (ie. zombie status)
            try:
                p = psutil.Process(from_pid)
                if p and p.is_running() and p.status() not in [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD]:
                    raise Exception(f"Trying to steal lock from process that is still alive, something seems really wrong from_pid={from_pid} pid={self.pid} p={p}")
            except psutil.NoSuchProcess:
                # If the process is already gone, we cannot find information about it.
                # It will be safe to steal the lock.
                pass
            except Exception as e:
                raise e

            return self.steal(from_pid=from_pid, release=release)

        def status(self):
            return {
                'has_lock': self.has_lock,
                'lock_remote': int.from_bytes(self.lock_remote, 'little'),
                'pid': self.pid,
                'pid_remote': int.from_bytes(self.pid_remote, 'little'),
            }

        def print_status(self, status=None):
            import pprint
            if not status:
                status = self.status()
            pprint.pprint(status)

        def cleanup(self):
            # The fast exchange holds the raw address of the shared memory,
            # it must never be used after the memory is released
            if ACCELERATED:
                self._unbind()
            elif hasattr(self, 'exchange'):
                if hasattr(self.exchange, 'close'):
                    self.exchange.close()
                del self.exchange
            if hasattr(self, 'ctx'):
                self.ctx.__exit__(None, None, None)
                del self.ctx
            if hasattr(self, 'lock_atomic'):
                del self.lock_atomic
            if hasattr(self, 'pid_remote_ctx'):
                self.pid_remote_ctx.__exit__(None, None, None)
                del self.pid_remote_ctx
            if hasattr(self, 'pid_remote_atomic'):
                del self.pid_remote_atomic
            del self.lock_remote
            del self.pid_remote
            del self.pid_bytes
            del self.pid

        def get_remote_pid(self):
            return int.from_bytes(self.pid_remote, 'little')

        def get_remote_lock(self):
            return int.from_bytes(self.lock_remote, 'little')

        def __repr__(self):
            return f"{self.__class__.__name__} @{hex(id(self))} lock_remote={int.from_bytes(self.lock_remote, 'little')}, has_lock={self.has_lock}, pid={self.pid}), pid_remote={int.from_bytes(self.pid_remote, 'little')}"

        def __call__(self, block=True, timeout=None, sleep_time=0.000001, steal_after_timeout=False):
            self.next_acquire_parameters = ( block, timeout, sleep_time, steal_after_timeout )

            return self

    # The attributes that `status()` shows
    STATUS_ATTRS = 'name', 'control', 'buffer', 'buffer_size', 'lock', 'shared_lock', \
        'update_stream_position', 'update_stream_position_remote', \
        'full_dump_counter', 'full_dump_memory', 'full_dump_size', \
        'serializer', \
        'lock_pid_remote', \
        'lock_remote', \
        'full_dump_counter_remote', \
        'full_dump_static_size_remote', \
        'shared_lock_remote', \
        'recurse', 'recurse_remote', 'recurse_register', \
        'full_dump_memory_name_remote', \
        'data', 'closed', 'auto_unlink', \
        'finalizer'

    # With the C extension, these attributes live in FastCore
    CORE_ATTRS = 'data', 'update_stream_position', 'full_dump_counter', 'buffer_size', \
        'serializer', 'recurse', 'fast_lock', 'use_marshal'

    __slots__ = python_attrs(STATUS_ATTRS + ('fast_lock', 'use_marshal', 'buffer_remote'), CORE_ATTRS)

    # True if the C extension module `_fast` is used
    accelerated = ACCELERATED

    def __init__(self, *args, name=None, create=None, buffer_size=10_000, serializer=pickle, shared_lock=None, full_dump_size=None,
            auto_unlink=None, recurse=None, recurse_register=None, **kwargs):
        # pylint: disable=too-many-branches, too-many-statements

        # On win32, only multiples of 4k are allowed
        if sys.platform == 'win32':
            buffer_size = -(buffer_size // -4096) * 4096
            if full_dump_size:
                full_dump_size = -(full_dump_size // -4096) * 4096

        assert buffer_size < 2**32

        if recurse:
            assert serializer == pickle

        self.data = {}

        # Local position, ie. the last position we have processed from the stream
        self.update_stream_position  = 0

        # Local version counter for the full dumps, ie. if we find a higher version
        # remote, we need to load a full dump
        self.full_dump_counter       = 0

        self.closed = False
        self.auto_unlink = auto_unlink

        # Small 1000 bytes of shared memory where we store the runtime state
        # of our update stream
        self.control = self.get_memory(create=create, name=name, size=1000)
        self.name = self.control.name

        def finalize(weak_self, name):
            #log.debug('Finalize', name)
            resolved_self = weak_self()
            if resolved_self is not None:
                #log.debug('Weakref is intact, closing')
                resolved_self.close(from_finalizer=True)
            #log.debug('Finalized')

        self.finalizer = weakref.finalize(self, finalize, weakref.ref(self), self.name)

        self.init_remotes()

        self.serializer = serializer
        # Fast path for simple types, see MARSHAL_TYPES
        self.use_marshal = serializer is pickle

        # Actual stream buffer that contains marshalled data of changes to the dict
        self.buffer = self.get_memory(create=create, name=self.name + '_memory', size=buffer_size)
        # TODO: Raise exception if buffer size mismatch
        self.buffer_size = self.buffer.size
        # Keep a direct reference to the memory view of the stream buffer,
        # `SharedMemory.buf` is a property and slower to access
        self.buffer_remote = self.buffer.buf
        if ACCELERATED:
            # The C base class reads and writes the control memory and the stream buffer directly
            self._bind(self.control.buf, self.buffer.buf)
            # The C base class may replay the stream itself, unless a subclass changes apply_update()
            self.direct_replay = type(self).apply_update is UltraDict.apply_update

        self.full_dump_memory = None

        # Dynamic full dump memory handling
        # Warning: Issues on Windows when the process ends that has created the full dump memory
        self.full_dump_size = None

        if hasattr(self.control, 'created_by_ultra'):

            if auto_unlink is None:
                self.auto_unlink = True

            if recurse:
                self.recurse_remote[0:1] = b'1'

            if shared_lock:
                self.shared_lock_remote[0:1] = b'1'

            # We created the control memory, thus let's check if we need to create the
            # full dump memory as well
            if full_dump_size:
                self.full_dump_size = full_dump_size
                self.full_dump_static_size_remote[:] = full_dump_size.to_bytes(4, 'little')

                self.full_dump_memory = self.get_memory(create=True, name=self.name + '_full', size=full_dump_size)
                self.full_dump_memory_name_remote[:] = self.full_dump_memory.name.encode('utf-8').ljust(255)

        # We just attached to the existing control
        else:
            # TODO: Detect configuration mismatch and raise an exception

            # Check if we have a fixed size full dump memory
            size = int.from_bytes(self.full_dump_static_size_remote, 'little')

            # Check if shared_lock parameter was not set to inconsistent value
            shared_lock_remote = self.shared_lock_remote[0:1] == b'1'
            if shared_lock is None:
                shared_lock = shared_lock_remote
            elif shared_lock != shared_lock_remote:
                raise Exceptions.ParameterMismatch(f"shared_lock={shared_lock} was set but the creator has used shared_lock={shared_lock_remote}")

            # Check if recurse parameter was not set to inconsistent value
            recurse_remote = self.recurse_remote[0:1] == b'1'
            if recurse is None:
                recurse = recurse_remote
            elif recurse != recurse_remote:
                raise Exceptions.ParameterMismatch(f"recurse={recurse} was set but the creator has used recurse={recurse_remote}")

            # Got existing size of full dump memory, that must mean it's static size
            # and we should attach to it
            if size > 0:
                self.full_dump_size = size
                self.full_dump_memory = self.get_memory(create=False, name=self.name + '_full')

        # Local lock for all processes and threads created by the same interpreter
        if shared_lock:
            try:
                self.lock = self.SharedLock(self, 'lock_remote', 'lock_pid_remote')
            except NameError:
                #self.cleanup()
                raise Exceptions.MissingDependency("Install `atomics` Python package to use shared_lock=True") from None
        else:
            self.lock = multiprocessing.RLock()

        # Lock for internal use. For the RLock, we use the underlying C level semaphore
        # directly because the Python wrapper of RLock doubles the cost of each acquire
        # and release. Both refer to the same semaphore, so they can be mixed freely.
        self.fast_lock = getattr(self.lock, '_semlock', self.lock)

        self.shared_lock = shared_lock

        # Parameters that could be read from remote if we are connecting to an existing UltraDict
        self.recurse = recurse

        # In recurse mode, we must ensure a recurse register
        if self.recurse:
            # Must be either the name of an UltraDict as a string or an UltraDict instance
            if recurse_register is not None:
                if type(recurse_register) == str:
                    self.recurse_register = UltraDict(name=recurse_register)
                elif type(recurse_register) == UltraDict:
                    self.recurse_register = recurse_register
                else:
                    raise Exception("Bad type for recurse_register")

            # If no register was defined, we should create one
            else:
                self.recurse_register = UltraDict(name=f'{self.name}_register',
                    recurse=False, auto_unlink=False, shared_lock=self.shared_lock)
                # The register should not run its own finalizer if we need it later for unlinking our nested children
                if self.auto_unlink:
                    self.recurse_register.finalizer.detach()
                    #log.debug("Created recurse register with name={}", self.recurse_register.name)

        else:
            self.recurse_register = None

        super().__init__(*args, **kwargs)

        # Load all data from shared memory
        self.apply_update()

        if sys.platform == 'win32':
            if not shared_lock:
                log.warning('You are running on win32, potentially without locks. Consider setting shared_lock=True')


        #if auto_unlink:
        #    atexit.register(self.unlink)
        #else:
        #    atexit.register(self.cleanup)

        #log.debug("Initialized", self.name)

    def __del__(self):
        #log.debug("__del__", self.name)
        self.close()
        #if hasattr(self, 'recurse') and self.recurse:
        #    #log.debug("Close recurse register")
        #    self.recurse_register.close()
        #    del self.recurse_register


    def init_remotes(self):
        # Memoryviews to the right buffer position in self.control

        # The two counters that are checked on every access are cast to an unsigned
        # 4 bytes integer, so reading them is a simple index access, e.g.
        # `self.update_stream_position_remote[0]`. They use the native byte order.
        # This is safe because shared memory never leaves the machine.
        self.update_stream_position_remote = self.control.buf[ 0:  4].cast('I')
        self.lock_pid_remote               = self.control.buf[ 4:  8]
        self.lock_remote                   = self.control.buf[ 8: 10]
        self.full_dump_counter_remote      = self.control.buf[10: 14].cast('I')
        self.full_dump_static_size_remote  = self.control.buf[14: 18]
        self.shared_lock_remote            = self.control.buf[18: 19]
        self.recurse_remote                = self.control.buf[19: 20]
        self.full_dump_memory_name_remote  = self.control.buf[20:275]

    def del_remotes(self):
        """
        Delete all instance attributes whose name ends with '_remote' from
        the instance for cleanup. This shall ensure there are no
        reference left to shared memory views so proper cleanup can happen.
        """
        remotes = [ r for r in dir(self) if r.endswith('_remote') ]
        for r in remotes:
            if hasattr(self, r):
                delattr(self, r)

    def __reduce__(self):
        from functools import partial
        return (partial(self.__class__, name=self.name, auto_unlink=self.auto_unlink, recurse_register=self.recurse_register), ())

    @staticmethod
    def get_memory(*, create=True, name=None, size=0):
        """
        Attach an existing SharedMemory object with `name`.

        If `create` is True, create the object if it does not exist.
        """
        assert size > 0 or not create
        if name:
            # First try to attach to existing memory
            try:
                memory = multiprocessing.shared_memory.SharedMemory(name=name)
                #log.debug('Attached shared memory: ', memory.name)

                if create:
                    raise Exceptions.AlreadyExists(f"Cannot create memory '{name}' because it already exists")

                return memory
            except FileNotFoundError:
                pass

        # No existing memory found
        if create or create is None:
            memory = multiprocessing.shared_memory.SharedMemory(create=True, size=size, name=name)
            #multiprocessing.resource_tracker.unregister(memory._name, 'shared_memory')
            # Remember that we have created this memory
            memory.created_by_ultra = True
            #log.debug('Created shared memory: ', memory.name)

            return memory

        raise Exceptions.CannotAttachSharedMemory(f"Could not get memory '{name}'")

    #@profile
    def dump(self):
        """ Dump the full dict into shared memory """

        with self.fast_lock:
            old = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')

            self.apply_update()

            marshalled = self.serializer.dumps(self.data)
            length = len(marshalled)
            # The full dump belongs to the next value of the counter, see FULL_DUMP_HEADER
            new_full_dump_counter = self.full_dump_counter_remote[0] + 1

            # If we don't have a fixed size, let's create full dump memory dynamically
            # TODO: This causes issues on Windows because the memory is not persistant
            #       Maybe switch to mmaped file?
            if self.full_dump_size and self.full_dump_memory:
                full_dump_memory = self.full_dump_memory
            else:
                # Dynamic full dump memory
                full_dump_memory = self.get_memory(create=True, size=length + FULL_DUMP_HEADER_SIZE)

            #log.debug("Full dump memory: ", full_dump_memory)

            if length + FULL_DUMP_HEADER_SIZE > full_dump_memory.size:
                raise Exceptions.FullDumpMemoryFull(f'Full dump memory too small for full dump: needed={length + FULL_DUMP_HEADER_SIZE} got={full_dump_memory.size}')

            # Write body, then header, see FULL_DUMP_HEADER
            full_dump_memory.buf[FULL_DUMP_HEADER_SIZE:FULL_DUMP_HEADER_SIZE + length] = marshalled
            checksum = zlib.crc32(marshalled, new_full_dump_counter & 0xFFFFFFFF)
            FULL_DUMP_HEADER.pack_into(full_dump_memory.buf, 0, 0xFF, length, checksum, 0xFF)

            # On Windows, if we close it, it cannot be read anymore by anyone else.
            if not self.full_dump_size and sys.platform != 'win32':
                full_dump_memory.close()

            # TODO: There's a slight chance of something going wrong when we first update
            #       the remote memory name and then the counter.

            # Only after we have filled the new full dump memory with the marshalled data,
            # we update the remote name so other users can find it
            if not (self.full_dump_size and self.full_dump_memory):
                self.full_dump_memory_name_remote[:] = full_dump_memory.name.encode('utf-8').ljust(255)

            # The full dump and its name must be visible to other processes before the
            # stream position and the counter change, see `memory_fence()`
            memory_fence()

            # Reset the stream position to zero as we have
            # just provided a fresh new full dump.
            # This must happen before the counter is incremented: a reader that sees the new
            # counter loads the full dump and then must not replay old records of the stream.
            self.update_stream_position = 0
            self.update_stream_position_remote[0] = 0
            memory_fence()

            self.full_dump_counter += 1
            current = self.full_dump_counter_remote[0]
            # Now also increment the remote counter
            self.full_dump_counter_remote[0] = current + 1

            #log.info("Dumped dict with {} elements to {} bytes, remote_counter={}", len(self), len(marshalled), current+1)

            # If the old full dump memory was dynamically created, delete it
            if old and old != full_dump_memory.name and not self.full_dump_size:
                self.unlink_by_name(old)

            # On Windows, we need to keep a reference to the full dump memory,
            # otherwise it's destoryed
            self.full_dump_memory = full_dump_memory

            return full_dump_memory

    def get_full_dump_memory(self, max_retry=3, retry=0):
        """
        Attach to the full dump memory.

        Retry if necessary for a low number of times. It could happen that the full
        dump memory was removed because a new full dump was created before we had the
        chance to read the old full dump.

        """
        try:
            name = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')
            #log.debug("Full dump name={}", name)
            assert len(name) >= 1
            return self.get_memory(create=False, name=name)
        except Exceptions.CannotAttachSharedMemory as e:
            if retry < max_retry:
                return self.get_full_dump_memory(max_retry=max_retry, retry=retry+1)
            elif retry == max_retry:
                # On the last retry, let's use a lock to ensure we can safely import the dump
                with self.fast_lock:
                    return self.get_full_dump_memory(max_retry=max_retry, retry=retry+1)
            else:
                raise e

    #@profile
    def load(self, force=False):
        """
        Opportunistacally load full dumps without any locking.

        There is a rare case where a full dump is replaced with a newer full dump while
        we didn't have the chance to load the old one. In this case, we just retry.
        """
        # A full dump that does not match the counter yet can arrive a moment later,
        # see FULL_DUMP_HEADER
        for delay in RECORD_RETRY_DELAYS:
            time.sleep(delay)
            try:
                return self.load_once(force=force)
            except FullDumpMismatch:
                pass
        raise AssertionError("The full dump does not match the full dump counter")

    def load_once(self, force=False):
        """ See `load()` """
        full_dump_counter = self.full_dump_counter_remote[0]
        # Read the name and the full dump only after the counter, see `memory_fence()`
        memory_fence()
        #log.debug("Loading full dump local_counter={} remote_counter={}", self.full_dump_counter, full_dump_counter)
        try:
            if force or (self.full_dump_counter < full_dump_counter):
                if self.full_dump_size and self.full_dump_memory:
                    full_dump_memory = self.full_dump_memory
                else:
                    # Retry if necessary
                    full_dump_memory = self.get_full_dump_memory()

                buf = full_dump_memory.buf

                # Read header, see FULL_DUMP_HEADER
                start_marker, length, checksum, end_marker = FULL_DUMP_HEADER.unpack_from(buf, 0)
                body = bytes(buf[FULL_DUMP_HEADER_SIZE:FULL_DUMP_HEADER_SIZE + length])
                if start_marker != 0xFF or end_marker != 0xFF or length <= 0 \
                        or zlib.crc32(body, full_dump_counter & 0xFFFFFFFF) != checksum:
                    if sys.platform != 'win32' and not self.full_dump_memory:
                        full_dump_memory.close()
                    raise FullDumpMismatch()
                # Unserialize the full dump
                self.data = self.serializer.loads(body)
                self.full_dump_counter = full_dump_counter
                self.update_stream_position = 0

                if sys.platform != 'win32' and not self.full_dump_memory:
                    full_dump_memory.close()
            else:
                raise Exception("Cannot load full dump, no new data available")
        except AssertionError as e:
            full_dump_delta = self.full_dump_counter_remote[0] - self.full_dump_counter
            if full_dump_delta > 1:
                # If more than one new full dump was created during the time we were trying to load one full dump
                # it can happen that our full dump has just disappeared
                return self.load_once(force=True)
            # TODO: Before we reach max recursion depth, try to load the full dump using a lock
            self.print_status()
            raise e

    #@profile
    def append_update(self, key, item, delete=False):
        """ Append dict changes to shared memory stream """

        # If mode is 0, it means delete the key from the dict
        # If mode is 1, it means update the key
        #mode = not delete
        marshalled = self.serializer.dumps((not delete, key, item))

        with self.fast_lock:
            self.append_marshalled(MARKER_SERIALIZER, marshalled, key, item, delete)

    def serialize_update(self, mode, key, item):
        """
        Serialize one change for the update stream.

        Returns the header marker byte and the serialized change.
        """
        if self.use_marshal and type(key) in MARSHAL_TYPES and type(item) in MARSHAL_TYPES:
            return MARKER_MARSHAL, marshal.dumps((mode, key, item), MARSHAL_VERSION)
        return MARKER_SERIALIZER, self.serializer.dumps((mode, key, item))

    #@profile
    def append_marshalled(self, marker, marshalled, key, item, delete):
        """
        Append an already serialized change to the shared memory stream.
        `marker` tells how `marshalled` was serialized, see MARKER_SERIALIZER and MARKER_MARSHAL.

        The caller must hold the lock.
        """
        length = len(marshalled)
        start_position = self.update_stream_position_remote[0]
        end_position = start_position + HEADER_SIZE + length
        #log.debug("Update start from={} len={}", start_position, length)
        if end_position > self.buffer_size:
            #log.debug("Buffer is full")

            # todo: is is necessary? apply_update() is also done inside dump()
            self.apply_update()
            if not delete:
                self.data.__setitem__(key, item)
            self.dump()
            return

        # Write header and body directly into the shared memory, without
        # concatenating them in a temporary bytes object first
        # Seed the CRC32 with the shared counter. The local counter is old if we have not loaded
        # the last full dump yet, e.g. in append_update(). The lock keeps the shared counter stable.
        buf = self.buffer_remote
        checksum = zlib.crc32(marshalled, self.full_dump_counter_remote[0])
        buf[start_position + HEADER_SIZE:end_position] = marshalled
        HEADER.pack_into(buf, start_position, marker, length, checksum, 0xFF)

        # Inform others about it, only after the full record is written
        self.update_stream_position = end_position
        self.update_stream_position_remote[0] = end_position
        #log.debug("Update end to={} buffer_size={} ", end_position, self.buffer_size)

    #@profile
    def apply_update(self):
        """ Opportunistically apply dict changes from shared memory stream without any locking.  """

        if self.full_dump_counter < self.full_dump_counter_remote[0]:
            self.load(force=True)

        if self.update_stream_position < self.update_stream_position_remote[0]:
            try:
                if self.replay_stream():
                    # A new full dump appeared while we were reading the stream
                    return self.apply_update()
            except RECORD_ERRORS:

                # It can happen that a slow process is not fast enough reading the stream and some
                # other process already got around overwriting the current position. It is possible to
                # recover from this situation if and only if a new, fresh full dump exists that can be loaded.
                if self.full_dump_counter < self.full_dump_counter_remote[0]:
                    log.warning(f"Full dumps too fast full_dump_counter={self.full_dump_counter} full_dump_counter_remote={self.full_dump_counter_remote[0]}. Consider increasing buffer_size.")
                    return self.apply_update()

                # The record can be incomplete, see RECORD_RETRY_DELAYS. Writers release the lock
                # only after their record is complete, so read it again under the lock. A reader
                # that attached by name with the default lock has its own lock, so it also waits
                # a moment for the record to arrive. The pauses happen without the lock, so other
                # processes can write meanwhile. A record that is still bad at the end is damaged.
                for delay in RECORD_RETRY_DELAYS:
                    time.sleep(delay)
                    with self.fast_lock:
                        if self.full_dump_counter < self.full_dump_counter_remote[0]:
                            log.warning(f"Full dumps too fast full_dump_counter={self.full_dump_counter} full_dump_counter_remote={self.full_dump_counter_remote[0]}. Consider increasing buffer_size.")
                            return self.apply_update()
                        try:
                            if self.replay_stream():
                                return self.apply_update()
                            return None
                        except RECORD_ERRORS:
                            if delay == RECORD_RETRY_DELAYS[-1]:
                                raise

    def replay_stream_python(self):
        """
        Apply all new records of the update stream to the local copy.

        Returns True if a new full dump appeared while reading. The caller must then load it.
        Raises AssertionError on a bad record.
        """
        remote_position = self.update_stream_position_remote
        full_dump_counter_remote = self.full_dump_counter_remote

        # Remember start position in the update stream
        pos = self.update_stream_position
        #log.debug("Apply update: stream position own={} remote={} full_dump_counter={}", pos, remote_position[0], self.full_dump_counter)

        buf = self.buffer_remote
        data = self.data
        loads = None

        # Iterate over all updates until the start of the last update
        while pos < remote_position[0]:
            # Read header, it is a marker byte, 4 bytes of length and a FF byte
            marker, length, checksum, end_marker = HEADER.unpack_from(buf, pos)
            if end_marker != 0xFF:
                raise AssertionError(f"Bad header in update stream at position {pos}")
            pos += HEADER_SIZE
            #log.debug("Found update, update_stream_position={} length={}", self.update_stream_position, length + 6)

            # Copy the record, then check that no new full dump appeared meanwhile.
            # Writers only overwrite the stream after a new full dump, so the copy is valid.
            record = bytes(buf[pos:pos+length])
            if self.full_dump_counter != full_dump_counter_remote[0]:
                return True

            # Never parse a record that is not complete, see HEADER
            if zlib.crc32(record, self.full_dump_counter & 0xFFFFFFFF) != checksum:
                raise AssertionError(f"Bad checksum in update stream at position {pos - HEADER_SIZE}")

            # Unserialize the update data, we expect a tuple of mode, key and value
            if marker == MARKER_MARSHAL:
                mode, key, value = marshal.loads(record)
            elif marker == MARKER_SERIALIZER:
                if loads is None:
                    loads = self.serializer.loads
                mode, key, value = loads(record)
            else:
                raise AssertionError(f"Bad header in update stream at position {pos - HEADER_SIZE}")

            # Update or local dict cache (in our parent).
            # A delete can be applied twice when two threads read through the same UltraDict.
            if mode:
                data[key] = value
            else:
                data.pop(key, None)
            pos += length
            # Remember that we have applied the update
            self.update_stream_position = pos

        return False

    def update(self, other=None, *args, **kwargs):
        # pylint: disable=arguments-differ, keyword-arg-before-vararg

        # The original signature would be `def update(self, other=None, /, **kwargs)` but
        # this is not possible with Cython. *args will just be ignored.

        if other is not None:
            for k, v in other.items() if isinstance(other, collections.abc.Mapping) else other:
                self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def check_change_after_error(self, delete, key):
        """
        Called when a change cannot be serialized, before the caller raises the serialization error.
        Raises the error that the change itself would raise first, e.g. KeyError for a missing key.
        The local copy does not change, so it stays the same as in all other processes.
        """
        with self.fast_lock:
            if delete:
                self.apply_update()
                if key not in self.data:
                    raise KeyError(key)
            else:
                # Raises TypeError for an unhashable key
                hash(key)

    def setitem_recurse(self, key, item):
        """ Slower version of `__setitem__()` that wraps nested dicts in an UltraDict """
        with self.fast_lock:
            self.apply_update()

            if self.recurse:

                assert type(self.recurse_register) == UltraDict, "recurse_register must be an UltraDict instance"

                if type(item) == dict:
                    # TODO: Use parent's buffer with a namespace prefix?
                    item = UltraDict(item,
                                     recurse          = True,
                                     recurse_register = self.recurse_register,
                                     auto_unlink      = False,
                                     shared_lock      = self.shared_lock,
                                     buffer_size      = self.buffer_size,
                                     full_dump_size   = self.full_dump_size)

                    if item.name not in self.recurse_register.data:
                        self.recurse_register[item.name] = True

            # Update our local copy
            # It's important for the integrity to do this first
            self.data.__setitem__(key, item)

            # Append the update to the update stream
            self.append_update(key, item)
            # TODO: Do something if append_update() fails

    if ACCELERATED:
        # The C implementations of FastCore come after UserDict in the MRO,
        # so they must be set here explicitly
        __getitem__ = FastCore.__getitem__
        __setitem__ = FastCore.__setitem__
        __delitem__ = FastCore.__delitem__
        __contains__ = FastCore.__contains__
        __len__ = FastCore.__len__
        __iter__ = FastCore.__iter__
        get = FastCore.get
        keys = FastCore.keys
        values = FastCore.values
        replay_stream = FastCore.replay_stream

    else:
        replay_stream = replay_stream_python

        def changed(self):
            """
            True if the stream position or the full dump counter differ from ours.
            Also True after `close()`, then `apply_update()` raises AlreadyClosed.
            """
            try:
                return self.update_stream_position != self.update_stream_position_remote[0] \
                    or self.full_dump_counter != self.full_dump_counter_remote[0]
            except AttributeError:
                return True

        def __delitem__(self, key):
            #log.debug("__delitem__ {}", key)

            # Serialize outside of the lock to keep the time holding the lock short
            try:
                marker, marshalled = self.serialize_update(False, key, DELETED)
            except Exception:
                self.check_change_after_error(True, key)
                raise

            with self.fast_lock:
                if self.changed():
                    self.apply_update()

                # Update our local copy
                self.data.__delitem__(key)

                self.append_marshalled(marker, marshalled, key, DELETED, True)
                # TODO: Do something if append_update() fails

        def __setitem__(self, key, item):
            #log.debug("__setitem__ {}, {}", key, item)
            if self.recurse:
                self.setitem_recurse(key, item)
                return

            # Serialize outside of the lock to keep the time holding the lock short.
            # The marshal fast path is inlined here, see `serialize_update()`.
            try:
                if self.use_marshal and type(key) in MARSHAL_TYPES and type(item) in MARSHAL_TYPES:
                    marker = MARKER_MARSHAL
                    marshalled = marshal.dumps((True, key, item), MARSHAL_VERSION)
                else:
                    marker = MARKER_SERIALIZER
                    marshalled = self.serializer.dumps((True, key, item))
            except Exception:
                self.check_change_after_error(False, key)
                raise

            with self.fast_lock:
                if self.changed():
                    self.apply_update()

                # Update our local copy
                # It's important for the integrity to do this first
                self.data[key] = item

                # Append the update to the update stream
                self.append_marshalled(marker, marshalled, key, item, False)
                # TODO: Do something if append_update() fails

        def __getitem__(self, key):
            #log.debug("__getitem__ {}", key)

            # Calling apply_update() only when the stream position or the full dump counter
            # has changed saves a function call on every read
            if self.changed():
                self.apply_update()
            return self.data[key]

        def get(self, key, default=None):
            if self.changed():
                self.apply_update()
            return self.data.get(key, default)

        def __contains__(self, key):
            if self.changed():
                self.apply_update()
            return key in self.data

        def __len__(self):
            self.apply_update()
            return len(self.data)

        def __iter__(self):
            self.apply_update()
            return iter(self.data)

        def keys(self):
            self.apply_update()
            return self.data.keys()

        def values(self):
            self.apply_update()
            return self.data.values()

    def __eq__(self, other):
        self.apply_update()
        if isinstance(other, UltraDict):
            other.apply_update()
            return self.data == other.data
        return self.data == other

    def __repr__(self):
        try:
            self.apply_update()
        except Exceptions.AlreadyClosed:
            # If something goes wrong during the update, let's ignore it and still return a representation
            # TODO: Maybe somehow add a stale update warning?
            pass
        return self.data.__repr__()

    def status(self):
        """ Internal debug helper to get the control state variables """
        ret = { attr: getattr(self, attr) for attr in self.STATUS_ATTRS if hasattr(self, attr) and attr != 'data' }

        ret['update_stream_position_remote'] = self.update_stream_position_remote[0]
        ret['lock_pid_remote']               = int.from_bytes(self.lock_pid_remote, 'little')
        ret['lock_remote']                   = int.from_bytes(self.lock_remote, 'little')
        ret['shared_lock_remote']            = self.shared_lock_remote[0:1] == b'1'
        ret['recurse_remote']                = self.recurse_remote[0:1] == b'1'
        ret['lock']                          = self.lock
        ret['full_dump_counter_remote']      = self.full_dump_counter_remote[0]
        ret['full_dump_memory_name_remote']  = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip('\x00').strip()

        return ret

    def print_status(self, status=None, stderr=False):
        """ Internal debug helper to pretty print the control state variables """
        import pprint
        if not status:
            status = self.status()
        pprint.pprint(status, stream=sys.stderr if stderr else sys.stdout)

    def cleanup(self):
        #log.debug('Cleanup')

        #for item in self.data.items():
        #    print(type(item))

        # Release the direct pointers of the C base class to the shared memory
        if ACCELERATED:
            self._unbind()

        if hasattr(self, 'lock') and hasattr(self.lock, 'cleanup'):
            self.lock.cleanup()

        # If we use RLock(), this closes the file handle
        if hasattr(self, 'lock'):
            del self.lock
        # Must also go, it can refer to the same semaphore as `self.lock`
        if hasattr(self, 'fast_lock'):
            del self.fast_lock
        if hasattr(self, 'full_dump_memory'):
            del self.full_dump_memory

        data = self.data
        del self.data

        self.del_remotes()

        #self.control.close()
        #self.buffer.close()

        #if self.full_dump_memory:
        #    self.full_dump_memory.close()

        # No further cleanup on Windows, it will break everything
        #if sys.platform == 'win32':
        #    return

        #Only do cleanup once
        #atexit.unregister(self.cleanup)

        self.apply_update = self.raise_already_closed
        self.append_update = self.raise_already_closed

        return data

    def raise_already_closed(self, *args, **kwargs):
        raise Exceptions.AlreadyClosed('UltraDict already closed, you can only access the `UltraDict.data` buffer!')

    def unlink(self):
        self.close(unlink=True)

    def close(self, unlink=False, from_finalizer=False):
        #log.debug('Close name={} unlink={} auto_unlink={} creator={}', self.name, unlink, self.auto_unlink, hasattr(self.control, 'created_by_ultra'))

        if self.closed:
            #log.debug('Already closed, doing nothing')
            return
        self.closed = True

        if hasattr(self, 'finalizer'):
            self.finalizer.detach()

        if hasattr(self, 'full_dump_memory_name_remote'):
            full_dump_name = bytes(self.full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')

        data = self.cleanup()

        # If we are the master creator of the shared memory, we'll delete (unlink) it
        # including the full dump memory; for the full dump memory, we delete it even
        # if we are not the creator
        if unlink or (self.auto_unlink and hasattr(self.control, 'created_by_ultra')):
            #log.debug('Unlink', self.name)
            self.control.unlink()
            self.buffer.unlink()
            if full_dump_name:
                self.unlink_by_name(full_dump_name, ignore_errors=True)

            # Only the UltraDict that created the recurse register unlinks the nested
            # UltraDicts. Nested UltraDicts and UltraDicts that only use the register of
            # another UltraDict must not unlink the whole tree.
            if getattr(self, 'recurse', False) and self.owns_recurse_register():
                self.unlink_recursed()

        if hasattr(self, 'control'):
            self.control.close()
        if hasattr(self, 'buffer'):
            self.buffer.close()

        return data

    def owns_recurse_register(self):
        """ True if this UltraDict has created its recurse register, see `__init__()` """
        register = getattr(self, 'recurse_register', None)
        return type(register) == UltraDict and register.name == f'{self.name}_register'

    def unlink_recursed(self):
        #log.debug("Unlink recursed id={}", hex(id(self)))
        if not self.recurse or (type(self.recurse_register) != UltraDict):
            raise Exception("Cannot unlink recursed for non-recurse UltraDict")

        # A nested UltraDict can be gone already, e.g. after a crash or on Windows.
        # Continue with the others, so the register itself is also unlinked.
        for name in self.recurse_register.keys():
            #log.debug("Unlink recursed child name={}", name)
            self.unlink_by_name(name=name, ignore_errors=True)
            self.unlink_by_name(name=f"{name}_memory", ignore_errors=True)

        self.recurse_register.close(unlink=True)


    @staticmethod
    def unlink_by_name(name, ignore_errors=False):
        """
        Can be used to delete left over shared memory blocks after crashes.
        """
        try:
            #log.debug("Unlinking memory '{}'", name)
            memory = UltraDict.get_memory(create=False, name=name)
            memory.unlink()
            memory.close()
            return True
        except Exceptions.CannotAttachSharedMemory as e:
            if not ignore_errors:
                raise e
        return False



# Saved as a reference

#def bytes_to_int(bytes):
#    result = 0
#    for b in bytes:
#        result = result * 256 + int(b)
#    return result
#
#def int_to_bytes(value, length):
#    result = []
#    for i in range(0, length):
#        result.append(value >> (i * 8) & 0xff)
#    result.reverse()
#    return result

#class Mapping(dict):
#
#    def __init__(self, *args, **kwargs):
#        print("__init__", args, kwargs)
#        super().__init__(*args, **kwargs)
#
#    def __setitem__(self, key, item):
#        print("__setitem__", key, item)
#        self.__dict__[key] = item
#
#    def __getitem__(self, key):
#        print("__getitem__", key)
#        return self.__dict__[key]
#
#    def __repr__(self):
#        print("__repr__")
#        return repr(self.__dict__)
#
#    def __len__(self):
#        print("__len__")
#        return len(self.__dict__)
#
#    def __delitem__(self, key):
#        print("__delitem__")
#        del self.__dict__[key]
#
#    def clear(self):
#        print("clear")
#        return self.__dict__.clear()
#
#    def copy(self):
#        print("copy")
#        return self.__dict__.copy()
#
#    def has_key(self, k):
#        print("has_key")
#        return k in self.__dict__
#
#    def update(self, *args, **kwargs):
#        print("update")
#        return self.__dict__.update(*args, **kwargs)
#
#    def keys(self):
#        print("keys")
#        return self.__dict__.keys()
#
#    def values(self):
#        print("values")
#        return self.__dict__.values()
#
#    def items(self):
#        print("items")
#        return self.__dict__.items()
#
#    def pop(self, *args):
#        print("pop")
#        return self.__dict__.pop(*args)
#
#    def __cmp__(self, dict_):
#        print("__cmp__")
#        return self.__cmp__(self.__dict__, dict_)
#
#    def __contains__(self, item):
#        print("__contains__", item)
#        return item in self.__dict__
#
#    def __iter__(self):
#        print("__iter__")
#        return iter(self.__dict__)
#
#    def __unicode__(self):
#        print("__unicode__")
#        return unicode(repr(self.__dict__))
#
