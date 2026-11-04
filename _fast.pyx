# cython: language_level=3
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

"""
Optional C implementation of the hot paths of UltraDict.

UltraDict.py works without this module. If it is available, `UltraDict` and
`UltraDict.SharedLock` use the classes of this module as base classes. The shared
memory layout and the record format are the same in both modes, so processes with and
without this module can share the same UltraDict.

Set the environment variable ULTRADICT_PURE_PYTHON=1 to disable this module.
"""

from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_WRITABLE, PyBUF_SIMPLE
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.string cimport memcpy, memcmp, memset
from libc.stdint cimport uint32_t, uint64_t, int32_t

cdef extern from "Python.h":
    bint PyUnicode_IS_ASCII(object o)
    void *PyUnicode_DATA(object o)
    Py_ssize_t PyUnicode_GET_LENGTH(object o)
    const char *PyUnicode_AsUTF8AndSize(object o, Py_ssize_t *size) except NULL
    double PyFloat_AS_DOUBLE(object o)
    long PyLong_AsLongAndOverflow(object o, int *overflow) except? -1
    char *PyBytes_AS_STRING(object o)
    Py_ssize_t PyBytes_GET_SIZE(object o)

cdef extern from "marshal.h":
    object PyMarshal_WriteObjectToString(object value, int version)
    object PyMarshal_ReadObjectFromString(const char *data, Py_ssize_t length)

cdef extern from "_fast_atomic.h":
    unsigned char ud_exchange_u8(unsigned char *p, unsigned char v) nogil
    uint32_t ud_load_acquire_u32(uint32_t *p) nogil
    void ud_store_release_u32(uint32_t *p, uint32_t v) nogil
    void ud_fence_acquire() nogil
    void ud_fence_full() nogil
    int ud_cmpxchg_u32(uint32_t *p, uint32_t expected, uint32_t desired) nogil
    void ud_crc32_init() nogil
    uint32_t ud_crc32(uint32_t seed, const unsigned char *p, size_t n) nogil

# UltraDict.py only uses this module if the version matches
VERSION = 3

ud_crc32_init()

def fence():
    """
    Full memory barrier. Loads and stores before it are not reordered with loads and stores
    after it. UltraDict.py uses it around full dumps, which are written in Python.
    """
    ud_fence_full()

# Header of a stream record: marker byte, 4 bytes length, 4 bytes CRC32 of the body, FF byte.
# The CRC32 is seeded with the full dump counter, see UltraDict.py.
cdef enum:
    HEADER_SIZE = 10
    MARKER_SERIALIZER = 0xFF
    MARKER_MARSHAL = 0xFE
    MARSHAL_VERSION = 4
    # Offsets in the control memory, see UltraDict.init_remotes()
    OFFSET_STREAM_POSITION = 0
    OFFSET_FULL_DUMP_COUNTER = 10
    CONTROL_MIN_SIZE = 14

cdef inline bint is_simple(object o):
    """ Same check as `type(o) in MARSHAL_TYPES` in UltraDict.py """
    t = type(o)
    return t is int or t is str or t is float or t is bool or t is bytes or o is None

cdef inline uint32_t read_u32_le(const unsigned char *p) noexcept nogil:
    return (<uint32_t>p[0]) | (<uint32_t>p[1] << 8) | (<uint32_t>p[2] << 16) | (<uint32_t>p[3] << 24)

cdef inline void write_u32_le(unsigned char *p, uint32_t v) noexcept nogil:
    p[0] = v & 0xFF
    p[1] = (v >> 8) & 0xFF
    p[2] = (v >> 16) & 0xFF
    p[3] = (v >> 24) & 0xFF

# Records up to this size are copied to the stack instead of a new bytes object
cdef enum:
    STACK_RECORD_SIZE = 512

# Type codes of the marshal format, see Python/marshal.c
cdef enum:
    M_SMALL_TUPLE = 0x29  # ')'
    M_TRUE = 0x54         # 'T'
    M_FALSE = 0x46        # 'F'
    M_NONE = 0x4E         # 'N'
    M_INT = 0x69          # 'i'
    M_BINARY_FLOAT = 0x67 # 'g'
    M_SHORT_ASCII = 0x7A  # 'z'
    M_ASCII = 0x61        # 'a'
    M_UNICODE = 0x75      # 'u'
    M_STRING = 0x73       # 's'

cdef Py_ssize_t marshal_size(object o):
    """
    Size of `o` in the marshal format as written by `marshal_write()`,
    or -1 if `marshal_write()` cannot write it. Only for simple types, see `is_simple()`.
    """
    cdef int overflow = 0
    cdef long v
    cdef Py_ssize_t n
    t = type(o)
    if t is bool or o is None:
        return 1
    if t is int:
        v = PyLong_AsLongAndOverflow(o, &overflow)
        if overflow or v < -2147483648 or v > 2147483647:
            return -1
        return 5
    if t is float:
        return 9
    if t is str:
        if PyUnicode_IS_ASCII(o):
            n = PyUnicode_GET_LENGTH(o)
            return 2 + n if n < 256 else 5 + n
        try:
            PyUnicode_AsUTF8AndSize(o, &n)
        except UnicodeError:
            # Lone surrogates, marshal itself can write them
            return -1
        return 5 + n
    if t is bytes:
        return 5 + PyBytes_GET_SIZE(o)
    return -1

cdef unsigned char *marshal_write(object o, unsigned char *p) except NULL:
    """ Write `o` in the marshal format. `marshal_size()` must have returned the size before. """
    cdef int overflow = 0
    cdef double d
    cdef uint64_t bits
    cdef Py_ssize_t n
    cdef const char *utf8
    cdef int i
    t = type(o)
    if t is bool:
        p[0] = M_TRUE if o else M_FALSE
        return p + 1
    if o is None:
        p[0] = M_NONE
        return p + 1
    if t is int:
        p[0] = M_INT
        write_u32_le(p + 1, <uint32_t><int32_t>PyLong_AsLongAndOverflow(o, &overflow))
        return p + 5
    if t is float:
        p[0] = M_BINARY_FLOAT
        d = PyFloat_AS_DOUBLE(o)
        memcpy(&bits, &d, 8)
        for i in range(8):
            p[1 + i] = (bits >> (8 * i)) & 0xFF
        return p + 9
    if t is str:
        if PyUnicode_IS_ASCII(o):
            n = PyUnicode_GET_LENGTH(o)
            if n < 256:
                p[0] = M_SHORT_ASCII
                p[1] = <unsigned char>n
                p += 2
            else:
                p[0] = M_ASCII
                write_u32_le(p + 1, <uint32_t>n)
                p += 5
            memcpy(p, PyUnicode_DATA(o), n)
            return p + n
        utf8 = PyUnicode_AsUTF8AndSize(o, &n)
        p[0] = M_UNICODE
        write_u32_le(p + 1, <uint32_t>n)
        memcpy(p + 5, utf8, n)
        return p + 5 + n
    # bytes
    n = PyBytes_GET_SIZE(o)
    p[0] = M_STRING
    write_u32_le(p + 1, <uint32_t>n)
    memcpy(p + 5, PyBytes_AS_STRING(o), n)
    return p + 5 + n

cdef raise_no_attribute(object obj, str name):
    raise AttributeError(f"'{type(obj).__name__}' object has no attribute '{name}'")


cdef class SharedLockCore:
    """
    Base class of `UltraDict.SharedLock` with the hot paths in C.

    The lock is one byte in shared memory. The byte is 1 while a process holds the lock.
    The PID of the owner is stored in 4 bytes, little endian.
    """

    cdef unsigned char *lock_ptr
    cdef unsigned char *pid_ptr
    cdef Py_buffer lock_buffer
    cdef Py_buffer pid_buffer
    cdef bint bound
    cdef long long _pid
    cdef bint _has_pid
    cdef unsigned char pid_le[4]

    cdef public int has_lock
    cdef public object next_acquire_parameters

    def __dealloc__(self):
        self.release_buffers()

    cdef void release_buffers(self) noexcept:
        if self.bound:
            PyBuffer_Release(&self.lock_buffer)
            PyBuffer_Release(&self.pid_buffer)
            self.bound = False
        self.lock_ptr = NULL
        self.pid_ptr = NULL

    def _bind(self, lock_view, pid_view):
        """ Keep pointers to the lock byte and to the PID bytes in shared memory """
        self.release_buffers()
        PyObject_GetBuffer(lock_view, &self.lock_buffer, PyBUF_WRITABLE)
        try:
            PyObject_GetBuffer(pid_view, &self.pid_buffer, PyBUF_WRITABLE)
        except BaseException:
            PyBuffer_Release(&self.lock_buffer)
            raise
        self.bound = True
        if self.lock_buffer.len < 1 or self.pid_buffer.len < 4:
            self.release_buffers()
            raise ValueError("Shared memory for the lock is too small")
        self.lock_ptr = <unsigned char *>self.lock_buffer.buf
        self.pid_ptr = <unsigned char *>self.pid_buffer.buf

    def _unbind(self):
        """ Release the shared memory, it must not be used afterwards """
        self.release_buffers()

    @property
    def pid(self):
        if not self._has_pid:
            raise_no_attribute(self, 'pid')
        return self._pid

    @pid.setter
    def pid(self, value):
        cdef long long v = value
        write_u32_le(self.pid_le, <uint32_t>v)
        self._pid = v
        self._has_pid = True

    @pid.deleter
    def pid(self):
        self._has_pid = False

    cdef inline int check_bound(self, str name) except -1:
        if self.lock_ptr == NULL:
            raise_no_attribute(self, name)
        return 0

    def exchange(self, desired):
        """ Atomically set the lock byte to `desired` (0 or 1) and return the old value """
        self.check_bound('exchange')
        return ud_exchange_u8(self.lock_ptr, 1 if desired else 0)

    def compare_exchange_pid(self, expected, desired):
        """
        Atomically replace the owner PID with `desired` if it is `expected`.
        Both are 4 bytes, little endian, like `pid_remote`. Returns True on success.
        """
        cdef const unsigned char[:] expected_view = expected
        cdef const unsigned char[:] desired_view = desired
        cdef uint32_t expected_raw, desired_raw
        self.check_bound('pid_remote_atomic')
        if expected_view.shape[0] != 4 or desired_view.shape[0] != 4:
            raise ValueError("expected and desired must have 4 bytes")
        memcpy(&expected_raw, &expected_view[0], 4)
        memcpy(&desired_raw, &desired_view[0], 4)
        return ud_cmpxchg_u32(<uint32_t *>self.pid_ptr, expected_raw, desired_raw) == 1

    def test_and_inc(self):
        self.check_bound('exchange')
        if ud_exchange_u8(self.lock_ptr, 1) != 0:
            # Oops, someone else was faster than us
            return False
        return True

    def test_and_dec(self):
        self.check_bound('exchange')
        if ud_exchange_u8(self.lock_ptr, 0) != 1:
            raise Exception("Failed to release lock")
        return True

    cdef int c_enter(self) except -1:
        """ Same as `__enter__()` without the Python call overhead """
        params = self.next_acquire_parameters
        if params:
            self.acquire(*params)
            self.reset_acquire_parameters()
            return 0

        # If we already own the lock, just increment our counter
        if self.has_lock:
            self.has_lock += 1
            return 0

        # Fast path, the lock is free
        if self.lock_ptr != NULL and self._has_pid and ud_exchange_u8(self.lock_ptr, 1) == 0:
            self.has_lock = 1
            # If nobody had owned the lock, so the remote pid should be zero
            if memcmp(self.pid_ptr, b"\x00\x00\x00\x00", 4) != 0:
                raise AssertionError()
            memcpy(self.pid_ptr, self.pid_le, 4)
            return 0

        # Slow path with waiting, timeouts etc.
        self.acquire()
        return 0

    cdef object c_release(self):
        """ Same as `release()` without the Python call overhead """
        cdef uint32_t owner
        if self.has_lock > 0:
            if self.pid_ptr == NULL:
                raise_no_attribute(self, 'pid_remote')
            if not self._has_pid or memcmp(self.pid_ptr, self.pid_le, 4) != 0:
                owner = read_u32_le(self.pid_ptr)
                raise Exception(f"Our lock for pid {self.pid} was stolen by pid {owner}")
            self.has_lock -= 1
            # Last local lock released, release shared lock
            if not self.has_lock:
                memset(self.pid_ptr, 0, 4)
                if ud_exchange_u8(self.lock_ptr, 0) != 1:
                    raise Exception("Failed to release lock")
            return True
        return False

    def release(self, *args):
        return self.c_release()

    def __enter__(self):
        self.c_enter()
        return self

    def __exit__(self, type, value, traceback):
        self.c_release()
        # Make sure exceptions are not ignored
        return False


cdef class FastCore(dict):
    """
    Base class of `UltraDict` with the hot paths in C.

    Holds the local state that the hot paths need and direct pointers into the
    control memory and the stream buffer. All slow paths, like full dumps, call the
    Python methods of UltraDict.
    """

    cdef object _data
    cdef bint _has_data
    cdef object _fast_lock
    cdef bint _has_fast_lock
    cdef SharedLockCore _shared_lock
    cdef object _lock_acquire
    cdef object _lock_release

    cdef public Py_ssize_t update_stream_position
    cdef public Py_ssize_t full_dump_counter
    cdef public Py_ssize_t buffer_size
    cdef public object serializer
    cdef public bint use_marshal
    cdef public object recurse
    # True if `apply_update()` is the one of UltraDict, then the C code may replay the stream itself
    cdef public bint direct_replay

    cdef Py_buffer control_buffer
    cdef Py_buffer stream_buffer
    cdef bint bound
    cdef uint32_t *position_ptr
    cdef unsigned char *counter_ptr
    cdef unsigned char *stream_ptr
    cdef Py_ssize_t stream_length

    def __dealloc__(self):
        self.release_buffers()

    cdef void release_buffers(self) noexcept:
        if self.bound:
            PyBuffer_Release(&self.control_buffer)
            PyBuffer_Release(&self.stream_buffer)
            self.bound = False
        self.position_ptr = NULL
        self.counter_ptr = NULL
        self.stream_ptr = NULL
        self.stream_length = 0

    def _bind(self, control_view, stream_view):
        """ Keep pointers to the control memory and to the stream buffer """
        self.release_buffers()
        PyObject_GetBuffer(control_view, &self.control_buffer, PyBUF_WRITABLE)
        try:
            PyObject_GetBuffer(stream_view, &self.stream_buffer, PyBUF_WRITABLE)
        except BaseException:
            PyBuffer_Release(&self.control_buffer)
            raise
        self.bound = True
        if self.control_buffer.len < CONTROL_MIN_SIZE:
            self.release_buffers()
            raise ValueError("Control memory is too small")
        self.position_ptr = <uint32_t *>(<unsigned char *>self.control_buffer.buf + OFFSET_STREAM_POSITION)
        self.counter_ptr = <unsigned char *>self.control_buffer.buf + OFFSET_FULL_DUMP_COUNTER
        self.stream_ptr = <unsigned char *>self.stream_buffer.buf
        self.stream_length = self.stream_buffer.len

    def _unbind(self):
        """ Release the shared memory, it must not be used afterwards """
        self.release_buffers()

    @property
    def data(self):
        if not self._has_data:
            raise_no_attribute(self, 'data')
        return self._data

    @data.setter
    def data(self, value):
        self._data = value
        self._has_data = True

    @data.deleter
    def data(self):
        if not self._has_data:
            raise_no_attribute(self, 'data')
        self._data = None
        self._has_data = False

    @property
    def fast_lock(self):
        if not self._has_fast_lock:
            raise_no_attribute(self, 'fast_lock')
        return self._fast_lock

    @fast_lock.setter
    def fast_lock(self, value):
        self._fast_lock = value
        self._has_fast_lock = True
        self._shared_lock = None
        self._lock_acquire = None
        self._lock_release = None
        if isinstance(value, SharedLockCore):
            self._shared_lock = value
        elif hasattr(value, 'acquire') and hasattr(value, 'release'):
            self._lock_acquire = value.acquire
            self._lock_release = value.release

    @fast_lock.deleter
    def fast_lock(self):
        if not self._has_fast_lock:
            raise_no_attribute(self, 'fast_lock')
        self._fast_lock = None
        self._has_fast_lock = False
        self._shared_lock = None
        self._lock_acquire = None
        self._lock_release = None

    cdef inline bint changed(self) noexcept:
        """ True if the stream position or the full dump counter differ from ours """
        cdef uint32_t counter
        if self.position_ptr == NULL:
            return True
        if <Py_ssize_t>ud_load_acquire_u32(self.position_ptr) != self.update_stream_position:
            return True
        memcpy(&counter, self.counter_ptr, 4)
        return <Py_ssize_t>counter != self.full_dump_counter

    cdef inline int sync(self) except -1:
        """ Apply remote changes, same as `if changed: apply_update()` """
        if self.changed():
            self.slow_sync()
        return 0

    cdef int slow_sync(self) except -1:
        cdef uint32_t counter
        if self.position_ptr == NULL or not self.direct_replay:
            self.apply_update()
            return 0
        memcpy(&counter, self.counter_ptr, 4)
        if <Py_ssize_t>counter != self.full_dump_counter:
            # A new full dump, the Python code loads it
            self.apply_update()
            return 0
        if self.update_stream_position < <Py_ssize_t>ud_load_acquire_u32(self.position_ptr):
            try:
                if self.replay():
                    self.apply_update()
            except Exception:
                # The Python code handles all errors, it starts again at the record that failed
                self.apply_update()
        return 0

    cdef inline object get_data(self):
        if not self._has_data:
            raise_no_attribute(self, 'data')
        return self._data

    cdef inline int lock_enter(self) except -1:
        if self._shared_lock is not None:
            return self._shared_lock.c_enter()
        if not self._has_fast_lock:
            raise_no_attribute(self, 'lock')
        if self._lock_acquire is not None:
            self._lock_acquire()
        else:
            self._fast_lock.__enter__()
        return 0

    cdef inline int lock_exit(self) except -1:
        if self._shared_lock is not None:
            self._shared_lock.c_release()
        elif self._lock_release is not None:
            self._lock_release()
        else:
            self._fast_lock.__exit__(None, None, None)
        return 0

    cdef int append(self, int marker, object payload, object key, object item, bint delete) except -1:
        """
        Append an already serialized change to the stream. Same as `append_marshalled()`.

        The caller must hold the lock.
        """
        cdef Py_buffer payload_buffer
        cdef Py_ssize_t start, end, length
        cdef unsigned char *p

        if self.position_ptr == NULL:
            self.append_marshalled(marker, payload, key, item, delete)
            return 0

        PyObject_GetBuffer(payload, &payload_buffer, PyBUF_SIMPLE)
        try:
            length = payload_buffer.len
            start = self.position_ptr[0]
            end = start + HEADER_SIZE + length
            if end > self.buffer_size or end > self.stream_length:
                # Buffer is full, the Python code creates a full dump
                self.append_marshalled(marker, payload, key, item, delete)
                return 0

            # Write header and body
            p = self.stream_ptr + start
            memcpy(p + HEADER_SIZE, payload_buffer.buf, length)
            p[0] = <unsigned char>marker
            write_u32_le(p + 1, <uint32_t>length)
            write_u32_le(p + 5, ud_crc32(<uint32_t>self.full_dump_counter, <const unsigned char *>payload_buffer.buf, length))
            p[9] = 0xFF
        finally:
            PyBuffer_Release(&payload_buffer)

        # Inform others about it, only after the full record is written
        self.update_stream_position = end
        ud_store_release_u32(self.position_ptr, <uint32_t>end)
        return 0

    def replay_stream(self):
        """
        Apply all new records of the update stream to the local copy.

        Returns True if a new full dump appeared while reading. The caller must then load it.
        Raises AssertionError on a bad record. Same as `replay_stream_python()`.
        """
        if self.position_ptr == NULL:
            return self.replay_stream_python()
        return self.replay() == 1

    cdef int replay(self) except -1:
        """ See `replay_stream()`, returns 1 if a new full dump appeared """
        cdef Py_ssize_t pos = self.update_stream_position
        cdef Py_ssize_t start, length
        cdef unsigned char *p
        cdef unsigned char marker
        cdef uint32_t counter, checksum, actual
        cdef char local_record[STACK_RECORD_SIZE]

        data = self.get_data()
        loads = None

        while True:
            if pos >= <Py_ssize_t>ud_load_acquire_u32(self.position_ptr):
                break

            # Read header, see HEADER_SIZE
            if pos + HEADER_SIZE > self.stream_length:
                raise AssertionError(f"Bad header in update stream at position {pos}")
            p = self.stream_ptr + pos
            marker = p[0]
            length = read_u32_le(p + 1)
            checksum = read_u32_le(p + 5)
            if p[9] != 0xFF:
                raise AssertionError(f"Bad header in update stream at position {pos}")
            start = pos + HEADER_SIZE
            if start + length > self.stream_length:
                raise AssertionError(f"Bad header in update stream at position {pos}")

            # Copy the record, then check that no new full dump appeared meanwhile.
            # Writers only overwrite the stream after a new full dump, so the copy is valid.
            if marker == MARKER_MARSHAL and length <= STACK_RECORD_SIZE:
                memcpy(local_record, self.stream_ptr + start, length)
                record = None
            else:
                record = PyBytes_FromStringAndSize(<char *>self.stream_ptr + start, length)
            ud_fence_acquire()
            memcpy(&counter, self.counter_ptr, 4)
            if <Py_ssize_t>counter != self.full_dump_counter:
                return 1

            # Never parse a record that is not complete. A writer without the C extension on a
            # CPU with weak memory order can make the stream position visible before the record.
            if record is None:
                actual = ud_crc32(<uint32_t>self.full_dump_counter, <const unsigned char *>local_record, length)
            else:
                actual = ud_crc32(<uint32_t>self.full_dump_counter, <const unsigned char *>PyBytes_AS_STRING(record), length)
            if actual != checksum:
                raise AssertionError(f"Bad checksum in update stream at position {pos}")

            # Unserialize the update data, we expect a tuple of mode, key and value
            if marker == MARKER_MARSHAL:
                if record is None:
                    mode, key, value = PyMarshal_ReadObjectFromString(local_record, length)
                else:
                    mode, key, value = PyMarshal_ReadObjectFromString(PyBytes_AS_STRING(record), length)
            elif marker == MARKER_SERIALIZER:
                if loads is None:
                    loads = self.serializer.loads
                mode, key, value = loads(record)
            else:
                raise AssertionError(f"Bad header in update stream at position {pos}")

            if mode:
                data[key] = value
            else:
                del data[key]

            pos = start + length
            # Remember that we have applied the update
            self.update_stream_position = pos

        return 0

    cdef int append_direct(self, bint mode, object key, object item, Py_ssize_t length) except -1:
        """
        Write a change of simple types in the marshal format directly into the stream.
        `length` is the size from `marshal_size()`. The caller must hold the lock.
        """
        cdef Py_ssize_t start, end
        cdef unsigned char *p
        cdef unsigned char *body

        if self.position_ptr != NULL:
            start = self.position_ptr[0]
            end = start + HEADER_SIZE + length
            if end <= self.buffer_size and end <= self.stream_length:
                p = self.stream_ptr + start
                body = p + HEADER_SIZE
                body[0] = M_SMALL_TUPLE
                body[1] = 3
                body[2] = M_TRUE if mode else M_FALSE
                marshal_write(item, marshal_write(key, body + 3))
                p[0] = MARKER_MARSHAL
                write_u32_le(p + 1, <uint32_t>length)
                write_u32_le(p + 5, ud_crc32(<uint32_t>self.full_dump_counter, body, length))
                p[9] = 0xFF

                # Inform others about it, only after the full record is written
                self.update_stream_position = end
                ud_store_release_u32(self.position_ptr, <uint32_t>end)
                return 0

        # Buffer is full, the Python code creates a full dump
        payload = PyMarshal_WriteObjectToString((bool(mode), key, item), MARSHAL_VERSION)
        self.append_marshalled(MARKER_MARSHAL, payload, key, item, not mode)
        return 0

    def __getitem__(self, key):
        self.sync()
        return self.get_data()[key]

    def get(self, key, default=None):
        self.sync()
        data = self.get_data()
        if type(data) is dict:
            return (<dict>data).get(key, default)
        return data.get(key, default)

    def __contains__(self, key):
        self.sync()
        return key in self.get_data()

    def __len__(self):
        self.sync()
        return len(self.get_data())

    def __iter__(self):
        self.sync()
        return iter(self.get_data())

    def keys(self):
        self.sync()
        return self.get_data().keys()

    def values(self):
        self.sync()
        return self.get_data().values()

    def __setitem__(self, key, item):
        cdef int marker = 0
        cdef Py_ssize_t key_size, item_size, direct_size = -1

        if self.recurse:
            self.setitem_recurse(key, item)
            return

        # Serialize outside of the lock to keep the time holding the lock short.
        # Simple types are written directly into the stream later, see `append_direct()`.
        try:
            if self.use_marshal and is_simple(key) and is_simple(item):
                key_size = marshal_size(key)
                item_size = marshal_size(item)
                if key_size >= 0 and item_size >= 0:
                    direct_size = 3 + key_size + item_size
                else:
                    payload = PyMarshal_WriteObjectToString((True, key, item), MARSHAL_VERSION)
                    marker = MARKER_MARSHAL
            else:
                payload = self.serializer.dumps((True, key, item))
                marker = MARKER_SERIALIZER
        except Exception:
            self.check_change_after_error(False, key)
            raise

        self.lock_enter()
        try:
            self.sync()
            # Update our local copy
            # It's important for the integrity to do this first
            self.get_data()[key] = item
            # Append the update to the update stream
            if direct_size >= 0:
                self.append_direct(True, key, item, direct_size)
            else:
                self.append(marker, payload, key, item, False)
        finally:
            self.lock_exit()

    def __delitem__(self, key):
        cdef int marker = 0
        cdef Py_ssize_t key_size, direct_size = -1

        # Serialize outside of the lock to keep the time holding the lock short
        try:
            if self.use_marshal and is_simple(key):
                key_size = marshal_size(key)
                if key_size >= 0:
                    # The value None needs 1 byte
                    direct_size = 3 + key_size + 1
                else:
                    payload = PyMarshal_WriteObjectToString((False, key, None), MARSHAL_VERSION)
                    marker = MARKER_MARSHAL
            else:
                payload = self.serializer.dumps((False, key, None))
                marker = MARKER_SERIALIZER
        except Exception:
            self.check_change_after_error(True, key)
            raise

        self.lock_enter()
        try:
            self.sync()
            # Update our local copy
            del self.get_data()[key]
            if direct_size >= 0:
                self.append_direct(False, key, None, direct_size)
            else:
                self.append(marker, payload, key, None, True)
        finally:
            self.lock_exit()
