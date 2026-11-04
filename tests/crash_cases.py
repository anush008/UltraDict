#
# Cases that crashed the process with a segmentation fault in earlier versions.
# tests/tests.py runs each case in its own process, so a crash fails one test only.
#
# Usage: python tests/crash_cases.py <case>
#
# Each case prints "ok: ..." if UltraDict raised an exception instead of crashing.
#

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from UltraDict import UltraDict

reader = None

class Closer:
    """ Closes the reader while the reader replays the stream and unpickles this object """
    def __init__(self):
        self.x = 1
    def __setstate__(self, state):
        self.__dict__.update(state)
        if reader is not None and not reader.closed:
            reader.close()

def close_during_replay():
    global reader
    writer = UltraDict(buffer_size=100_000)
    reader = UltraDict(name=writer.name)
    writer['a'] = Closer()
    # A second record, so the replay continues after the reader is closed
    writer['b'] = 2
    try:
        reader.get('b')
    except Exception as e: # pylint: disable=broad-except
        print(f"ok: {type(e).__name__}")
        return
    print("ok: no exception")

def bad_record_length():
    module = sys.modules['UltraDict.UltraDict']
    writer = UltraDict(buffer_size=10_000)
    reader = UltraDict(name=writer.name)
    # A record header with a length of 2**31, which is negative as a signed 32 bit value
    start = writer.update_stream_position_remote[0]
    module.HEADER.pack_into(writer.buffer.buf, start, module.MARKER_MARSHAL, 2**31, 0, 0xFF)
    writer.update_stream_position_remote[0] = start + module.HEADER_SIZE
    try:
        reader.get('a')
    except AssertionError as e:
        print(f"ok: {type(e).__name__}")
        return
    print("FAIL: the bad record was accepted")

def exchange_after_close():
    d = UltraDict(shared_lock=True)
    exchange = d.lock.exchange
    d.close()
    try:
        exchange(1)
    except Exception as e: # pylint: disable=broad-except
        print(f"ok: {type(e).__name__}")
        return
    print("FAIL: exchange() worked after close()")

if __name__ == '__main__':
    globals()[sys.argv[1]]()
