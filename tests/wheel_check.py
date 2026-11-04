#
# Checks an installed UltraDict wheel, see .github/workflows/wheels.yml.
#
# Run it from outside the project directory, so Python imports the installed package:
# python tests/wheel_check.py
#
import subprocess, sys
from UltraDict import UltraDict

module = sys.modules['UltraDict.UltraDict']
if sys.implementation.name == 'cpython' and not module.ACCELERATED:
    sys.exit(f"FAIL: the wheel has no working C extension, UltraDict is at {module.__file__}")
if sys.implementation.name == 'pypy' and not module.PYPY_CFFI:
    sys.exit("FAIL: PyPy does not use cffi")

# The C extension does not need the atomics package for the shared lock
ultra = UltraDict(shared_lock=module.ACCELERATED, recurse=True)
ultra['simple'] = 1
ultra['complex'] = [1, 'a']
ultra['nested'] = {'inner': {'value': 1}}

# A second process reads all values and changes some of them
child = (
    "from UltraDict import UltraDict\n"
    f"d = UltraDict(name={ultra.name!r})\n"
    "assert d['simple'] == 1 and d['complex'] == [1, 'a'] and d['nested']['inner']['value'] == 1\n"
    "d['simple'] += 1\n"
    "d['nested']['inner']['value'] = 2\n"
    "del d['complex']\n"
)
subprocess.run([sys.executable, '-c', child], check=True)

assert ultra['simple'] == 2, ultra
assert ultra['nested']['inner']['value'] == 2, ultra
assert 'complex' not in ultra, ultra
ultra.unlink()

print(f"ok: {sys.implementation.name} {sys.version.split()[0]}, C extension {module.ACCELERATED}, cffi {module.PYPY_CFFI}")
