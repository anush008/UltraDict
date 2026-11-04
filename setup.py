import os, sys
from pathlib import Path
from setuptools import setup, Extension
from setuptools.command.build_py import build_py
import Cython.Build

# read the contents of your README file
this_directory = Path(__file__).parent
long_description = (this_directory / "readme.md").read_text()

version = '0.1.0'

# Optional C implementation of the hot paths, UltraDict.py works without it
ext_fast = Extension(name="UltraDict._fast", sources=["_fast.pyx"], include_dirs=["."], depends=["_fast_atomic.h"])

# The extension uses CPython internals. PyPy uses cffi, other implementations the pure Python code.
ext_modules = Cython.Build.cythonize([ext_fast], compiler_directives={'language_level' : "3"}) \
    if sys.implementation.name == 'cpython' else []

class optional_build_ext(Cython.Build.build_ext):
    """
    Install UltraDict without the C extension if it cannot be built, e.g. without a C compiler.
    UltraDict then uses the pure Python implementation. Set ULTRADICT_REQUIRE_C_EXTENSION=1
    to stop with an error instead, e.g. when building wheels.
    """
    def run(self):
        try:
            super().run()
        except Exception as e: # pylint: disable=broad-except
            self.skip(e)

    def build_extension(self, ext):
        try:
            super().build_extension(ext)
        except Exception as e: # pylint: disable=broad-except
            self.skip(e)

    @staticmethod
    def skip(error):
        if os.environ.get('ULTRADICT_REQUIRE_C_EXTENSION'):
            raise error
        print(f"WARNING: cannot build the UltraDict C extension ({error}). "
              "UltraDict uses the pure Python implementation, which is slower.", file=sys.stderr)

class build_py_without_setup(build_py):
    """ The package directory is the project directory, so leave this file out of the package """
    def find_package_modules(self, package, package_dir):
        return [module for module in super().find_package_modules(package, package_dir) if module[1] != 'setup']

setup(
    name='UltraDict',
    version=version,
    description='Synchronized, streaming dictionary that uses shared memory as a backend',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Ronny Rentner',
    author_email='ultradict.code@ronny-rentner.de',
    url='https://github.com/ronny-rentner/UltraDict',
    cmdclass={'build_ext': optional_build_ext, 'build_py': build_py_without_setup},
    package_dir={'UltraDict': '.'},
    packages=['UltraDict'],
    zip_safe=False,
    ext_modules=ext_modules,
    # atomics: `shared_lock=True` without the C extension. psutil: `steal_from_dead()` of the shared lock.
    install_requires=['atomics', 'psutil'],
    python_requires=">=3.11",
)
