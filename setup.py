import os
import sys
import subprocess
import shutil
import numpy as np
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
from wheel.bdist_wheel import bdist_wheel

current_dir = os.path.dirname(os.path.abspath(__file__))

class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        bdist_wheel.finalize_options(self)
        # Set platform tag based on OS and architecture
        if sys.platform == 'darwin':
            arch = os.environ.get('GOARCH', 'arm64')
            self.plat_name = f"macosx_11_0_{arch}"
        elif sys.platform == 'win32':
            self.plat_name = "win_amd64"

class CustomBuildExt(build_ext):
    def build_extension(self, ext):
        if sys.platform == 'darwin':
            ext.extra_compile_args = ['-mmacosx-version-min=11.0']
            # Add rpath to look in the same directory as the extension
            ext.extra_link_args = [
                '-mmacosx-version-min=11.0',
                '-Wl,-rpath,@loader_path',
                '-L.',
                '-lgo_polars'
            ]
            # Don't use -l flag on macOS
            ext.libraries = []
        elif sys.platform == 'win32':
            ext.libraries = []  # Windows uses .dll directly
        super().build_extension(ext)

    def run(self):
        # Build the Go shared library
        print("Building Go shared library...")
        go_env = os.environ.copy()
        go_env['CGO_ENABLED'] = '1'

        if sys.platform == 'darwin':
            go_env['GOOS'] = 'darwin'
            arch = os.environ.get('GOARCH', 'arm64')
            go_env['GOARCH'] = arch
            if arch == 'arm64':
                go_env['CGO_CFLAGS'] = '-mmacosx-version-min=11.0'
                go_env['CGO_LDFLAGS'] = '-mmacosx-version-min=11.0'
            lib_name = 'libgo_polars.dylib'
            subprocess.check_call(['go', 'build', '-buildmode=c-shared',
                                '-o', lib_name, './bridge/bridge.go'],
                                env=go_env)
            
            # Create the package directory and copy the shared library
            os.makedirs('go_polars', exist_ok=True)
            shutil.copy2(lib_name, 'go_polars/')
            shutil.copy2('libgo_polars.h', 'go_polars/')
            
            # Set the install name in the copied library
            subprocess.check_call(['install_name_tool', '-id', '@rpath/libgo_polars.dylib',
                                os.path.join('go_polars', lib_name)])
            
            # Build the Python extension
            build_ext.run(self)
            
            # Fix the library reference in the extension
            ext_path = os.path.join(self.build_lib, 'go_polars', '_go_polars.cpython-313-darwin.so')
            subprocess.check_call(['install_name_tool', '-change', 'libgo_polars.dylib',
                                '@rpath/libgo_polars.dylib', ext_path])
            
            # Copy the dylib to the build directory
            build_lib = os.path.join(self.build_lib, 'go_polars')
            os.makedirs(build_lib, exist_ok=True)
            shutil.copy2(os.path.join('go_polars', lib_name), build_lib)
            
            # Set the install name in the final dylib
            subprocess.check_call(['install_name_tool', '-id', '@rpath/libgo_polars.dylib',
                                os.path.join(build_lib, lib_name)])
            
        elif sys.platform == 'win32':
            go_env['GOOS'] = 'windows'
            go_env['GOARCH'] = 'amd64'
            lib_name = 'go_polars.dll'
            subprocess.check_call(['go', 'build', '-buildmode=c-shared',
                                '-o', lib_name, './bridge/bridge.go'],
                                env=go_env)
            os.makedirs('go_polars', exist_ok=True)
            shutil.copy2(lib_name, 'go_polars/')
            shutil.copy2('libgo_polars.h', 'go_polars/')
            build_ext.run(self)
        else:  # Linux
            go_env['GOOS'] = 'linux'
            go_env['GOARCH'] = 'amd64'
            lib_name = 'libgo_polars.so'
            subprocess.check_call(['go', 'build', '-buildmode=c-shared',
                                '-o', lib_name, './bridge/bridge.go'],
                                env=go_env)
            os.makedirs('go_polars', exist_ok=True)
            shutil.copy2(lib_name, 'go_polars/')
            shutil.copy2('libgo_polars.h', 'go_polars/')
            build_ext.run(self)

# Read the contents of README.md
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

# Platform-specific package data extensions
if sys.platform == 'win32':
    package_data = {'go_polars': ['*.dll', '*.h']}
elif sys.platform == 'darwin':
    package_data = {'go_polars': ['*.dylib', '*.h', 'libgo_polars.dylib']}
else:
    package_data = {'go_polars': ['*.so', '*.h']}

# Platform-specific extension module settings
ext_kwargs = {
    'include_dirs': ['./bridge', np.get_include(), 'go_polars'],
    'library_dirs': [os.path.join(current_dir, 'go_polars')],
}

if sys.platform == 'win32':
    ext_kwargs['libraries'] = []  # Windows uses .dll directly
else:
    ext_kwargs['libraries'] = []
    if sys.platform == 'darwin':
        ext_kwargs['extra_link_args'] = ['-Wl,-rpath,@loader_path/.']
    else:
        ext_kwargs['runtime_library_dirs'] = [os.path.join(current_dir, 'go_polars')]
        ext_kwargs['extra_link_args'] = ['-Wl,-rpath,' + os.path.join(current_dir, 'go_polars')]

setup(
    name='go-polars',
    version='0.1.13',
    description='A high-performance DataFrame library for Python powered by Go',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/yourusername/go-polars',
    packages=['go_polars'],
    package_data=package_data,
    ext_modules=[Extension(
        'go_polars._go_polars',
        ['go_polars/_go_polars.c'],
        **ext_kwargs
    )],
    cmdclass={
        'build_ext': CustomBuildExt,
        'bdist_wheel': CustomBdistWheel,
    },
    install_requires=['numpy>=1.20.0'],
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Go',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
    ],
) 