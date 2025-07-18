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
        # Force the platform tag to be macosx_11_0_arm64
        if sys.platform == 'darwin':
            self.plat_name = "macosx_11_0_arm64"

class CustomBuildExt(build_ext):
    def build_extension(self, ext):
        if sys.platform == 'darwin':
            ext.extra_compile_args = ['-mmacosx-version-min=11.0']
            ext.extra_link_args = ['-mmacosx-version-min=11.0']
        super().build_extension(ext)

    def run(self):
        # Build the Go shared library
        print("Building Go shared library...")
        if sys.platform == 'darwin':
            os.environ['CGO_ENABLED'] = '1'
            os.environ['GOOS'] = 'darwin'
            os.environ['GOARCH'] = 'arm64'
            os.environ['CGO_CFLAGS'] = '-mmacosx-version-min=11.0'
            os.environ['CGO_LDFLAGS'] = '-mmacosx-version-min=11.0'
        subprocess.check_call(['go', 'build', '-buildmode=c-shared',
                             '-o', 'libgo_polars.so', './bridge/bridge.go'])
        
        # Create the package directory and copy the shared library
        os.makedirs('go_polars', exist_ok=True)
        shutil.copy2('libgo_polars.so', 'go_polars/')
        shutil.copy2('libgo_polars.h', 'go_polars/')
        
        # Build the Python extension
        build_ext.run(self)

# Read the contents of README.md
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='go-polars',
    version='0.1.1',
    description='A high-performance DataFrame library for Python powered by Go',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/yourusername/go-polars',
    packages=['go_polars'],
    package_data={
        'go_polars': ['*.so', '*.h'],
    },
    ext_modules=[Extension(
        'go_polars._go_polars',
        ['go_polars/_go_polars.c'],
        include_dirs=['./bridge', np.get_include(), 'go_polars'],
        library_dirs=[os.path.join(current_dir, 'go_polars')],
        libraries=['go_polars'],
        runtime_library_dirs=[os.path.join(current_dir, 'go_polars')] if sys.platform != 'darwin' else None,
        extra_link_args=['-Wl,-rpath,' + os.path.join(current_dir, 'go_polars')] if sys.platform != 'darwin' else None,
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
        'Programming Language :: Go',
    ],
) 