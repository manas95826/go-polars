from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
import os
import sys
import subprocess
import numpy as np
import shutil

class CustomBuildExt(build_ext):
    def build_extensions(self):
        # Ensure Go is installed
        try:
            subprocess.check_output(['go', 'version'])
        except:
            raise RuntimeError('Go must be installed to build this package')
        
        # Build the Go part
        try:
            subprocess.check_call(['go', 'build', '-buildmode=c-shared', '-o', 'gopolars.so', './bridge/bridge.go'])
            # Copy the shared library to the package directory
            os.makedirs('gopolars', exist_ok=True)
            shutil.copy2('gopolars.so', 'gopolars/')
            shutil.copy2('gopolars.h', 'gopolars/')
        except subprocess.CalledProcessError as e:
            print(f"Go build failed: {e}")
            raise RuntimeError('Failed to build Go shared library')
            
        super().build_extensions()

# Read README.md
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

# Get the absolute path to the current directory
current_dir = os.path.abspath(os.path.dirname(__file__))

setup(
    name='polars-go',
    version='0.1.0',
    description='High-performance DataFrame library with Go backend',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Manas Chopra',
    author_email='manaschopra@example.com',  # Replace with your email
    url='https://github.com/manaschopra/go-polars',
    packages=['gopolars'],
    package_data={
        'gopolars': ['*.so', '*.h'],
    },
    ext_modules=[
        Extension(
            'gopolars._gopolars',
            ['gopolars/_gopolars.c'],
            include_dirs=['./bridge', np.get_include(), 'gopolars'],
            library_dirs=[os.path.join(current_dir, 'gopolars')],
            libraries=['gopolars'],
            extra_compile_args=['-std=c99'],
            extra_link_args=['-Wl,-rpath,' + os.path.join(current_dir, 'gopolars')] if sys.platform != 'darwin' else None,
        )
    ],
    cmdclass={
        'build_ext': CustomBuildExt,
    },
    install_requires=[
        'numpy>=1.20.0',
    ],
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Go',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='dataframe, data analysis, go, performance',
) 