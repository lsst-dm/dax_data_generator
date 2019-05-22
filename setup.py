
import setuptools
from setuptools import setup, Extension

setup(name='lsst_dax_data_generator',
      version='1.0',
      # Include package lsst to copy lsst/__init__.py
      # That will probably break something to have multiple packages editing that file.
      packages=['lsst', 'lsst.dax', 'lsst.dax.data_generator'],
      package_dir={'': "python/"},
      )


