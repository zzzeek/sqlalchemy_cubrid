import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_cubrid', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.rst')


setup(name='sqlalchemy_cubrid',
      version=VERSION,
      description="Cubrid Support for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
      'Development Status :: 4 - Beta',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Programming Language :: Python :: Implementation :: PyPy',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='SQLAlchemy Cubrid',
      author='Ovidiu Veliscu',
      author_email='ovidiu.veliscu@cubrid.org',
      license='MIT',
      packages=['sqlalchemy_cubrid'],
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'cubrid.cubriddb = sqlalchemy_cubrid.cubriddb:CubridDialect_cubriddb',
              'cubrid.cubriddb = sqlalchemy_cubrid.cubriddb:CubridDialect_cubriddb',
              ]
        }
)
