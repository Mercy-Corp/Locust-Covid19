import os
import glob
import shutil
from setuptools import setup, Command


class CompleteClean(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        shutil.rmtree('./build', ignore_errors=True)
        shutil.rmtree('./dist', ignore_errors=True)
#        shutil.rmtree('./' + project + '.egg-info', ignore_errors=True)
        temporal = glob.glob('./' + project + '/*.pyc')
        for t in temporal:
            os.remove(t)


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    project = 'locustcovid19'

setup(
    name=project,
    python_requires='>=3.7',
    version="1.0.0",   # use semantic versioning. See https://semver.org/ 
    description="template for python projects",
    long_description=read('README.md'),
    url='',
    packages=[project, 'tests'],
    install_requires=['python>=3.7'],
    cmdclass={'clean': CompleteClean},
    test_suite='nose.collector'
)
