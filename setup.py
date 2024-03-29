#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='jeffTest',
    version='0.2.0',
    packages=find_packages(),
    install_requires=[
        'dill==0.3.0',
        'ibm-cos-sdk==2.1.3',
        'numpy==1.22.0',
        'pandas>=0.24.0',
        'scikit-learn==0.20.3',
        'scipy>=1.1.0',
        'requests==2.20.0',
        'urllib3==1.26.5',
        'ibm_db==3.0.1',
        'ibm_db_sa==0.3.3',
        'lxml==4.6.5',
        'nose>=1.3.7',
        'psycopg2-binary==2.8.4',
        'pyod==0.7.5',
        'scikit-image==0.16.2',
        'sqlalchemy==1.3.10',
        'tabulate==0.8.5'
    ],
    extras_require = {
        'kafka':  ['confluent-kafka==0.11.5']
    }
)
