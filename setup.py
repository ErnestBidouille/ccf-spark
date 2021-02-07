from distutils.core import setup
from os import path

dir = path.abspath(path.dirname(__file__))
with open(path.join(dir, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='CcfSpark',
    version='1.0',
    description='A little CCF implementation in a Spark context with networkx',
    long_description=long_description,
    author=['Théo Chennebault', 'Louis Ledoux'],
    author_email='theo.chennebault@le-cab-politique.fr',
    url='https://www.python.org/sigs/distutils-sig/',
    packages=[
        'ccf_spark',
    ],
    install_requires=['networkx==2.5', 'pyspark==3.0.1'])
