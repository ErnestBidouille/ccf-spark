from setuptools import setup, find_packages
from os import path

dir = path.abspath(path.dirname(__file__))
with open(path.join(dir, 'README.md'), encoding='utf-8') as f:
    README = f.read()

setup(
    name='ccf_spark',
    version='0.3.1.1',
    description='A little CCF implementation in a Spark context with networkx',
    license='MIT',
    long_description_content_type='text/markdown',
    long_description=README,
    author='Théo Chennebault',
    author_email='theo.chennebault@le-cab-politique.fr',
    url='https://github.com/ErnestBidouille/ccf-spark',
    packages=find_packages(exclude=['tests*']),
    install_requires=['networkx==2.5', 'pyspark==3.0.1'],
)
