from distutils.core import setup
from os import path

# dir = path.abspath(path.dirname(__file__))
# with open(path.join(dir, 'README.md'), encoding='utf-8') as f:
#     long_description = f.read()

setup(
    name='ccf_spark',
    version='0.1.2',
    description='A little CCF implementation in a Spark context with networkx',
    # long_description=long_description,
    # long_description_content_type=text/markdown,
    author='Théo Chennebault',
    author_email='theo.chennebault@le-cab-politique.fr',
    url='https://github.com/ErnestBidouille/ccf-spark',
    packages=[
        'ccf_spark',
    ],
    install_requires=['networkx==2.5', 'pyspark==3.0.1'])
