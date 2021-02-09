# CcfSpark version 0.3

A little CCF implementation in a Spark context with networkx.

## Installation 

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install.

```bash
pip install ccf_spark
```

## Usage

```python
import ccf_spark

sc = SparkContext()
p = ccf_spark.CcfSpark(
    sc,
    secondary_sorting=True,
)
p.iterate_all()
print(p.print())
```
Or see example.py 

## Authors
- Théo Chennebault
- Louis Ledeoux

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)