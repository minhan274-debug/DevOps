# User manuals

This repository was created to store code and usage instructions based on requirements:
    Currently, the volume of the histories table in the database is increasing day by day, leading to an increase in the monthly bill. These histories tables are seldom accessed. Therefore, migrating that data to S3 and using Athena to query it when needed not only helps optimize storage but also leads to up to 90% savings on the monthly bill.
## Installation

Please ensure that your server has Python 3 and pip3 installed, and install the following packages to run:

```python
pip3 install psycopg2
pip3 install boto3
pip3 install pyyaml
```

## Usage

```python
fix me
```

## Contributing

Pull requests are welcome. For any changes, please open an issue first to discuss what you want changed.

Make sure your code runs correctly before running it on the Prod environment

## License

[DevOps]