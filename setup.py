from datetime import datetime
from setuptools import setup, find_packages

__version__ = datetime.today().strftime('%y.%m.%d')


setup(
    name='zpz',
    version=__version__,
    description='Python utilities',
    packages=find_packages('src/'),
    include_package_data=True,
    python_requires='>=3.7',
    # install_requires=[
    #     'arrow',
    #     'avro-python3==1.*',
    #     'boltons>=20',
    #     'orjson>=3',
    #     'pytz>=2019',
    #     'tenacity>=6',
    #     'toolz>=0.11',
    # ],
    # extras_require={
    #     'web': [
    #         'httpx>=0.15.5',
    #         'python-multipart==0.0.5',
    #         'starlette>=0.13.8',
    #         'uvicorn>=0.12.1',
    #     ],
    #     'ml': [
    #         'numpy>=1.19',
    #         'pandas>=1,<2',
    #         'scikit-learn>=0.23.2',
    #     ],
    #     'db': [
    #         'mysqlclient==2.*',
    #         'aioodbc>=0.3.3',
    #         'pyodbc==4.*',
    #     ],
    # },
)
