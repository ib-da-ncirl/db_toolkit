# db_toolkit

**db_toolkit** provides some utility functions/classes to help work with databases in Python.

It is _definitely_ a work-in-progress.

Current functionality includes:

* CosmosDb

    Class to work with Azure Cosmo DB. 
       
* PostgresDb

    Class to work with PostgreSQL.
    
* MongoDb

    Class to work with MongoDb.

    **Note:**
    If utilising the insert_many() method of pymongo.collection.Collection with an Azure Cosmos DB for MongoDb API 
    server, consider using the MongoDb.insert_many() method instead, as it will attempt to continue in slower batch mode 
    in the event of the throughput (RU/s) being exceeded and a BulkWriteError being raised.
    
Connection parameters may be specified during object creation, or via a configuration file.
See [cosmos_cfg.sample](db_toolkit/docs/cosmos_cfg.sample), [postgres_cfg.sample](db_toolkit/docs/postgres_cfg.sample) 
or [mongo_cfg.sample](db_toolkit/docs/mongo_cfg.sample). If using a YAML configuration file
with an application, see [sample.yaml](db_toolkit/docs/sample.yaml).
    
## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

Install dependencies via

    pip install -r requirements.txt



    
## Acknowledgements

Package layout inspired by https://github.com/bast/somepackage
