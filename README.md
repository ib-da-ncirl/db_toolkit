# db_toolkit

**db_toolkit** provides some utility functions/classes to help work with databases in Python.

It is _definitely_ a work-in-progress.

Current functionality includes:

* CosmosDb

    Class to work with Azure Cosmo DB. 
    
    Connection parameters may be specified during object creation, or via a configuration file.
    See [cosmos_cfg.sample](db_toolkit/docs/cosmos_cfg.sample).
    
* PostgresDb

    Class to work with PostgreSQL.
    
    Connection parameters may be specified during object creation, or via a configuration file.
    See [postgres_cfg.sample](db_toolkit/docs/postgres_cfg.sample).

* MongoDb

    Class to work with MongoDb.
    
    Connection parameters may be specified during object creation, or via a configuration file.
    See [mongo_cfg.sample](db_toolkit/docs/mongo_cfg.sample).
    
## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

Install dependencies via

    pip install -r requirements.txt



    
## Acknowledgements

Package layout inspired by https://github.com/bast/somepackage
