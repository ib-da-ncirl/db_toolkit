# db_toolkit

**db_toolkit** provides some utility functions/classes to help work with databases in Python.

It is _definitely_ a work-in-progress.

Current functionality includes:

* CosmosDb

    Class to work with Azure Cosmo DB. 
    
    Connection parameters may be specified during object creation, or via a configuration file.
    See [cosmos_cfg.sample](cosmos_cfg.sample).
    
* PostgresDb

    Class to work with PostgreSQL.
    
    Connection parameters may be specified during object creation, or via a configuration file.
    See [postgres_cfg.sample](postgres_cfg.sample).

* MongoDb

    Class to work with MongoDb.
    
    Connection parameters may be specified during object creation, or via a configuration file.
    See [mongo_cfg.sample](mongo_cfg.sample).
    
## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

**db_toolkit** is currently available on Test PyPI https://test.pypi.org/project/db-toolkit/



    
## Acknowledgements

Package layout inspired by https://github.com/bast/somepackage
