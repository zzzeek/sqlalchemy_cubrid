sqlalchemy_cubrid
=================

SQLAlchemy driver for the CUBRID database

Release notes
=================
* Tested with CUBRID 8.4.1.6004
* Supports each data type CUBRID Offers
* Supports getting column data
* Supports getting primary key constraints

Known Limitations
=================
* CUBRID Python Driver 8.4.1 does not support changing autocommit mode (fixed in 8.4.3 version)
* CUBRID cannot get the referred table/columns for a foreign key
* The CUBRID Python Driver 8.4.1 does not properly support collections and None values for auto-increment columns (will be fixed in next release)

ToDo
=================
* Test CUBRID 8.4.3 and 9.0 beta
* Test transactions using the new python driver
