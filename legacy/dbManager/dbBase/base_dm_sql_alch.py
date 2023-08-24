"""
This class provides functionality for managing a generic sql database
including:
    - MySQL
    - PostgreSQL
    #TODO: - SQLite

─────────────────────────────────────────────────────────────────────────────
Last modification: October 2021

@authors: Jerónimo Arenas García (jeronimo.arenas@uc3m.es)
          Saúl Blanco Fortes (sblanco@tsc.uc3m.es)
          Jesús Cid Sueiro (jcid@ing.uc3m.es)
          José Antonio Espinosa Melchor (joespino@pa.uc3m.es)

─────────────────────────────────────────────────────────────────────────────

Exports class BaseDMsql that can be used to derive new classes
for specific projects that may include table creation, data import,
etc for a particular project.

The base class provided in this file implements the following methods:
┌────────────────┐
│   CONNECTION   │
└────────────────┘
* __init__          : The constructor of the class
* connectDB         : Create connection to database
* disconnect        : Close connection to database
* __del__           : Cleanly closes the database

┌───────────────────┐
│   AUX FUNCTIONS   │
└───────────────────┘
* _conv_literal     : Transforms string into the correct format to insert in command
* _els_in_table     : Returns True if all elements exist in table
* _table_in_DB      : Returns True if all elements exist in table

┌──────────────────────────┐
│   DATABASE DESCRIPTION   │
└──────────────────────────┘
* describeDatabase  : Get some relevant information of database
* getDatabases      : Get the names of all non-template databases
* useDatabase       : Change used database

┌────────────────────────┐
│   TABLES DESCRIPTION   │
└────────────────────────┘
* getTableNames     : Gets the names of the tables in the database
* getTableShape     : Get number of rows and columns in the selected table
* getColumnNames    : Gets the names of the columns in a particular table
* describeTable     : Get some relevant information of table

┌───────────────────────┐
│   TABLES ALTERATION   │
└───────────────────────┘
* createDBtable     : Create new table in database
* deleteDBtables    : Deletes table(s) from database
* addTableColumn    : Adds a column at the end of table of the database
* dropTableColumn   : Removes column from table

┌───────────────┐
│   FUNCTIONS   │
└───────────────┘
* readDBtable       : Reads rows from table and returns a pandas dataframe
                      with the retrieved data
* readDBchunks      : Provides an iterator to read chunks of rows in table.
                      Each iteration returns a chunk of predefined max number of rows
                      to avoid stalling the sql server
* insertInTable     : Insert new records in Table. Input data comes as a list of tuples.
* deleteFromTable   : Delete records from Table. Conditions are given on columname and values
* setField          : Updates table records. Input data comes as a list of tuples.
* upsert            : Update or insert records in a table. Input data comes as panda df
                      If the record exists (according to primary key) data will be updated
                      If the record does not exist, new records will be created
* findDuplicated    : Get duplicated entries in table
* removeDuplicated  : Remove duplicated entries in table

┌────────────┐
│   EXPORT   │
└────────────┘
* exportTable       : Export a table from database either as pickle or excel file
* importTable       : Import data to a table from a file
* DBdump            : Creates dump of full database, or dump of selected tables

┌───────────┐
│   OTHER   │
└───────────┘
* execute           : Execute SQL command received as parameter
* executeSQLfile    : Execute SQL file

"""

import os
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm


def chunks(l, n):
    """
    Yields successive n-sized chunks from list l.
    """
    for i in range(0, len(l), n):
        yield l[i : i + n]


def str2lst(el):
    if isinstance(el, str):
        el = [el.strip() for el in el.split(",") if len(el.strip())]
        return el
    elif isinstance(el, list):
        return el
    return []


def conv_size(size_bytes):
    """ Converts byte size to human readable"""
    if size_bytes:
        for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
            if abs(size_bytes) < 1000:
                return f"{size_bytes:3.1f} ({unit}B)"
            size_bytes /= 1000
        return f"{size_bytes:.1f} (YB)"
    return "0 (B)"


class BaseDMsql(object):
    """
    Data manager base class.
    """

    # ┌────────────────┐
    # │   CONNECTION   │
    # └────────────────┘

    def __init__(
        self,
        dbconnector="postgres",
        dbname=None,
        dbhost=None,
        dbuser=None,
        dbpass=None,
        dbport=None,
    ):
        """
        Initializes a DataManager object

        Parameters
        ----------
            dbconnector: str
                Connector. Options: {postgres, mysql}
            dbname: str
                Name of the DB
            dbhost: str
                Server
            dbuser: str
                User
            dbpass: str
                Password
            dbport: str
                Port. Default: 5432
        """

        # Store paths to the main project folders and files
        self.dbname = dbname
        self.dbhost = dbhost
        self.dbuser = dbuser
        self.dbpass = dbpass
        self.dbport = dbport

        # Other class variables
        self.dbON = False  # Will switch to True when the db is connected.

        # Connector type
        self.dbconnector = dbconnector

        # Schema
        if dbconnector == "postgres":
            self.schema = "public"
        elif dbconnector == "mysql":
            self.schema = dbname

        self.connectDB()

    def connectDB(self):
        """
        Create connection to database
        """
        # Try connection
        try:

            self._url = f"{self.dbuser}:{self.dbpass}@{self.dbhost}"
            self._url += f":{self.dbport}" if self.dbport else ""
            self._url += f"/{self.dbname}" if self.dbname else ""

            if self.dbconnector == "postgres":
                self.engine = create_engine(f"postgresql+psycopg2://{self._url}")
            elif self.dbconnector == "mysql":
                print(self._url)
                self.engine = create_engine(f"mysql+pymysql://{self._url}")
            else:
                print("ERROR: Not a valid connector")
                return

            with self.engine.begin() as conn:
                db_version = conn.execute("SELECT version()").fetchone()[0]

            print("Database connection successful.")
            if self.dbconnector == "postgres":
                print(f"PostgreSQL database version: {db_version}")
            elif self.dbconnector == "mysql":
                print(f"MySQL database version: {db_version}")
            print(f"Database: '{self.dbname}'\n")

            self.dbON = True

        except Exception as e:
            print("ERROR connecting to the database")
            print(e)

    def disconnect(self):
        """
        Close connection to database
        When destroying the object, it is necessary to commit changes
        in the database and close the connection
        """
        try:
            print("Closing...")
            self.engine.dispose()
            self._url = None
            self.dbON = False
        except Exception as e:
            print("---- Error closing database")
            print("\n", e)

    def __del__(self):

        self.disconnect()

    # ┌───────────────────┐
    # │   AUX FUNCTIONS   │
    # └───────────────────┘

    def _conv_literal(self, string):
        """
        Transforms string into the correct format to insert in command
        Postgres format:
            - tb_name: "str"
            - tb_col: "str"
            - value: 'str'
        MySQL format:
            - tb_name: `str`
            - tb_col: `str`
            - value: 'str'
        """
        if self.dbconnector == "postgres":
            string = f'"{string}"'
        elif self.dbconnector == "mysql":
            string = f"`{string}`"
        return string

    def _els_in_table(self, els, tablename):
        """
        Returns True if all elements exist in table
        """
        els = set(str2lst(els))
        cols = self.getColumnNames(tablename)
        opt = [o for o in els if o not in cols if o != "*"]
        if opt:
            print(
                f"ERROR: Selected columns '{','.join(opt)}' not found in the selected table '{tablename}'"
            )
            return False
        return True

    def _table_in_DB(self, tablename):
        """
        Returns True if all elements exist in table
        """
        if tablename not in self.getTableNames():
            print(f"ERROR: Table '{tablename}' does not exist in database.")
            return False
        return True

    # ┌──────────────────────────┐
    # │   DATABASE DESCRIPTION   │
    # └──────────────────────────┘

    def describeDatabase(self):
        """
        Get some relevant information of database as a dictionary
            - DB name
            - DB size
            - Tables description (see `describeTable`)
        """

        # DB size
        if self.dbconnector == "postgres":
            # pg_size_pretty()
            sqlcmd = "SELECT pg_database_size(current_database()) AS total_db_size"
        elif self.dbconnector == "mysql":
            sqlcmd = f"""
            SELECT
                ROUND(SUM(data_length + index_length), 2) AS "total_db_size"
            FROM information_schema.tables
            WHERE table_schema='{self.schema}'
            GROUP BY table_schema
            """
        with self.engine.begin() as conn:
            size = conn.execute(sqlcmd).fetchone()
            if size:
                size = size[0]
        size = conv_size(size)
        info = {"database": self.dbname, "size": size}

        # Tables info
        tables = self.getTableNames()
        info.update({"num_tables": len(tables)})
        info.update({"tables": [self.describeTable(tname) for tname in tables]})

        return info

    def getDatabases(self):
        """
        Returns a list with the names of all non-template databases
        """

        if self.dbconnector == "postgres":
            sqlcmd = "SELECT datname FROM pg_database WHERE datistemplate=false;"
        elif self.dbconnector == "mysql":
            sqlcmd = "SHOW DATABASES WHERE `Database` NOT IN ('information_schema','sys','performance_schema','mysql')"
        with self.engine.begin() as conn:
            result = conn.execute(sqlcmd)
        databases = [el[0] for el in result.fetchall()]
        return databases

    def useDatabase(self, db):
        """
        Change used database
        """

        if db not in self.getDatabases():
            print(f"ERROR: Database '{db}' not available.")
            return

        self.disconnect()
        self.dbname = db
        # Schema
        if self.dbconnector == "mysql":
            self.schema = db
        self.connectDB()

    # ┌────────────────────────┐
    # │   TABLES DESCRIPTION   │
    # └────────────────────────┘

    def getTableNames(self):
        """
        Returns a list with the names of all tables in the database
        """

        sqlcmd = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='{self.schema}'
        """

        with self.engine.begin() as conn:
            result = conn.execute(sqlcmd)
        tbnames = [el[0] for el in result.fetchall()]

        return tbnames

    def getTableShape(self, tablename):
        """
        Returns number of rows and columns of table
        """

        if not self._table_in_DB(tablename):
            return

        # Get number of rows
        sqlcmd = f"SELECT COUNT(*) FROM {self._conv_literal(tablename)}"
        with self.engine.begin() as conn:
            result = conn.execute(sqlcmd)
        rows = result.fetchone()[0]

        # Get columns
        cols = len(self.getColumnNames(tablename))

        return rows, cols

    def getColumnNames(self, tablename):
        """
        Returns a list with the names of all columns in the indicated table
        """

        if not self._table_in_DB(tablename):
            return []

        sqlcmd = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='{tablename}' AND table_schema='{self.schema}'
        """

        with self.engine.begin() as conn:
            result = conn.execute(sqlcmd)
        columnnames = [el[0] for el in result.fetchall()]

        return columnnames

    def describeTable(self, tablename):
        """
        Get some relevant information of a table as a dictionary
            - Table shape
            - Table size
            - Column types
        """

        if not self._table_in_DB(tablename):
            return {}

        info = {"table_name": tablename}

        # Table shape
        rows, cols = self.getTableShape(tablename)
        info.update({"num_rows": rows, "num_columns": cols})

        # Size
        if self.dbconnector == "postgres":
            sqlcmd = f"""
                SELECT 
                    pg_indexes_size(quote_ident("table_name")) as "index_size",
                    pg_relation_size(quote_ident("table_name")) as "table_size",
                    ROUND(
                        pg_indexes_size(quote_ident("table_name"))+
                        pg_relation_size(quote_ident("table_name"))
                    ) as "total_size"
                FROM information_schema.tables
                WHERE table_schema='{self.schema}' AND table_name='{tablename}'
            """
        if self.dbconnector == "mysql":
            sqlcmd = f"""
                SELECT
                    index_length AS "index_size",
                    data_length AS "table_size",
                    ROUND(((index_length + data_length)), 2) AS "total_size"
                FROM information_schema.tables
                WHERE table_schema='{self.schema}' AND table_name='{tablename}'
            """
        size = pd.read_sql(sqlcmd, con=self.engine, coerce_float=False)
        info.update(size.to_dict(orient="index")[0])
        if rows:
            info["avg_row_size"] = info["total_size"] / rows
        else:
            info["avg_row_size"] = 0

        info["index_size"] = conv_size(info["index_size"])
        info["table_size"] = conv_size(info["table_size"])
        info["total_size"] = conv_size(info["total_size"])
        info["avg_row_size"] = conv_size(info["avg_row_size"])

        # Columns info
        # Add ",COLUMN_KEY AS key"
        sqlcmd = f"""
            SELECT
                COLUMN_NAME AS cname,
                IS_NULLABLE AS nullable,
                DATA_TYPE AS dtype,
                CHARACTER_MAXIMUM_LENGTH AS max_len
            FROM information_schema.COLUMNS
            WHERE table_schema='{self.schema}' AND table_name='{tablename}'
        """
        ctype = pd.read_sql(sqlcmd, con=self.engine, coerce_float=False)
        ctype = [val for _, val in ctype.to_dict(orient="index").items()]
        info.update({"columns": ctype})

        return info

    # ┌───────────────────────┐
    # │   TABLES ALTERATION   │
    # └───────────────────────┘

    def createDBtable(self, tablename, columns):
        """
        Create new table in database

        Parameters
        ----------
        tablename: str
            Name of table
        columns: list(dict)
            Definitions of columns. See `addTableColumn`.
            {"columnname": str,\\
                "columntype": str,\\
                "nullable": bool,\\
                "primary_key": bool,\\
                "foreign_key": str}
        """

        if tablename in self.getTableNames():
            print(f"ERROR: Table '{tablename}' already exists in database.")
            return

        sqlcmd = f"CREATE TABLE {self._conv_literal(tablename)} ("
        num_cols = len(columns)
        for n, col in enumerate(columns):
            columnname = col.get("columnname")
            columntype = col.get("columntype")
            nullable = col.get("nullable")
            primary_key = col.get("primary_key")
            unique = col.get("unique")
            foreign_key = col.get("foreign_key")

            sqlcmd += f"{self._conv_literal(columnname)} {columntype}"
            if not nullable:
                sqlcmd += " NOT NULL"
            if primary_key:
                sqlcmd += " PRIMARY KEY"
            if unique:
                sqlcmd += " UNIQUE"
            if foreign_key:
                if not len(foreign_key) == 2:
                    print("ERROR: invalid foreign key. Format MUST be: (table, cname)")
                    return
                constraint_name = (
                    f"FK_{tablename.replace(' ', '_')}_{columnname.replace(' ', '_')}"
                )
                sqlcmd += f",CONSTRAINT {constraint_name}"
                sqlcmd += f" FOREIGN KEY ({columnname})"
                sqlcmd += f" REFERENCES {self._conv_literal(foreign_key[0])}"
                sqlcmd += f"({self._conv_literal(foreign_key[1])}) ON DELETE CASCADE"
            if n < num_cols - 1:
                sqlcmd += ","
        sqlcmd += ")"

        with self.engine.begin() as conn:
            conn.execute(sqlcmd)

    def deleteDBtables(self, tables=None):
        """
        Delete tables from database

        Parameters
        ----------
        tables: str | list
            Tables to delete.
            If None (default), all tables are deleted
        """

        # If tables is None, all tables are deleted and re-generated
        if tables is None:
            with self.engine.begin() as conn:
                if self.dbconnector == "postgres":
                    for table in self.getTableNames():
                        sqlcmd = f"TRUNCATE TABLE {self._conv_literal(table)} CASCADE"
                        # sqlcmd = f"DROP SCHEMA {self.schema} CASCADE; CREATE SCHEMA {self.schema};"
                        conn.execute(sqlcmd)
                elif self.dbconnector == "mysql":
                    conn.execute("SET FOREIGN_KEY_CHECKS = 0")
                    for table in self.getTableNames():
                        conn.execute(f"DROP TABLE {self._conv_literal(table)}")
                    conn.execute("SET FOREIGN_KEY_CHECKS = 1")

        else:
            tables = str2lst(tables)
            # Remove all selected tables (if exist in the database).
            with self.engine.begin() as conn:
                tables = [t for t in dict.fromkeys(tables) if t in self.getTableNames()]
                for table in tables:
                    conn.execute(f"DROP TABLE {self._conv_literal(table)}")

    def addTableColumn(
        self,
        tablename,
        columnname,
        columntype,
        nullable=True,
        primary_key=False,
        unique=False,
        foreign_key=None,
    ):
        """
        Add a new column to the specified table.

        Parameters
        ----------
        tablename: str
            Table to which the column will be added
        columnname: str
            Name of new column
        columntype: str
            Type of new column.
        nullable: bool
            If True, value can be NULL
        primary_key: bool
            If True, column is used as primary key
        unique: bool
            If True, value MUST be unique
        foreign_key: tuple(str, str)
            Specify foreign key as a tuple
            First element: referenced table
            Second element: referenced column in table

        """

        if not self._table_in_DB(tablename):
            return
        if columnname in self.getColumnNames(tablename):
            print(
                f"WARNING: Column '{columnname}' already exists in table '{tablename}'."
            )
            return

        sqlcmd = f"ALTER TABLE {self._conv_literal(tablename)}"
        sqlcmd += f" ADD COLUMN {self._conv_literal(columnname)} {columntype}"
        if not nullable:
            sqlcmd += " NOT NULL"
        if primary_key:
            sqlcmd += " PRIMARY KEY"
        if unique:
            sqlcmd += " UNIQUE"
        if foreign_key:
            if not len(foreign_key) == 2:
                print("ERROR: invalid foreign key. Format MUST be: (table, cname)")
                return
            constraint_name = (
                f"FK_{tablename.replace(' ', '_')}_{columnname.replace(' ', '_')}"
            )
            sqlcmd += f", ADD CONSTRAINT {constraint_name}"
            sqlcmd += f" FOREIGN KEY ({columnname})"
            sqlcmd += f" REFERENCES {self._conv_literal(foreign_key[0])}"
            sqlcmd += f"({self._conv_literal(foreign_key[1])}) ON DELETE CASCADE"

        with self.engine.begin() as conn:
            conn.execute(sqlcmd)

    def dropTableColumn(self, tablename, columnname):
        """
        Remove column from the specified table

        Parameters
        ----------
            tablename: str
                Table from which the column will be removed
            columnname: str
                Name of column to be removed

        """

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(columnname, tablename):
            return

        sqlcmd = f"""
            ALTER TABLE {self._conv_literal(tablename)}
            DROP COLUMN {self._conv_literal(columnname)}
        """

        with self.engine.begin() as conn:
            conn.execute(sqlcmd)

    # ┌───────────────┐
    # │   FUNCTIONS   │
    # └───────────────┘
    def readDBtable(
        self,
        tablename,
        selectOptions=None,
        filterOptions=None,
        orderOptions=None,
        limit=None,
    ):
        """
        Read data from a table in the database can choose to read only some
        specific fields

        Parameters
        ----------
            tablename: str
                Table to read from
            selectOptions: str | list(str)
                List with fields that will be retrieved
                (e.g. ['REFERENCIA', 'Resumen'])
            filterOptions: str | list(str)
                String with filtering options for the SQL query.
                If a list is passed, elements will be concatenated with 'AND'.\\
                Column name MUST be in double quotes and string values in single quotes. E.g.\\
                "\"Country\"='Germany' AND (\"City\"='Berlin' OR \"City\"='München')"\\
                "%" character MUST be escaped with "%". E.g.\\
                "\"name\" LIKE '%%Li'"
            orderOptions: str | list(str)
                List with field that will be used for sorting the
                results of the query
                (e.g, ['Cconv'])
            limit: int
                The maximum number of records to retrieve

        Returns
        -------
            df: DataFrame
                Table information

        """

        selectOptions = str2lst(selectOptions)
        orderOptions = str2lst(orderOptions)

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(selectOptions + orderOptions, tablename):
            return

        if isinstance(filterOptions, list):
            filterOptions = " AND ".join(f"({o})" for o in filterOptions)

        try:
            sqlQuery = "SELECT "
            if selectOptions:
                sqlQuery += ", ".join(
                    f"{self._conv_literal(o)}" if o != "*" else "*"
                    for o in selectOptions
                )
            else:
                sqlQuery += "*"

            sqlQuery += f" FROM {self._conv_literal(tablename)} "

            if filterOptions:
                sqlQuery += " WHERE " + filterOptions

            if orderOptions:
                sqlQuery += " ORDER BY " + ", ".join(
                    f"{self._conv_literal(o)}" for o in orderOptions
                )

            if limit:
                sqlQuery += f" LIMIT {limit}"

            # Return the pandas dataframe. Note that numbers in text format
            # are not converted to
            df = pd.read_sql(sqlQuery, con=self.engine, coerce_float=False)
            return df

        except Exception as e:
            print("Error in query:\n", sqlQuery)
            print(str(e))

    def readDBchunks(
        self,
        tablename,
        orderField,
        chunksize=100000,
        selectOptions=None,
        filterOptions=None,
        limit=None,
        verbose=True,
    ):
        """
        Read data from a table in the database using chunks.
        Can choose to read only some specific fields
        Rather than returning a dataframe, it returns an iterator that builds
        dataframes of desired number of rows (chunksize)

        Parameters
        ----------
            tablename: str
                Table to read from
            orderField: str
                Name of field that will be used for sorting the
                results of the query (e.g, 'ID'). This is the column
                that will be used for iteration, so this variable is mandatory
                and SHOULD be unique. Non-unique fields can result in information loss.
            chunksize: int
                Length of chunks for reading the table. Default value: 100000
            selectOptions: str | list(str)
                List with fields that will be retrieved
                (e.g. 'REFERENCIA, Resumen')
                If None, all columns will be retrieved
            filterOptions: str | list(str)
                String with filtering options for the SQL query.
                If a list is passed, elements will be concatenated with 'AND'.\\
                Column name MUST be in double quotes and string values in single quotes. E.g.\\
                "\"Country\"='Germany' AND (\"City\"='Berlin' OR \"City\"='München')"\\
                "%" character MUST be escaped with "%". E.g.\\
                "\"name\" LIKE '%%Li'"
            limit: int
                The total maximum number of records to retrieve.
            verbose: bool
                If True, information on the number of rows read so far will be displayed

        Returns
        -------
            df: DataFrame generator
                Table information
        """

        selectOptions = str2lst(selectOptions)
        orderField = str2lst(orderField)

        selectOptions = selectOptions + orderField
        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(selectOptions, tablename):
            return

        # Set number of elements to retrieve
        if limit and chunksize:
            remaining = limit
            next_chunk = min(remaining, chunksize)
        elif chunksize:
            next_chunk = chunksize
        else:
            next_chunk = limit

        df = self.readDBtable(
            tablename,
            selectOptions=selectOptions,
            filterOptions=filterOptions,
            orderOptions=orderField,
            limit=next_chunk,
        )

        cont = 0  # Total elements retrieved
        while len(df):
            cont += len(df)
            if verbose:
                print(
                    f"\r[DBManager (readDBchunks)] Number of rows read so far: {cont}",
                    end="",
                    flush=True,
                )
            yield df.iloc[:, :-1]

            # Update remaining elements
            if limit and chunksize:
                remaining = limit - cont
                next_chunk = min(remaining, chunksize)
            elif chunksize:
                next_chunk = chunksize
            else:
                next_chunk = 0

            if next_chunk > 0:
                # Next we need to start from last retrieved element
                filtercondition = (
                    f"{self._conv_literal(orderField[0])} > '{str(df.iloc[-1, -1])}'"
                )
                if filterOptions:
                    if isinstance(filterOptions, list):
                        filterOptions = " AND ".join(f"({o})" for o in filterOptions)
                    filtercondition = f"({filtercondition}) AND {filterOptions}"

                df = self.readDBtable(
                    tablename,
                    limit=next_chunk,
                    selectOptions=selectOptions,
                    filterOptions=filtercondition,
                    orderOptions=orderField,
                )

            else:
                # If maximum number of records has been reached, set df to empty list to exit
                df = []
        print()

    def insertInTable(self, tablename, columns, arguments, chunksize=None):
        """
        Insert new records into table

        Parameters
        ----------
            tablename: str
                Name of table in which the data will be inserted
            columns: str | list(str)
                List of names of columns for which data are provided
            arguments: list(tuple())
                A list of lists or tuples, each element associated
                to one new entry for the table
            chunksize: int
                If chunksize is not None, data will be inserted in chunks
                of the specified size
        """

        columns = str2lst(columns)
        ncol = len(columns)

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(columns, tablename):
            return

        if len(arguments[0]) == ncol:
            # Make sure we have a list of tuples; necessary for mysql
            arguments = list(map(tuple, arguments))

            sqlcmd = f"INSERT INTO {self._conv_literal(tablename)}("
            sqlcmd += ",".join(f"{self._conv_literal(c)}" for c in columns)
            sqlcmd += ") VALUES ("
            sqlcmd += ",".join(["%s"] * ncol)
            sqlcmd += ")"

            if chunksize:
                n_chunks = np.ceil(len(arguments) / chunksize)
                with tqdm(total=n_chunks, leave=None) as pbar:
                    pbar.set_description("Inserting chunks of data in database")
                    for chk in chunks(arguments, chunksize):
                        with self.engine.begin() as conn:
                            conn.execute(sqlcmd, chk)
                        pbar.update()
            else:
                with self.engine.begin() as conn:
                    conn.execute(sqlcmd, arguments)
        else:
            print(
                f"Error inserting data in table '{tablename}': number of columns mismatch"
            )

    def deleteFromTable(self, tablename, columns=None, arguments=None, chunksize=None):
        """
        Delete rows from table

        Parameters
        ----------
            tablename: str
                Name of table from which data will be removed
            columns: str | list
                List of names of columns for which data are provided.
                If columns and arguments are None delete all
            arguments: list(tuple())
                A list of lists or tuples, conditions for data to be removed.
                If columns and arguments are None delete all
            chunksize: int
                If chunksize is not None, data will be deleted in chunks
                of the specified size

        E.g., if columns is ['userID','productID'] and arguments is
        [['234', '227'],['234', '228']] this function will delete from the
        table all rows where userID='234' AND productID='227', and all rows
        where userID='234' and productID='228'
        """

        # if tablename not in self.getTableNames():
        #     print(f"ERROR: Table '{tablename}' does not exist in database.")
        #     return

        columns = str2lst(columns)
        ncol = len(columns)

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(columns, tablename):
            return

        if arguments is None:
            arg_len = 0
        else:
            arg_len = len(arguments[0])

        # Query
        sqlcmd = f"DELETE FROM {self._conv_literal(tablename)} "
        if arg_len == ncol:
            if ncol > 0:
                # Make sure we have a list of tuples; necessary for mysql
                arguments = list(map(tuple, arguments))

                sqlcmd += "WHERE "
                sqlcmd += " AND ".join(
                    [f"{self._conv_literal(el)}=%s" for el in columns]
                )

                if chunksize:
                    n_chunks = np.ceil(len(arguments) / chunksize)
                    with tqdm(total=n_chunks, leave=None) as pbar:
                        pbar.set_description("Deleting chunks of data in database")
                        for chk in chunks(arguments, chunksize):
                            with self.engine.begin() as conn:
                                conn.execute(sqlcmd, chk)
                            pbar.update()
                else:
                    with self.engine.begin() as conn:
                        conn.execute(sqlcmd, arguments)
            else:
                with self.engine.begin() as conn:
                    conn.execute(sqlcmd)
        else:
            print(
                f"Error deleting data from table '{tablename}': number of columns mismatch"
            )

    def setField(self, tablename, keyflds, valueflds, values):
        """
        Update records of a DB table

        Parameters
        ----------
            tablename: str
                Table that will be modified
            keyflds: str | list(str)
                List with the column names that will be used as key or reference
                and fulfill some conditions\\
                (e.g. 'REFERENCIA')
            valueflds: str | list
                List with the names of the columns that will be updated\\
                (e.g., 'Lemas', 'year')
            values: 
                A list of tuples in the format
                (keyfldvalue, valuefldvalue1, valuefldvalue2...)\\
                (e.g., [('Ref1', 'gen cell', 2015),
                        ('Ref2', 'big_data', 2018)])

        """

        # Make sure keyflds and valueflds are a list, and not a single string
        keyflds = str2lst(keyflds)
        valueflds = str2lst(valueflds)
        ncol = len(valueflds)
        nkey = len(keyflds)

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(keyflds + valueflds, tablename):
            return

        if not len(values[0]) == (ncol + nkey):
            print(
                f"Error updating table values in '{tablename}': number of columns mismatch"
            )
            return

        def circ_left_shift(tup, pos=1):
            """
            Auxiliary function to circularly shift a tuple n positions
            to the left, where n is len(keyfld)
            """
            ls = list(tup[pos:]) + list(tup[:pos])
            return tuple(ls)

        # Set keyfldvalue at the end of the list
        values = list(map(partial(circ_left_shift, pos=nkey), values))

        sqlcmd = f"UPDATE {self._conv_literal(tablename)} SET "
        sqlcmd += ",".join([f"{self._conv_literal(el)}=%s" for el in valueflds])
        sqlcmd += " WHERE "
        sqlcmd += " AND ".join([f"{self._conv_literal(el)}=%s" for el in keyflds])

        with self.engine.begin() as conn:
            conn.execute(sqlcmd, values)

    def upsert(
        self, tablename, keyflds, df, keyintable=None, chunksize=None, robust=True,
    ):
        """
        Update records of a DB table with the values in the df.
        This function implements the following additional functionality:
        * If there are columns in df that are not in the SQL table,
          columns will be added
        * New records will be created in the table if there are rows
          in the dataframe without an entry already in the table. For this,
          keyfld indicates which is the column that will be used as an
          index

        Parameters
        ----------
            tablename: str
                Table that will be modified
            keyflds: str | list(str)
                List with the column names that will be used as key or reference
                and fulfill some conditions.
                These SHOULD be unique. Non-unique fields can result in information loss.\\
                (e.g. 'REFERENCIA')
            df: DataFrame
                Dataframe that we wish to save in table 'tablename'
            keyintable: DataFrame
                (Optional) Rows of unique combinations of keyfld values
                Used to differentiate faster values to insert/update.
                (Best option is to create this DataFrame outside with the function
                `readDBtable(tablename, selectOptions=keyfld)` and pass as parameter
                when this function is called, then, update its value outside)
            chunksize: int
                If chunksize is not None, data will be inserted in chunks
                of the specified size
            robust: bool
                If False, verifications are skipped
                (for a faster execution)
        """

        keyflds = str2lst(keyflds)
        nkey = len(keyflds)

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(keyflds, tablename):
            return

        # Reorder dataframe to make sure that the key field goes first
        flds = keyflds + [x for x in df.columns if x not in keyflds]
        df = df[flds]

        # # Fill nan values
        # df = df.fillna(df.dtypes.replace({"int64": 9999, "float64": 9999, "O": ""}))
        # # df = df.where(pd.notnull(df), '')

        if robust:
            # Create new columns if necessary
            for clname in df.columns:
                if clname not in self.getColumnNames(tablename):
                    if df[clname].dtypes == np.float64:
                        self.addTableColumn(tablename, clname, "DOUBLE")
                    else:
                        if df[clname].dtypes == np.int64:
                            self.addTableColumn(tablename, clname, "INTEGER")
                        else:
                            self.addTableColumn(tablename, clname, "TEXT")

        # Check which values are already in the table, and split
        # the dataframe into records that need to be updated, and
        # records that need to be inserted
        if keyintable is None:
            keyintable = self.readDBtable(tablename, selectOptions=keyflds)

        # Split insert and update values
        values = pd.merge(df, keyintable, how="outer", on=keyflds, indicator=True)
        values_insert = values.loc[values["_merge"] == "left_only", flds].to_numpy()
        values_update = values.loc[values["_merge"] == "both", flds].to_numpy()

        # Insert new values
        if len(values_insert):
            # print(f"Insert {len(values_insert)} values in {tablename}")
            self.insertInTable(tablename, flds, values_insert, chunksize)

        # Update previous values
        if len(values_update):
            # print(f"Update {len(values_update)} values in {tablename}")
            self.setField(tablename, keyflds, flds[nkey:], values_update)

    def findDuplicated(
        self,
        tablename,
        keyflds,
        tableIdentifier=None,
        showDup=False,
        selectOptions=None,
    ):
        """
        Get duplicated entries in table.

        Parameters
        ----------
            tablename: 
                Name of the table
            keyflds: str | list(str)
                List of the column names that will be used as unique identifier\\
                (e.g. 'S2paperID')
            tableIdentifier: str
                (Optional) Unique table identifier of entries.
                Typically an incremental value used when inserting in table\\
                (e.g. 'paperID')
            showDup: bool
                If True, each repeated value will appear in different rows
            selectOptions: str | list(str)
                (Optional) Only when `showDup` is True.
                List with fields that will be retrieved only when .
                Use "*" to select all columns

        Returns
        -------
            df: DataFrame
                Dataframe with all repeated values
        """

        keyflds = str2lst(keyflds)
        tableIdentifier = str2lst(tableIdentifier)
        selectOptions = str2lst(selectOptions)

        if any([el == "*" for el in selectOptions]):
            selectOptions = self.getColumnNames(tablename)
            [selectOptions.remove(el) for el in keyflds]

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(keyflds + tableIdentifier + selectOptions, tablename):
            return

        # Query
        sqlQuery = "SELECT "
        if tableIdentifier:
            sqlQuery += f'MIN({self._conv_literal(tableIdentifier[0])}) AS "min_{tableIdentifier[0]}",'
        sqlQuery += ",".join(f"{self._conv_literal(el)}" for el in keyflds)
        sqlQuery += (
            f',COUNT(*) AS "appearances" FROM {self._conv_literal(tablename)} GROUP BY '
        )
        sqlQuery += ",".join(f"{self._conv_literal(el)}" for el in keyflds)
        sqlQuery += " HAVING COUNT(*)>1 "
        sqlQuery += "ORDER BY appearances DESC"
        if showDup:
            sqlQuery = f'WITH {self._conv_literal("dup")} AS(' + sqlQuery + ")"
            sqlQuery += " SELECT "
            sqlQuery += ",".join(
                f'{self._conv_literal("orig")}.{self._conv_literal(el)}'
                for el in keyflds
            )
            if selectOptions:
                sqlQuery += ","
                sqlQuery += ",".join(
                    f'{self._conv_literal("orig")}.{self._conv_literal(el)}'
                    for el in selectOptions
                )
            sqlQuery += (
                f' FROM {self._conv_literal(tablename)} {self._conv_literal("orig")} '
            )
            sqlQuery += f'INNER JOIN {self._conv_literal("dup")} ON '
            sqlQuery += " AND ".join(
                f'{self._conv_literal("dup")}.{self._conv_literal(el)}={self._conv_literal("orig")}.{self._conv_literal(el)}'
                for el in keyflds
            )
            sqlQuery += " ORDER BY "
            sqlQuery += ",".join(
                f'{self._conv_literal("orig")}.{self._conv_literal(el)}'
                for el in keyflds
            )

        try:
            # Return the pandas dataframe. Note that numbers in text format
            # are not converted to
            df = pd.read_sql(sqlQuery, con=self.engine, coerce_float=False)
            return df

        except Exception as e:
            print("Error in query:\n", sqlQuery)
            print(e)

    def removeDuplicated(self, tablename, keyflds, tableIdentifier):
        """
        Remove duplicated entries in table. Keep values with lower tableIdentifier.

        Parameters
        ----------
            tablename: str
                Name of the table
            keyflds: str | list(str)
                List of the column names that will be used as unique identifier.
                After execution, each entry will have unique value.\\
                (e.g. 'S2paperID')
            tableIdentifier: 
                Unique table identifier of entries.
                Typically an incremental value used when inserting in table\\
                (e.g. 'paperID')
        """

        keyflds = str2lst(keyflds)
        tableIdentifier = str2lst(tableIdentifier)

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(keyflds, tablename):
            return

        # Get duplicated values
        df = self.findDuplicated(tablename, keyflds, tableIdentifier)

        if df.empty:
            print("No duplicated entries.")
            return

        # Sort info to remove
        index = f"min_{tableIdentifier[0]}"
        columns = df.columns[1:-1].tolist()
        arguments = df[columns + [index]].to_numpy().tolist()

        sqlcmd = f"DELETE FROM {self._conv_literal(tablename)} WHERE "
        sqlcmd += " AND ".join(f"{self._conv_literal(c)}=%s" for c in columns)
        sqlcmd += f" AND {self._conv_literal(tableIdentifier[0])}!=%s"

        with self.engine.begin() as conn:
            conn.execute(sqlcmd, arguments)

    # ┌────────────┐
    # │   EXPORT   │
    # └────────────┘

    def exportTable(self, tablename, filename, path="", fileformat="csv", columns=None):
        """
        Export columns from a table to a file.

        Parameters
        ----------
            tablename: str
                Name of the table
            filename: str
                Name of the output file
            fileformat: str
                Type of output file. Available options are:
                {'csv', 'json', 'xlsx', 'pkl'}
            path: Path
                Route to the output folder
            columns: str | list(str)
                Columns to save. It can be a list or a string
                of comma-separated columns.
                If None, all columns saved.
        """

        columns = str2lst(columns)

        if not self._table_in_DB(tablename):
            return
        if not self._els_in_table(columns, tablename):
            return

        # Path to the output file
        fpath = Path(path)

        # Read data:
        df = self.readDBtable(tablename, selectOptions=columns)

        # Export results to file
        if fileformat == "csv":
            df.to_csv(fpath.joinpath(f"{filename}.csv"), index=False)

        elif fileformat == "json":
            df.to_json(fpath.joinpath(f"{filename}.json"), orient="records")

        elif fileformat == "xlsx":
            df.to_excel(fpath.joinpath(f"{filename}.xlsx"), index=False)

        elif fileformat == "pkl":
            df.to_pickle(fpath.joinpath(f"{filename}.pkl"), index=False)

        else:
            print("Invalid file format.")

    def importTable(self, tablename, filename, path="", fileformat="csv"):
        """
        Import data to a table from a file.

        Parameters
        ----------
            tablename: str
                Name of the table
            filename: str
                Name of the output file
            fileformat: str
                Type of output file. Available options are:
                {'csv', 'json', 'xlsx', 'pkl'}
            path: Path
                Route to the output folder
            columns: str | list(str)
                Columns to save. It can be a list or a string
                of comma-separated columns.
                If None, all columns saved.
        """

        if not self._table_in_DB(tablename):
            return

        # Path to the output file
        fpath = Path(path)

        # Import from file
        if fileformat == "csv":
            df = pd.read_csv(fpath.joinpath(f"{filename}.csv"))

        elif fileformat == "json":
            df = pd.read_json(fpath.joinpath(f"{filename}.json"), orient="records")

        elif fileformat == "xlsx":
            df = pd.read_excel(fpath.joinpath(f"{filename}.xlsx"))

        elif fileformat == "pkl":
            df = pd.read_pickle(fpath.joinpath(f"{filename}.pkl"))

        else:
            print("Invalid file format.")
            return

        # Insert in database
        cols = df.columns.tolist()
        data = df.to_numpy()
        if not self._els_in_table(cols, tablename):
            return
        self.insertInTable(tablename, cols, data)

    def DBdump(self, filename, path="", tables=None, compressed=False):
        """
        Creates dump of database

        Parameters
        ----------
            filename: str
                Name of the output file
            path: Path
                Path of file
            tables: str | list(str)
                List of tables to dump
                If None, all tables are saved
            compressed: bool
                Compress output file
        """

        if tables:
            tables = str2lst(tables)
            tabs = self.getTableNames()
            opt = [o for o in tables if o not in tabs]
            if opt:
                f"ERROR: Selected tables '{','.join(opt)}' not found in the datanase"
                return

        fpath = Path(path).joinpath(f"{filename}.sql")

        # DB command
        if self.dbconnector == "postgres":
            dumpcmd = f"pg_dump --column-inserts --dbname=postgresql://{self._url} "
            dumpcmd += " --format plain --verbose "
        elif self.dbconnector == "mysql":
            dumpcmd = f"mysqldump -P {self.dbport} -h {self.dbhost} -u {self.dbuser} -p{self.dbpass} -B {self.dbname} "

        if tables:
            if self.dbconnector == "postgres":
                # To select tables:
                # -t \"tableName\"
                table_list = r'\"" -t "\"'.join(tables)
                dumpcmd += fr' -t "\"{table_list}\""'
            elif self.dbconnector == "mysql":
                table_list = '" "'.join(tables)
                dumpcmd += " --tables " + f'"{table_list}"'

        if compressed:
            dumpcmd = dumpcmd + f" | gzip > {fpath}.gz"
        else:
            if self.dbconnector == "postgres":
                dumpcmd += f" -f {fpath}"
            elif self.dbconnector == "mysql":
                dumpcmd += f" > {fpath}"

        try:
            os.system(dumpcmd)
        except:
            print("Error when creating dump. Check route to filename")

    # ┌───────────┐
    # │   OTHER   │
    # └───────────┘
    def execute(self, sqlcmd):
        """
        Execute SQL command received as parameter
        """
        with self.engine.begin() as conn:
            conn.execute(sqlcmd)

    def executeSQLfile(self, file):
        """
        Execute SQL file
        """

        with open(file, "r") as f:
            self.execute(f.read())
