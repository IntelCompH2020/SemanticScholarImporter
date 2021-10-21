"""
This class provides functionality for managing a generic postgresql database:

Created on July 2021

@authors: Jerónimo Arenas García (jeronimo.arenas@uc3m.es)
          Saúl Blanco Fortes (sblanco@tsc.uc3m.es)
          Jesús Cid Sueiro (jcid@ing.uc3m.es)
          José Antonio Espinosa Melchor (joespino@pa.uc3m.es)

Exports class BaseDMsql that can be used to derive new classes
for specific projects that may include table creation, data import,
etc for a particular project

The base clase provided in this file implements the following methods:
* __init__          : The constructor of the class. It creates connection
                      to a particular database
* connectDB         : Create connection to database
* disconnect        : Close connection to database
* __del__           : Cleanly closes the database
* deleteDBtables    : Deletes table(s) from database
* addTableColumn    : Adds a column at the end of table of the database
* dropTableColumn   : Removes column from table (only MySQL)
* readDBtable       : Reads rows from table and returns a pandas dataframe
                      with the retrieved data
* readDBchunks      : Provides an iterator to read chunks of rows in table.
                      Each iteration returns a chunk of predefined max number of rows
                      to avoid stalling the mysql server
* getTableNames     : Gets the names of the tables in the database
* getColumnNames    : Gets the names of the columns in a particular table
* getTableShape     : Get number of rows and columns in the indicated table
# * describeTables    : Gets the number of rows and the names of columns in table
* describeDatabase  : Get some relevant information of database
* getDatabases      : Get the names of all non-template databases
* useDatabase       : Change used database
* insertInTable     : Insert new records in Table. Input data comes as a list of tuples.
* deleteFromTable   : Delete records from Table. Conditions are given on columname and values
* setField          : Updates table records. Input data comes as a list of tuples.
* upsert            : Update or insert records in a table. Input data comes as panda df
                      If the record exists (according to primary key) data will be updated
                      If the record does not exist, new records will be created
* exportTable       : Export a table from database either as pickle or excel file
* DBdump            : Creates dump of full database, or dump of selected tables
* execute           : Execute SQL command received as parameter
* findDuplicated    : Get duplicated entries in table.
* removeDuplicated  : Remove duplicated entries in table.

"""

import json
import os
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg2 import connect
from psycopg2.sql import SQL, Identifier, Literal
from tqdm import tqdm


def chunks(l, n):
    """Yields successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


class BaseDMsql(object):
    """
    Data manager base class.
    """

    def __init__(
        self, dbname, dbhost=None, dbuser=None, dbpass=None, dbport=None,
    ):
        """
        Initializes a DataManager object

        Parameters
        ----------
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
        # Connector to database
        self._conn = None
        # Cursor of the database
        self._c = None

        self.connectDB()

    def connectDB(self):
        """
        Create connection to database
        """
        # Try connection
        try:
            self._conn = connect(
                dbname=self.dbname,
                user=self.dbuser,
                password=self.dbpass,
                host=self.dbhost,
                port=self.dbport,
            )
            self._c = self._conn.cursor()
            self._c.execute("SELECT version()")
            db_version = self._c.fetchone()[0]

            print("Database connection successful.")
            print(f"PostgreSQL database version: {db_version}")
            print(f"Database: {self.dbname}\n")

            self.dbON = True

        except Exception as e:
            print("---- Error connecting to the database")
            print("\n", e)

    def disconnect(self):
        """
        Close connection to database
        When destroying the object, it is necessary to commit changes
        in the database and close the connection
        """
        try:
            print("Closing...")
            self._conn.commit()
            self._conn.close()
            self.dbON = False
        except Exception as e:
            print("---- Error closing database")
            print("\n", e)

    def __del__(self):

        self.disconnect()

    """def setConnCharset(self, charsetcode):
        self._conn.set_character_set(charsetcode)
        return
    """

    def deleteDBtables(self, tables=None):
        """
        Delete tables from database

        Parameters
        ----------
        tables: str | list
            If string, name of the table to reset.
            If list, list of tables to reset
            If None (default), all tables are deleted
        """

        # If tables is None, all tables are deleted and re-generated
        if tables is None:
            self._c.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

        else:
            # It tables is not a list, make the appropriate list
            if isinstance(tables, str):
                tables = [el.strip() for el in tables.split(",")]

            # Remove all selected tables (if exist in the database).
            for table in set(tables) & set(self.getTableNames()):
                self._c.execute(SQL("DROP TABLE {}").format(Identifier(table)))

        self._conn.commit()

    def addTableColumn(self, tablename, columnname, columntype):
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
        """

        # Check if the table exists
        if tablename in self.getTableNames():

            # Check that the column does not already exist
            if columnname not in self.getColumnNames(tablename):

                # Allow columnames with spaces
                sqlcmd = SQL("ALTER TABLE {tname} ADD COLUMN {cname} ").format(
                    tname=Identifier(tablename), cname=Identifier(columnname),
                )
                sqlcmd += SQL(columntype)
                self._c.execute(sqlcmd)

                # Commit changes
                self._conn.commit()

            else:
                print(
                    f"WARNING: Column '{columnname}' already exists in table '{tablename}'."
                )

        else:
            print(
                "Error adding column to table. Please, select a valid table name from the list"
            )
            print(self.getTableNames())

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

        # Check if the table exists
        if tablename in self.getTableNames():

            # Check that the column exists
            if columnname in self.getColumnNames(tablename):

                # Allow columnames with spaces
                sqlcmd = SQL("ALTER TABLE {tname} DROP COLUMN {cname}").format(
                    tname=Identifier(tablename), cname=Identifier(columnname),
                )
                self._c.execute(sqlcmd)

                # Commit changes
                self._conn.commit()

            else:
                print(
                    f"Error deleting column. The column '{columnname}' does not exist."
                )

        else:
            print(
                "Error deleting column. Please, select a valid table name from the list"
            )
            print(self.getTableNames())

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
            filterOptions: str
                String with filtering options for the SQL query
                Column name MUST be in double quotes and string values in scaped single quotes
                (e.g., '"Country"="Germany" AND ("City"=\'Berlin\' OR "City"=\'München\')')
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

        if isinstance(selectOptions, str):
            selectOptions = [el.strip() for el in selectOptions.split(",")]

        if isinstance(orderOptions, str):
            orderOptions = [el.strip() for el in orderOptions.split(",")]

        try:
            sqlQuery = SQL("SELECT ")
            if selectOptions:
                sqlQuery += SQL(", ").join(Identifier(o) for o in selectOptions)
            else:
                sqlQuery += SQL("*")

            sqlQuery += SQL(" FROM {} ").format(Identifier(tablename))

            if filterOptions:
                sqlQuery += SQL(f" WHERE ") + SQL(filterOptions)

            if orderOptions:
                sqlQuery += SQL(" ORDER BY ") + SQL(", ").join(
                    Identifier(o) for o in orderOptions
                )

            if limit:
                sqlQuery += SQL(" LIMIT {}").format(Literal(limit))

            # print(sqlQuery.as_string(self._c))

            # This is to update the connection to changes by other
            # processes.
            self._conn.commit()

            # Return the pandas dataframe. Note that numbers in text format
            # are not converted to
            df = pd.read_sql(sqlQuery, con=self._conn, coerce_float=False)
            return df

        except Exception as e:
            print("Error in query:", sqlQuery)
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
            chunksize: int
                Length of chunks for reading the table. Default value: 100000
            selectOptions: str | list(str)
                List with fields that will be retrieved
                (e.g. 'REFERENCIA, Resumen')
                If None, all columns will be retrieved
            filterOptions: str
                String with filtering options for the SQL query
                (e.g., 'UNESCO_cd=23')
            limit: int
                The total maximum number of records to retrieve.
            verbose: bool
                If True, information on the number of rows read so far will be displayed

        Returns
        -------
            df: DataFrame
                Table information
        """

        # Ensure selectOptions is a list
        if isinstance(selectOptions, str):
            selectOptions = [el.strip() for el in selectOptions.split(",")]
        selectOptions = selectOptions + [orderField]

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
                filtercondition = f'"{orderField}" > {str(df.iloc[-1, -1])}'
                if filterOptions:
                    filtercondition = filtercondition + " AND " + filterOptions

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

    def getTableNames(self):
        """
        Returns a list with the names of all tables in the database
        """

        # The specific command depends on whether we are using mysql or sqlite
        sqlcmd = (
            "SELECT table_name FROM INFORMATION_SCHEMA.TABLES "
            + "WHERE table_schema='public'"
        )

        self._c.execute(sqlcmd)
        tbnames = [el[0] for el in self._c.fetchall()]

        return tbnames

    def getColumnNames(self, tablename):
        """
        Returns a list with the names of all columns in the indicated table
        """

        # Check if tablename exists in database
        if tablename in self.getTableNames():
            sqlcmd = "SELECT column_name FROM information_schema.columns WHERE table_name = %s"
            data = (tablename,)
            self._c.execute(sqlcmd, data)
            columnnames = [el[0] for el in self._c.fetchall()]

            return columnnames

        else:
            print(
                f"Error retrieving column names: Table does not exist on '{self.dbname}' database."
            )
            return []

    def getTableShape(self, tablename):
        """
        Returns number of rows and columns of table
        """
        # Get number of rows
        sqlcmd = SQL("SELECT COUNT(*) FROM {}").format(Identifier(tablename))
        self._c.execute(sqlcmd)
        rows = self._c.fetchone()[0]

        # Get columns
        cols = len(self.getColumnNames(tablename))

        return rows, cols

    # def describeTables(self):
    #     dfs = []
    #     sqlcmd = f"""
    #         SELECT
    #             table_name AS 'tname',
    #             ROUND((data_length + index_length) / 1024 / 1024, 2) AS 'tsize (MB)'
    #         FROM
    #             information_schema.tables
    #         WHERE
    #             table_schema = '{self.dbname}';"""

    #     table_list = pd.read_sql(sqlcmd, con=self._conn, coerce_float=False)

    #     for i, table in table_list.iterrows():
    #         name = table.tname
    #         # Number rows
    #         sqlcmd = f"""SELECT '{name}' tname , COUNT(*) nrows FROM {name};"""
    #         df = pd.read_sql(sqlcmd, con=self._conn, coerce_float=False)

    #         # Merge with size
    #         info = pd.DataFrame().append(table)
    #         info = info.merge(df, on="tname")
    #         info[["nrows", "tsize (MB)"]] = info[["nrows", "tsize (MB)"]].astype(float)
    #         info["avg_row_size (MB)"] = np.divide(info["tsize (MB)"], info["nrows"])

    #         # Columns info
    #         sqlcmd = f"""
    #                 SELECT
    #                     COLUMN_NAME AS cname,
    #                     IS_NULLABLE AS nullable,
    #                     DATA_TYPE AS dtype,
    #                     CHARACTER_MAXIMUM_LENGTH AS max_len,
    #                     COLUMN_KEY AS 'key'
    #                 FROM INFORMATION_SCHEMA.COLUMNS
    #                 WHERE TABLE_NAME='{name}' AND TABLE_SCHEMA='{self.dbname}';
    #                 """
    #         cols = pd.read_sql(sqlcmd, con=self._conn, coerce_float=False)

    #         dfs.append(pd.concat([info, cols], axis=1))
    #     return pd.concat(dfs, ignore_index=True)

    def describeDatabase(self):
        """
        Get some relevant information of database as a dictionary
            - DB name
            - DB size
            - Tables size
            - Tables shape
        """

        # DB size
        sqlcmd = "SELECT pg_size_pretty(pg_database_size(current_database())) AS total_db_size"
        self._c.execute(sqlcmd)
        size = self._c.fetchone()[0]
        info = {"database": self.dbname, "size": size}

        # Tables' size
        sqlcmd = """
            SELECT 
                "table_name",
                pg_size_pretty(pg_indexes_size(quote_ident("table_name"))) as "index_size",
                pg_size_pretty(pg_relation_size(quote_ident("table_name"))) as "table_size",
                pg_size_pretty(pg_total_relation_size(quote_ident("table_name"))) as "total_size"
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY "table_size"
            """
        df = pd.read_sql(sqlcmd, con=self._conn, coerce_float=False)
        df.set_index("table_name", inplace=True)
        info.update({"table": json.loads(df.to_json(orient="index"))})

        # Tables' shape
        for t in info["table"].keys():
            rows, cols = self.getTableShape(t)
            info["table"][t].update({"rows": rows, "columns": cols})

        # info = json.dumps(info, indent=4)

        return info

    def getDatabases(self):
        """
        Returns a list with the names of all non-template databases
        """

        sqlcmd = "SELECT datname FROM pg_database WHERE datistemplate=false;"
        self._c.execute(sqlcmd)
        databases = [el[0] for el in self._c.fetchall()]
        return databases

    def useDatabase(self, db):
        """
        Change used database
        """

        self.disconnect()
        self.dbname = db
        self.connectDB()

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

        # Make sure columns is a list, and not a single string
        if isinstance(columns, str):
            columns = [el.strip() for el in columns.split(",")]

        ncol = len(columns)
        if len(arguments[0]) == ncol:
            # Make sure the tablename is valid
            if tablename in self.getTableNames():
                # Make sure we have a list of tuples; necessary for mysql
                arguments = list(map(tuple, arguments))

                sqlcmd = SQL("INSERT INTO {} (").format(Identifier(tablename))

                sqlcmd += SQL(",").join(Identifier(c) for c in columns) + SQL(
                    ") VALUES ("
                )
                sqlcmd += SQL(",".join(["%s"] * ncol)) + SQL(")")

                # print(sqlcmd.as_string(self._c))
                if chunksize:
                    n_chunks = np.ceil(len(arguments) / chunksize)
                    with tqdm(total=n_chunks, leave=None) as pbar:
                        pbar.set_description("Inserting chunks of data in database")
                        for chk in chunks(arguments, chunksize):
                            try:
                                self._c.executemany(sqlcmd, chk)
                                self._conn.commit()
                                pbar.update()
                            except Exception as e:
                                print("\n\n")
                                print(e)
                                print(sqlcmd.as_string(self._c))
                                for n in range(len(chk[0])):
                                    print(f"{columns[n]}:\t{chk[0][n]}")
                                print("\n\n")
                                return
                else:
                    self._c.executemany(sqlcmd, arguments)
                    self._conn.commit()
            else:
                print(
                    f"Error inserting data in table: The table '{tablename}' does not exist"
                )
        else:
            print(
                f"Error inserting data in table '{tablename}': number of columns mismatch"
            )

    def deleteFromTable(self, tablename, columns, arguments, chunksize=None):
        """
        Delete rows from table

        Parameters
        ----------
            tablename: str
                Name of table from which data will be removed
            columns: str | list
                List of names of columns for which data are provided
            arguments: list(tuple())
                A list of lists or tuples, conditions for data to be removed
            chunksize: int
                If chunksize is not None, data will be deleted in chunks
                of the specified size

        E.g., if columns is ['userID','productID'] and arguments is
        [['234', '227'],['234', '228']] this function will delete from the
        table all rows where userID='234' AND productID='227', and all rows
        where userID='234' and productID='228'
        """

        # Make sure columns is a list, and not a single string
        if isinstance(columns, str):
            columns = [el.strip() for el in columns.split(",")]

        ncol = len(columns)

        if len(arguments[0]) == ncol:
            # Make sure the tablename is valid
            if tablename in self.getTableNames():
                # Make sure we have a list of tuples; necessary for mysql
                arguments = list(map(tuple, arguments))

                sqlcmd = SQL("DELETE FROM {} WHERE ").format(Identifier(tablename))
                sqlcmd += SQL(" AND ".join([f"{el}=%s" for el in columns]))

                # print(sqlcmd.as_string(self._c))

                if chunksize:
                    n_chunks = np.ceil(len(arguments) / chunksize)
                    with tqdm(total=n_chunks, leave=None) as pbar:
                        pbar.set_description("Deleting chunks of data in database")
                        for chk in chunks(arguments, chunksize):
                            self._c.executemany(sqlcmd, chk)
                            self._conn.commit()
                            pbar.update()

                else:
                    self._c.executemany(sqlcmd, arguments)
                    self._conn.commit()

            else:
                print(
                    f"Error deleting data from table: The table '{tablename}' does not exist"
                )

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
            keyfld: str | list(str)
                String with the column name that will be used as key\\
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

        # Auxiliary function to circularly shift a tuple n positions
        # to the left, where n is len(keyfld)
        def circ_left_shift(tup, pos=1):
            ls = list(tup[pos:]) + list(tup[:pos])
            return tuple(ls)

        # Make sure keyflds and valueflds are a list, and not a single string
        if isinstance(keyflds, str):
            keyflds = [el.strip() for el in keyflds.split(",")]
        if isinstance(valueflds, str):
            valueflds = [el.strip() for el in valueflds.split(",")]

        ncol = len(valueflds)
        nkey = len(keyflds)

        if len(values[0]) == (ncol + nkey):
            # Make sure the tablename is valid
            if tablename in self.getTableNames():
                # Set keyfldvalue at the end of the list
                values = list(map(partial(circ_left_shift, pos=nkey), values))

                sqlcmd = SQL("UPDATE {} SET ").format(Identifier(tablename))
                sqlcmd += SQL(",".join(["{}=%s"] * ncol)).format(
                    *[Identifier(el) for el in valueflds]
                )
                # sqlcmd += SQL(" WHERE {}=%s").format(Identifier(keyfld))
                sqlcmd += SQL(" WHERE ")
                sqlcmd += SQL(" AND ".join(["{}=%s"] * nkey)).format(
                    *[Identifier(el) for el in keyflds]
                )

                # print(sqlcmd.as_string(self._c))

                self._c.executemany(sqlcmd, values)
                self._conn.commit()

            else:
                print(
                    f"Error udpating table values: The table '{tablename}' does not exist"
                )
        else:
            print(
                f"Error updating table values in '{tablename}': number of columns mismatch"
            )

    def upsert(
        self, tablename, keyfld, df, keyintable={}, chunksize=None, robust=True,
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
            keyfld: str
                String with the column name that will be used as key.
                MUST be unique value\\
                (e.g. 'S2paperID')
            df: DataFrame
                Dataframe that we wish to save in table tablename
            keyintable: set
                (Optional) Set of unique keyfld values.
                Used to differentiate faster values to insert/update.
                (Best option is to create this set outside with the function
                `readDBtable(tablename, selectOptions=keyfld)` and pass a set to
                this function when it is called)
            chunksize: int
                If chunksize is not None, data will be inserted in chunks
                of the specified size
            robust: bool
                If False, verifications are skipped
                (for a faster execution)
        """

        # Check that table exists, keyfld exists both in the Table and the
        # Dataframe, and tableID exists in table
        if robust:
            if tablename in self.getTableNames():
                if not keyfld in self.getColumnNames(tablename):
                    print(
                        f"Upsert function failed: Key field '{keyfld}' does not exist",
                        f"in the selected table '{tablename}' and/or dataframe",
                    )
                    return
            else:
                print("Upsert function failed: Table does not exist")
                return

        # Reorder dataframe to make sure that the key field goes first
        flds = [keyfld] + [x for x in df.columns if x != keyfld]
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
        if not keyintable:
            keyintable = self.readDBtable(tablename, selectOptions=keyfld)
            keyintable = set(keyintable[keyfld].tolist())

        # Insert new values
        values_insert = list(filter(lambda x: x[0] not in keyintable, df.values))
        if len(values_insert):
            # print(f"Insert {len(values_insert)} values in {tablename}")
            self.insertInTable(tablename, flds, values_insert, chunksize)

        # Update previous values
        values_update = list(filter(lambda x: x[0] in keyintable, df.values))
        if len(values_update):
            # print(f"Update {len(values_update)} values in {tablename}")
            self.setField(tablename, keyfld, flds[1:], values_update)

    def exportTable(self, tablename, filename, path="", fileformat="pkl", cols=None):
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
                {'xlsx', 'pkl'}
            path: Path
                Route to the output folder
            columnames: str | list(str)
                Columns to save. It can be a list or a string
                of comma-separated columns.
                If None, all columns saved.
        """

        # Path to the output file
        fpath = Path(path)

        # Read data:
        df = self.readDBtable(tablename, selectOptions=cols)

        # ######################
        # Export results to file
        if fileformat == "pkl":
            df.to_pickle(fpath.joinpath(f"{filename}.pkl"))

        elif fileformat == "xlsx":
            df.to_excel(fpath.joinpath(f"{filename}.xlsx"))
        else:
            print("Invalid file format.")

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

        fpath = Path(path).joinpath(f"{filename}.sql")

        # DB command
        dumpcmd = f"pg_dump --dbname=postgresql://{self.dbuser}:{self.dbpass}@{self.dbhost}:{self.dbport}/{self.dbname}"

        if tables:
            # To select tables:
            # -t \"tableName\"
            table_list = r"\" -t \"".join(tables)
            dumpcmd = dumpcmd + fr" -t \"{table_list}\""

        if compressed:
            dumpcmd = dumpcmd + f" | gzip > {fpath}.gz"
        else:
            dumpcmd = dumpcmd + f" -f {fpath}"

        try:
            os.system(dumpcmd)
        except:
            print("Error when creating dump. Check route to filename")

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

        # Convert keyfld to list
        if isinstance(keyflds, str):
            keyflds = [el.strip() for el in keyflds.split(",")]
        nkey = len(keyflds)

        if selectOptions:
            if isinstance(selectOptions, str):
                if selectOptions == "*":
                    selectOptions = self.getColumnNames(tablename)
                    [selectOptions.remove(el) for el in keyflds]
                else:
                    selectOptions = [el.strip() for el in selectOptions.split(",")]
            ncol = len(selectOptions)

        # Check that table and options exist
        if tablename in self.getTableNames():
            cols = self.getColumnNames(tablename)
            if selectOptions:
                opt = [o for o in selectOptions if o not in cols]
                if opt:
                    print(
                        f"Selected options '{','.join(opt)}' not found in the selected table '{tablename}'"
                    )
                    return
            opt = [o for o in keyflds if o not in cols]
            if opt:
                print(
                    f"Selected keyfld '{','.join(opt)}' not found in the selected table '{tablename}'"
                )
                return
        else:
            print(f"Table '{tablename}' not found.")
            return

        # Query
        sqlQuery = SQL("SELECT ")
        if tableIdentifier:
            sqlQuery += SQL("MIN({}) AS {},").format(
                Identifier(tableIdentifier), Identifier(f"min_{tableIdentifier}")
            )
        sqlQuery += SQL(",".join(["{}"] * nkey)).format(
            *[Identifier(el) for el in keyflds]
        )
        sqlQuery += SQL(',COUNT(*) AS "appearances" FROM {} GROUP BY ').format(
            Identifier(tablename)
        )
        sqlQuery += SQL(",".join(["{}"] * nkey)).format(
            *[Identifier(el) for el in keyflds]
        )
        sqlQuery += SQL(" HAVING COUNT(*)>1 ")
        sqlQuery += SQL("ORDER BY appearances DESC")
        if showDup:
            sqlQuery = SQL('WITH "dup" AS(') + sqlQuery + SQL(")")
            sqlQuery += SQL(" SELECT ")
            sqlQuery += SQL(",".join(['"orig".{}'] * nkey)).format(
                *[Identifier(el) for el in keyflds]
            )
            if selectOptions:
                sqlQuery += SQL(",")
                sqlQuery += SQL(",".join(['"orig".{}'] * ncol)).format(
                    *[Identifier(el) for el in selectOptions]
                )
            sqlQuery += SQL(' FROM {} "orig" ').format(Identifier(tablename))
            sqlQuery += SQL('INNER JOIN "dup" ON ')
            format_join = [[Identifier(el), Identifier(el)] for el in keyflds]
            format_join = [el for f in format_join for el in f]
            sqlQuery += SQL(" AND ".join(['"dup".{}="orig".{}'] * nkey)).format(
                *format_join
            )
            sqlQuery += SQL(" ORDER BY ")
            sqlQuery += SQL(",".join(['"orig".{}'] * nkey)).format(
                *[Identifier(el) for el in keyflds]
            )

        # print(sqlQuery.as_string(self._c))

        try:
            # This is to update the connection to changes by other
            # processes.
            self._conn.commit()

            # Return the pandas dataframe. Note that numbers in text format
            # are not converted to
            df = pd.read_sql(sqlQuery, con=self._conn, coerce_float=False)
            return df

        except Exception as e:
            print("Error in query:\n", sqlQuery.as_string(self._c))
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

        # Get duplicated values
        df = self.findDuplicated(tablename, keyflds, tableIdentifier)

        if df is None:
            print("No duplicated entries.")
            return

        # Sort info to remove
        index = f"min_{tableIdentifier}"
        columns = df.columns[1:-1].tolist()
        arguments = np.concatenate(
            (df[columns].values, df[index].values[:, None]), axis=1
        )

        # Make sure we have a list of tuples; necessary for mysql
        arguments = list(map(tuple, arguments))

        sqlcmd = SQL("DELETE FROM {} WHERE ").format(Identifier(tablename))
        sqlcmd += SQL("{}=%s").format(Identifier(columns[0]))
        for c in columns[1:]:
            sqlcmd += SQL(" AND {}=%s").format(Identifier(c))
        sqlcmd += SQL(" AND {}!=%s").format(Identifier(tableIdentifier))

        # print(sqlcmd.as_string(self._c))

        self._c.executemany(sqlcmd, arguments)
        self._conn.commit()

    # def removeDuplicated(self, tablename, keyfld, tableIdentifier):
    #     """
    #     Remove duplicated entries in table. Keep values with lower tableIdentifier.

    #     Parameters
    #     ----------
    #         tablename: str
    #             Name of the table
    #         keyfld: str | list(str)
    #             List of the column names that will be used as unique identifier.
    #             After execution, each entry will have unique value.\\
    #             (e.g. 'S2paperID')
    #         tableIdentifier:
    #             Unique table identifier of entries.
    #             Typically an incremental value used when inserting in table\\
    #             (e.g. 'paperID')
    #     """
    #     # Get duplicated values
    #     df = self.findDuplicated(tablename, keyfld, tableIdentifier)

    #     if df is None:
    #         print("No duplicated entries.")
    #         return

    #     # Sort info to remove
    #     index = f"min_{tableIdentifier}"
    #     columns = df.columns[1:-1].tolist()
    #     arguments = np.concatenate(
    #         (df[columns].values, df[index].values[:, None]), axis=1
    #     )

    #     # Make sure we have a list of tuples; necessary for mysql
    #     arguments = list(map(tuple, arguments))

    #     sqlcmd = SQL("DELETE FROM {} WHERE ").format(Identifier(tablename))
    #     sqlcmd += SQL("{}=%s").format(Identifier(columns[0]))
    #     for c in columns[1:]:
    #         sqlcmd += SQL(" AND {}=%s").format(Identifier(c))
    #     sqlcmd += SQL(" AND {}!=%s").format(Identifier(tableIdentifier))

    #     # print(sqlcmd.as_string(self._c))

    #     self._c.executemany(sqlcmd, arguments)
    #     self._conn.commit()

    #     return

    def execute(self, sqlcmd):
        """
        Execute SQL command received as parameter
        """
        self._c.execute(sqlcmd)
        return
