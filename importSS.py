####################################################
# Imports

import argparse
import time
from configparser import ConfigParser
from pathlib import Path

from dbManager.S2manager import S2manager


def main():
    """
    """

    ####################################################
    # Read connection parameters

    cf = ConfigParser()
    cf.read("config.cf")

    dbuser = cf.get("database", "dbuser")
    dbpass = cf.get("database", "dbpass")
    dbhost = cf.get("database", "dbhost")
    dbport = cf.get("database", "dbport")
    dbname = cf.get("database", "dbname")
    dbncpu = int(cf.get("data", "ncpu"))
    dbchunksize = int(cf.get("data", "chunksize"))

    #########################
    # Datafiles

    dir_data = Path(cf.get("data", "dir_data"))

    ####################################################
    # Database connection

    DB = S2manager(
        dbname=dbname, dbhost=dbhost, dbuser=dbuser, dbpass=dbpass, dbport=dbport
    )

    ####################################################
    # 1. If activated, display console
    # print(interface)
    # if interface:
    while True:
        print("\nSelect option:")
        print("1. Reset database")
        print("2. Import papers from data files")
        print("3. Import authors from data files")
        print("4. Import citations from data files")
        print("5. Import fields, journals and volumes of study data from data files")
        print("6. Import authorship from data files")

        print("0. Quit")
        selection = input()

        if selection == "1":
            print("Previous info will be deleted. Continue?\n[y]/[n]")
            selection = input()
            if selection == "y":
                print("Regenerating the database. Existing data will be removed.")
                # The following method deletes all existing tables, and create them
                # again without data.
                DB.drop_database()
                DB.create_database("dbManager/create_sql.sql")

        elif selection == "2":
            print("Importing papers data ...")
            t0 = time.time()
            DB.importPapers(dir_data, dbncpu, dbchunksize)
            print(f"\nTotal time: {time.time()-t0}")

        elif selection == "3":
            print("Importing authors data ...")
            t0 = time.time()
            DB.importAuthorsData(dir_data, dbncpu)
            print(f"\nTotal time: {time.time()-t0}")

        elif selection == "4":
            # 4. If activated, citations data
            # will be imported from S2 data files
            print("Importing citations data ...")
            # DB.importCitations(dir_data, dbncpu, dbchunksize)
            DB.importSources(dbncpu, stype="references")

        elif selection == "5":
            # 5. If activated, journals, volumes, and Fields of Study data
            # will be imported from S2 data files
            print("Importing fields, journals and venues of study data ...")
            DB.importFields(dir_data, dbncpu, dbchunksize)

        elif selection == "6":
            # 6. If activated, authorship data
            # will be imported from S2 data files
            print("Importing authorship data ...")
            DB.importAuthors(dir_data, dbncpu, dbchunksize)

        elif selection == "0":
            return

        else:
            print("Invalid option")


if __name__ == "__main__":

    # parser = argparse.ArgumentParser()
    # parser.add_argument("--dbuser", action=, help="")
    # parser.add_argument("--dbpass", action=, help="")
    # parser.add_argument("--dbhost", action=, help="")
    # parser.add_argument("--dbport", action=, help="")
    # parser.add_argument("--dbname", action=, help="")
    # parser.add_argument("--dir_data", action=, help="")
    # parser.add_argument("--ncpu", action=, help="")
    # parser.add_argument("--chunksize", action=, help="")
    # parser.add_argument(
    #     "--interface", action="store_true", help="",
    # )

    # args = parser.parse_args()

    # main(interface=args.interface)

    main()
