####################################################
# Imports

import argparse
from configparser import ConfigParser
from pathlib import Path

from dbManager.S2manager import S2manager


def main(
    interface=False,
    reset=False,
    papers=False,
    authors=False,
    sources=False,
    fields=False,
    authorship=False,
):
    """
    Parameters
    ----------    
    interface:  Use terminal interface
    reset:      Reset database
    papers:     Import papers from data files
    authors:    Import authors from data files
    sources:    Import citations from data files
    fields:     Import fields, journals and volumes of study data from data files
    authorship: Import authorship from data files
    """

    ####################################################
    # Read connection parameters from config.cf

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
    if interface:
        while True:
            print("\nSelect option:")
            print("1. Reset database")
            print("2. Import papers from data files")
            print("3. Import authors from data files")
            print("4. Import sources data for papers in database")
            print(
                "5. Import fields, journals and volumes of study data from data files"
            )
            print("6. Import authorship from data files")
            print("0. Quit")
            selection = input()

            if selection == "1":
                print("Previous info will be deleted. Continue?\n[y]/[n]")
                selection = input()
                if selection == "y":
                    print("Regenerating the database. Existing data will be removed.")
                    DB.deleteDBtables()
                    DB.create_database("dbManager/create_sql.sql")

            elif selection == "2":
                print("Importing papers data.")
                DB.importPapers(dir_data, dbncpu, dbchunksize)

            elif selection == "3":
                print("Importing authors data.")
                DB.importAuthorsData(dir_data, dbncpu, dbchunksize)

            elif selection == "4":
                print("Importing sources data.")
                DB.importCitations(dir_data, dbncpu, "references", dbchunksize)
                # DB.importSourceTypes(dbncpu, "references", dbchunksize)

            elif selection == "5":
                print("Importing venues, journals and fields of study data.")
                DB.importFields(dir_data, dbncpu, dbchunksize)

            elif selection == "6":
                print("Importing authorship data.")
                DB.importAuthorship(dir_data, dbncpu, dbchunksize)

            elif selection == "0":
                return

            else:
                print("Invalid option")
    else:
        if reset:
            print("Regenerating the database. Existing data will be removed.")
            DB.deleteDBtables()
            DB.create_database("dbManager/create_sql.sql")

        if papers:
            print("Importing papers data.")
            DB.importPapers(dir_data, dbncpu, dbchunksize)

        if authors:
            print("Importing authors data.")
            DB.importAuthorsData(dir_data, dbncpu, dbchunksize)

        if sources:
            print("Importing sources data.")
            DB.importCitations(dir_data, dbncpu, "references", dbchunksize)
            # DB.importSourceTypes(dbncpu, "references", dbchunksize)

        if fields:
            print("Importing venues, journals and fields of study data.")
            DB.importFields(dir_data, dbncpu, dbchunksize)

        if authorship:
            print("Importing authorship data.")
            DB.importAuthorship(dir_data, dbncpu, dbchunksize)


if __name__ == "__main__":

    description = """
─────────────────────────────────────────

    SemanticScholar importer program.

─────────────────────────────────────────────────────────────────────────────

Imports information from SemanticScholar to PostgreSQL database.

Usage:
    · Use "config.cf" file to configure the connection to the database.
    · Use the interface  with "--interface" command or pass arguments
      to select which actions to take.

    If interface is selected other commands will not be considered.

─────────────────────────────────────────────────────────────────────────────

"""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter, description=description,
    )
    parser.add_argument(
        "-I",
        "--interface",
        action="store_true",
        help="Show simple interface in terminal.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset database. This will remove all previously stored information.",
    )
    parser.add_argument(
        "--papers", "-p", action="store_true", help="Import papers from data files"
    )
    parser.add_argument(
        "--authors", "-a", action="store_true", help="Import authors from data files"
    )
    parser.add_argument(
        "--sources",
        "-s",
        action="store_true",
        help="Import sources data for papers in database",
    )
    parser.add_argument(
        "--fields",
        "-f",
        action="store_true",
        help="Import fields, journals and volumes of study data from data files",
    )
    parser.add_argument(
        "--authorship",
        "-u",
        action="store_true",
        help="Import authorship from data files",
    )

    args = vars(parser.parse_args())

    # EITHER [--function_name] or [--interface] MUST BE PASSED
    if not any(args.values()):
        parser.error("No action requested.")

    main(**args)
