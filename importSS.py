####################################################
# Imports

import argparse
from configparser import ConfigParser
from pathlib import Path

from dbManager.S2manager import S2manager

# from downloadSS import download_SS


options = [
    {
        "name": "reset",
        "abbr": None,
        "action": "store_true",
        "title": "Reset database",
    },
    {
        "name": "index",
        "abbr": "i",
        "action": "store_true",
        "title": "Create table indexes",
    },
    {
        "name": "papers",
        "abbr": "p",
        "action": "store_true",
        "title": "Import papers from data files",
    },
    {
        "name": "authors",
        "abbr": "a",
        "action": "store_true",
        "title": "Import authors from data files",
    },
    {
        "name": "sources",
        "abbr": "s",
        "action": "store_true",
        "title": "Import sources data for papers in database",
        "type": "references",
    },
    {
        "name": "fields",
        "abbr": "f",
        "action": "store_true",
        "title": "Import fields, journals and volumes of study data from data files",
    },
    {
        "name": "authorship",
        "abbr": "u",
        "action": "store_true",
        "title": "Import authorship from data files",
    },
]


def manager(DB: S2manager, option, args=[]):
    """
    Executes option
    """
    option_names = [o["name"] for o in options]
    if option not in option_names:
        print(f"Error: invalid option '{option}'")
        return

    if option == "reset":
        print("Regenerating the database. Existing data will be removed.")
        if not DB.getTableNames():
            # Create database if doesn't exist
            DB.executeSQLfile("dbManager/create_sql.sql")
        DB.deleteDBtables()

    elif option == "index":
        print("Creating table indexes.")
        DB.executeSQLfile("dbManager/create_index.sql")

    elif option == "papers":
        print("Importing papers data.")
        DB.importPapers(*args)

    elif option == "authors":
        print("Importing authors data.")
        DB.importAuthorsData(*args)

    elif option == "sources":
        print("Importing sources data.")
        DB.importCitations(*args)
        # DB.importSourceTypes(dbncpu, "references", dbchunksize)

    elif option == "fields":
        print("Importing venues, journals and fields of study data.")
        DB.importFields(*args)

    elif option == "authorship":
        print("Importing authorship data.")
        DB.importAuthorship(*args)

    # if option == "to-parquet":
    #     print("Saving data files to parquet.")
    #     download_SS(*args)


def menu():
    print("\nSelect option:")
    for i, opt in enumerate(options):
        print(f"{i+1}. {opt['title']}")
    print("0. Exit")


def interface(DB, args):
    num_options = len(options)
    while True:
        menu()
        selection = input()
        try:
            selection = int(selection)
        except:
            print("Invalid option")
            continue

        if selection not in range(num_options + 1):
            print("Invalid option")

        else:
            if selection == 0:
                exit()

            opt = options[selection - 1].get("name")
            if selection == 1:
                print("Previous info will be deleted. Continue?\n[y]/[n]")
                selection = input()
                if selection == "y":
                    manager(DB, opt)
                else:
                    continue
            else:
                if opt == "sources":
                    manager(DB, opt, args + [options[selection - 1].get("type")])
                else:
                    manager(DB, opt, args)


def main():

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
    ####################################################
    # Parse args

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=description,
    )
    parser.add_argument(
        "-I",
        "--interface",
        action="store_true",
        help="Show simple interface in terminal.",
    )
    for arg in options:
        # print(arg)
        if arg["abbr"]:
            parser.add_argument(
                f'-{arg["abbr"]}',
                f'--{arg["name"]}',
                action=arg["action"],
                help=arg["title"],
            )
        else:
            parser.add_argument(
                f'--{arg["name"]}',
                action=arg["action"],
                help=arg["title"],
            )
    # parser.add_argument(
    #     "--to-parquet",
    #     action="store_true",
    #     help="Convert files in dir_data/version to parquet files",
    # )
    args = vars(parser.parse_args())

    # EITHER [--function_name] or [--interface] MUST BE PASSED
    if not any(args.values()):
        parser.error("No action requested.")

    ####################################################
    # Read connection parameters from config.cf

    cf = ConfigParser()
    cf.read("config.cf")

    dbuser = cf.get("database", "dbuser")
    dbpass = cf.get("database", "dbpass")
    dbhost = cf.get("database", "dbhost")
    dbport = cf.get("database", "dbport")
    dbname = cf.get("database", "dbname")
    dbncpu = int(cf.get("import", "ncpu"))
    dbchunksize = int(cf.get("import", "chunksize"))

    #########################
    # Datafiles
    dir_data = Path(cf.get("data", "dir_data"))
    version = cf.get("data", "version")

    releases = sorted(
        [d.name for d in dir_data.iterdir() if d.is_dir() and d.name.isdigit()]
    )
    version = version.replace("-", "")
    if version == "last":
        version = releases[-1]
    if version not in releases:
        print(f"Version {version} not found")
        return
    data_files_dir = dir_data.joinpath(version)

    ####################################################
    # Database connection

    DB = S2manager(
        dbname=dbname, dbhost=dbhost, dbuser=dbuser, dbpass=dbpass, dbport=dbport
    )

    ####################################################
    arguments = [data_files_dir, dbncpu, dbchunksize]

    # 1. If activated, display console
    if args["interface"]:
        interface(DB, arguments)
    else:
        for opt in options:
            if args[opt["name"]]:
                if opt["name"] == "sources":
                    manager(DB, opt["name"], arguments + [opt["type"]])
                manager(DB, opt["name"], arguments)

        # if args["to-parquet"]:
        #     print("Saving data files to parquet.")
        #     download_SS(version=version, dest_dir=dir_data)


if __name__ == "__main__":

    main()
