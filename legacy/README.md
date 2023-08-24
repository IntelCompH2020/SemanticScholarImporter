# SemanticScholarImporter
This repository provides code to save, import and process [SemanticScholar](https://www.semanticscholar.org/) dataset.

Semantic Scholar is a search engine for research articles powered by the Allen Institute for Artificial Intelligence. This database is generated from the Semantic Scholar Open Research Corpus. JSON files are used to generate a relational database with author and citation tables. This dataset is still under construction to incorporate other metadata not provided directly by Semantic Scholar.

As of Feb 2022 we include a notebook to create parquet files for the papers and authors information

# Usage
Copy and rename `config_default.cf` into `config.cf` adjusting options as needed.

Create a new environment.
```
python -m venv S2importer
```

Then, activate environment.

```
source S2importer/bin/activate
```

And install all dependencies:

```
pip install -r requirements.txt
```

# Executable files

## 1. importSS.py
Executes the necessary code to import from json files into a PostgreSQL database.

### Usage
```
python importSS.py [-h] [-I] [--reset] [-i] [-p] [-a] [-s] [-f] [-u]
```
Optional arguments:
| short |     long     |                            Description                             |
|:-----:|:------------:|:------------------------------------------------------------------:|
|   -h  | --help       | Show help message and exit.                                        |
|   -I  | --interface  | Show simple interface in terminal.                                 |
|       | --reset      | Reset database. Removes all previous data.                         |
|   -i  | --index      | Create table indexes.                                              |
|   -p  | --papers     | Import papers from data files.                                     |
|   -a  | --authors    | Import authors from data files.                                    |
|   -s  | --sources    | Import sources data for papers in database.                        |
|   -f  | --fields     | Import fields, journals and volumes of study data from data files. |
|   -u  | --authorship | Import authorship from data files.                                 |

Either _interface_ or other options must be selected.

If _interface_ option is selected, a simple menu will be displayed in terminal and other commands will not be considered. This menu offers the possibility to execute the same options in the desired order.

## 2. downloadSS.py
Downloads SemanticScholar version based on configuration file. May be imported into some other script or directly executed with:
```
python downloadSS.py
```

## 3. pySpark/SemanticScholar_to_parquet.ipynb

This notebook contains all the code to transform original json files to parquet files. It creates the following tables in the directory specified in `config.cf`:
- **papers**: information related to papers
- **authors**: maps author id to their name
- **paper-author**: maps each paper to its authors
- **citations**: source paper to referenced paper

Additionally, the PDFs for the available papers can be also downloaded.
