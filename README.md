# SemanticScholarImporter
This repository provides code to save, import and process [SemanticScholar](https://www.semanticscholar.org/) dataset.

Semantic Scholar is a search engine for research articles powered by the Allen Institute for Artificial Intelligence.

## 1. Download raw data

- Copy and rename `config_default.cf` into `config.cf` adjusting options as needed. You just need to fill in the first section `[downloadS2]`:
   * `dir_data` is the path to the directory where you want to save the raw data
   * `S2_API_Key` are your credentials for downloading the Semantic Scholar dataset. You should get in contact with the Allen Institute to get them
   * `version` is the version you want to download (default value `latest` is normally what you will want)

- Run the following command line 

```
python downloadSS.py
```
## 2. pySpark/SemanticScholar_to_parquet.ipynb

This notebook contains all the code to transform original json files to parquet files. This notebook should be adapted to run in your pySpark cluster

It creates the following tables in parquet format:
- **papers**: information related to papers
- **authors**: maps author id to their name
- **paper-author**: maps each paper to its authors
- **citations**: source paper to referenced paper

Additionally, the notebook includes a section to create some domain specific datasets for Artificial Intelligence and Cancer
