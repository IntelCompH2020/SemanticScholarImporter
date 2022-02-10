#!/usr/bin/env python
# coding: utf-8

# In[1]:


sc.version


# ### Imports

# In[2]:


from configparser import ConfigParser
from pathlib import Path
import pyspark.sql.functions as F
import requests
from pyspark.sql.types import ArrayType, StringType


# ### Define directories

# In[10]:


# Define directories
#
# Relevant directories are read from the config file:
# dir_data:    full path to hdfs directory where the raw data .gz files are stored
# dir_parquet: full path to hdfs directory where the parquet tables will be stored
# version:     Version of Semantic Scholar that is being processed
#              for information purposes only

cf = ConfigParser()
cf.read("../config.cf")

dir_data = Path(cf.get("spark", "dir_data"))
dir_parquet = Path(cf.get("spark", "dir_parquet"))
version = cf.get("spark", "version")
dir_pdfs = Path(cf.get("spark", "dir_pdfs"))


# ### Configuration hdfs

# It is not possible to listdir() directly using Path as it is a hdfs

# In[11]:


# Configuration hdfs
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
hdfs_dir_data = spark._jvm.org.apache.hadoop.fs.Path(dir_data.as_posix())

print(hdfs_dir_data)

# Get selected version
releases = sorted(
    [
        f.getPath().getName()
        for f in fs.listStatus(hdfs_dir_data)
        if f.isDirectory() and f.getPath().getName().isdigit()
    ]
)
version = version.replace("-", "")
if version == "last":
    version = releases[-1]
if version not in releases:
    print(f"Version {version} not found")
    print(f"Available versions: {releases}")

hdfs_dir_data_files = spark._jvm.org.apache.hadoop.fs.Path(
    dir_data.joinpath(version).as_posix()
)
hdfs_dir_parquet = spark._jvm.org.apache.hadoop.fs.Path(dir_parquet.as_posix())
hdfs_dir_version = spark._jvm.org.apache.hadoop.fs.Path(
    dir_parquet.joinpath(version).as_posix()
)

# Create output directories if they do not exist
# !hadoop dfs ...
# !hadoop dfs -put 20220201 /export/ml4ds/IntelComp/Datalake/SemanticScholar/

if not fs.exists(hdfs_dir_parquet):
    fs.mkdirs(hdfs_dir_parquet)

if not fs.exists(hdfs_dir_version):
    fs.mkdirs(hdfs_dir_version)


# ### Auxiliary functions

# In[13]:


def normalize(text):
    """
    Removes extra spaces in text
    """
    if isinstance(text, str):
        text = " ".join(text.split())
    return text


def get_pdf(pdf_list):
    """
    Gets the first valid pdf url for a paper
    """
    pdf_list = [pdf for pdf in pdf_list if pdf.endswith(".pdf")]
    if len(pdf_list) > 0:
        return pdf_list[0]
    else:
        return None


#
# Create user defined functions to apply in dataframes
#

# Obtain ID from author
take_id = F.udf(lambda x: normalize(x[0] if len(x) > 0 else None), StringType())

# For each paper get all authors
take_authors_ids = F.udf(
    lambda x: [normalize(el[0] if len(el) > 0 else None) for el in x],
    ArrayType(StringType()),
)

# Remove extra spaces
norm_string = F.udf(normalize, StringType())

# Get first valid pdf url
get_first_pdf = F.udf(get_pdf, StringType())


# ### Read data files

# In[12]:


get_ipython().run_cell_magic('time', '', '\n#\xa0Read data files\n#\n# Create a spark df with all the papers in all json files\n\ndf = spark.read.json(dir_data.joinpath(version).as_posix())')


# ### Create papers dataframe and save as parquet file

# In[ ]:


get_ipython().run_cell_magic('time', '', '\n# Create papers dataframe and save as parquet file\n#\n# Papers table will be created keeping only a subset of desired columns\n# It is then stored in disk as a parquet file\n\n# Columns to save\ncolumns = [\n    "id",\n    "title",\n    "paperAbstract",\n    "s2Url",\n    "pdfUrls",\n    "year",\n    "sources",\n    "doi",\n    "doiUrl",\n    "pmid",\n    "magId",\n    "fieldsOfStudy",\n    "journalName",\n    "journalPages",\n    "journalVolume",\n    "venue",\n]\n# Select papers info\ndf_papers = df.select(columns)\n\n# Clean info\nfor c in columns:\n    if df.select(c).dtypes[0][1] == "string":\n        df_papers = df_papers.withColumn(c, norm_string(c))\n\n# Save dataframe as parquet\ndf_papers.write.parquet(\n    dir_parquet.joinpath(version).joinpath("papers.parquet").as_posix(),\n    mode="overwrite",\n)\n\nprint(\'Number of papers in S2 version \' + version + \':\', df_papers.count())')


# ### Create authors dataframe and save as parquet file

# In[ ]:


get_ipython().run_cell_magic('time', '', '\n# Create authors dataframe and save as parquet file\n#\n# Authors table will be created from all authors listed in every paper\n# - Duplicates will be removed keeping only one row for each author id\n# - Authors with empty ids will also be removed from dataframe\n\n# Select only the authors\ndf_authors = df.select(F.explode("authors").alias("authors"))\n\n# Convert dataframe into two columns (id, author name)\ndf_authors = (\n    df_authors.select("authors.ids", "authors.name")\n    .withColumn("ids", take_id("ids"))\n    .withColumn("name", norm_string("name"))\n    .withColumnRenamed("ids", "id")\n    .drop_duplicates(subset=["id"])\n    .dropna(subset=["id"])\n)\n\n# Save dataframe as parquet\ndf_authors.write.parquet(\n    dir_parquet.joinpath(version).joinpath("authors.parquet").as_posix(), mode="overwrite"\n)\n\nprint(\'Number of authors in S2 version \' + version + \':\', df_authors.count())')


# ### Create citations dataframe and save as parquet file

# In[ ]:


get_ipython().run_cell_magic('time', '', '\n# Create citations dataframe and save as parquet file\n#\n# We create a row paper_source_id -> paper_destination_id\n# by exploding all citations of all papers in the version\n\n# Select paper-authors info\ndf_citations = df.select(["id", "outCitations"])\ndf_citations = (\n    df_citations.withColumn("outCitations", F.explode("outCitations"))\n    .withColumnRenamed("id", "source")\n    .withColumnRenamed("outCitations", "dest")\n)\n\n# Save dataframe as parquet\ndf_citations.write.parquet(\n    dir_parquet.joinpath(version).joinpath("citations.parquet").as_posix(),\n    mode="overwrite",\n)\n\nprint(\'Number of citations in S2 version \' + version + \':\', df_citations.count())')


# ### Create paper_author dataframe and save as parquet file

# In[14]:


get_ipython().run_cell_magic('time', '', '\n# Create paper_author dataframe and save as parquet file\n#\n#\xa0We create a row paper_id -> author_id\n# by exploding all authors of all papers in the version\n\n# Select paper-authors info\ndf_paperAuthor = df.select(["id", "authors"])\ndf_paperAuthor = df_paperAuthor.dropna()\ndf_paperAuthor = (\n    df_paperAuthor.withColumn("authors", F.explode(take_authors_ids("authors.ids")))\n    .withColumnRenamed("id", "paper_id")\n    .withColumnRenamed("authors", "author_id")\n    .dropna()\n)\n\n# Save dataframe as parquet\ndf_paperAuthor.write.parquet(\n    dir_parquet.joinpath(version).joinpath("paper_author.parquet").as_posix(),\n    mode="overwrite",\n)\n\nprint(\'Number of authorships in S2 version \' + version + \':\', df_paperAuthor.count())')


# ### Download PDFs (IN PROGRESS)

# In[15]:


# Get previously downloaded pdfs
list_pdfs = set(
    [x.stem for x in dir_pdfs.iterdir() if x.is_file()]
)

# Select pdfs to download
pdf_urls = (
    df.select(["id", "pdfUrls"])
    .withColumn("pdfUrls", get_first_pdf("pdfUrls"))
    .filter(F.length("pdfUrls") > 0)
)
pdf_urls = pdf_urls.where(~F.col("id").isin(list_pdfs))

pdf_test = pdf_urls.limit(5)


# In[ ]:


## Download PDFs
#
# We download PDFs for all papers with valid a valid pdfUrl
# This option is not activated by default, since the number
# of papers to download would be huge
paper_download = 1

if paper_download:

    def download_pdfs(row, dir_pdfs=dir_pdfs):
        try:
            r = requests.get(row["pdfUrls"], stream=True)
            with dir_pdfs.joinpath(f"{row['id']}.pdf").open("wb") as f:
                f.write(r.content)
        except:
            pass

    pdf_test.foreach(download_pdfs)

