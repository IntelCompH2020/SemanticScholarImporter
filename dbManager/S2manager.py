"""
Datamanager for importing Semantic Scholar papers
into a Postgres database

Oct 2021

@authors: Jerónimo Arenas García (jeronimo.arenas@uc3m.es)
          José Antonio Espinosa Melchor (joespino@pa.uc3m.es)


"""

import gzip
import json
import re
from functools import partial
from multiprocessing import Pool

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

# from dbManager.dbBase.base_dm_postgres import BaseDMsql
from dbManager.dbBase.base_dm_sql_alch import BaseDMsql

try:
    # UCS-4
    regex = re.compile("[\U00010000-\U0010ffff]")
except re.error:
    # UCS-2
    regex = re.compile("[\uD800-\uDBFF][\uDC00-\uDFFF]")

"""
Some functions need to be defined outside the class for allowing 
parallel processing of the Semantic Scholar files. It is necessary
to do so to make pickle serialization work
"""


def ElementInList(source_list, search_string):
    if search_string in source_list:
        return True
    else:
        return False


def normalize(data):
    """ Remove spaces and empty data """
    data = data.strip()
    if len(data) > 0:
        return data
    return None


def get_gzfiles(dir_data):
    """ Get files  """
    return sorted([el for el in dir_data.iterdir() if el.name.startswith("s2-corpus")])


def read_papers_infile(gz_file):
    """ Load papers information in file """
    try:
        # Read json and separate papers
        # with gzip.open(gz_file, "rt", encoding="utf8") as f:
        #     papers_infile = f.read().replace("}\n{", "},{")
        with gzip.open(gz_file, "rt", encoding="utf8") as f:
            papers_infile = [json.loads(l) for l in f.readlines()]
    except:
        print(f"Error with file {gz_file}")
        return []
    # papers_infile = json.loads("[" + papers_infile + "]")
    return papers_infile


def process_paper(paperEntry):
    """
    This function takes a dictionary with paper information as input
    and returns a list to insert in S2papers
    """
    try:
        year = int(paperEntry["year"])
    except:
        year = 9999

    try:
        magid = int(paperEntry["magid"])
    except:
        magid = None
    try:
        pmid = int(paperEntry["pmid"])
    except:
        pmid = None

    paper_list = [
        paperEntry["id"],
        regex.sub(" ", paperEntry["title"]),
        regex.sub(" ", paperEntry["title"].lower()),
        regex.sub(" ", paperEntry["paperAbstract"]),
        regex.sub(" ", paperEntry["s2Url"]),
        "\t".join(paperEntry["pdfUrls"]),
        year,
        ElementInList(paperEntry["sources"], "DBLP"),
        ElementInList(paperEntry["sources"], "Medline"),
        paperEntry["doi"],
        paperEntry["doiUrl"],
        pmid,
        magid,
    ]

    return paper_list


def process_paperFile(gzf):
    """
    Process Semantic Scholar gzip file, and extract a list of
    journals, a list of venues, a list of fields of study, and a
    list wih paper information to save in the S2papers table

    Parameters
    ----------
    gzf: String
        Name of the file to process

    Returns
    -------
    A list containing 4 lists: papers in file, unique journals in file,
    unique venues in file, unique fields in file
    """

    # Get papers in file
    papers_infile = read_papers_infile(gzf)

    # Extract venues getting rid of repetitions
    thisfile_venues = set(normalize(paper["venue"]) for paper in papers_infile)
    thisfile_venues.discard(None)
    thisfile_venues = [[el] for el in thisfile_venues]

    # Extract journals getting rid of repetitions
    thisfile_journals = set(normalize(paper["journalName"]) for paper in papers_infile)
    thisfile_journals.discard(None)
    thisfile_journals = [[el] for el in thisfile_journals]

    # Extract all fields, and flatten before getting rid of repetitions
    # Flatenning is necessary because each paper has a list of fields
    thisfile_fields = set(
        normalize(item) for paper in papers_infile for item in paper["fieldsOfStudy"]
    )
    thisfile_fields.discard(None)
    thisfile_fields = [[el] for el in thisfile_fields]

    # Extract fields for the S2papers table
    thisfile_papers = [process_paper(el) for el in papers_infile]

    return [thisfile_papers, thisfile_venues, thisfile_journals, thisfile_fields]


def process_Citations(gzf, stype="references"):
    """
    This function takes a zfile with paper information as input
    and returns a list ready to insert in table

    stype: String
        {"references", "citations"}. Default: "references"
        "references": paper1 cites paper2
        "citations": paper1 is cited by paper2
    """

    # Get papers in file
    papers_infile = read_papers_infile(gzf)

    # Process each paper
    cite_list = []
    for paperEntry in papers_infile:
        if stype == "references":
            if len(paperEntry["outCitations"]):
                for el in paperEntry["outCitations"]:
                    cite_list.append([paperEntry["id"], el])
        else:
            if len(paperEntry["inCitations"]):
                for el in paperEntry["inCitations"]:
                    cite_list.append([el, paperEntry["id"]])
    return cite_list


def process_Authors(gzf):
    """
    This function takes a zfile with paper information as input
    and returns a list of all authors in the file ready to insert in table
    """

    # Get papers in file
    papers_infile = read_papers_infile(gzf)

    # Process each paper
    thisfile_authors = []
    for paperEntry in papers_infile:
        if len(paperEntry["authors"]):
            for author in paperEntry["authors"]:
                auth_id = author["ids"]
                auth_nm = normalize(author["name"])
                if len(auth_id) and auth_nm is not None:
                    thisfile_authors.append((int(auth_id[0]), auth_nm))
    return thisfile_authors


def process_Authorship(gzf):
    """
    This function takes a zfile with paper information as input
    and returns a list ready to insert in paperAuthor (paper-authors information)
    """

    # Get papers in file
    papers_infile = read_papers_infile(gzf)

    # Get list of authors for each paper in file
    lista_paper_author = []
    for paper in papers_infile:
        author_list = [
            (paper["id"], int(el["ids"][0]))
            for el in paper["authors"]
            if len(el["ids"])
        ]
        lista_paper_author += author_list

    return lista_paper_author


def process_Fields(gzf, venues_dict, journals_dict, fields_dict, papers_set):
    """
    This function takes a zfile with paper information as input
    and returns a list(dict) to transform into DataFrame and
    ready to insert in paperField, paperJournal and paperVenues
    """

    # Get papers in file
    papers_infile = read_papers_infile(gzf)

    papers_fields = []
    papers_journals = []
    papers_venues = []

    for paper in papers_infile:
        paper_id = paper["id"]
        # If paper in database
        if paper_id in papers_set:
            # Fields
            fields_list = []
            for el in paper["fieldsOfStudy"]:
                el = normalize(el)
                try:
                    fields_list.append(
                        {"S2paperID": paper_id, "fieldID": fields_dict[el]}
                    )
                except:
                    pass
            papers_fields.extend(fields_list)

            # Journal
            journal = normalize(paper["journalName"])
            journal_vol = normalize(paper["journalVolume"])
            journal_pag = normalize(paper["journalPages"])
            try:
                papers_journals.append(
                    {
                        "S2paperID": paper_id,
                        "journalID": journals_dict[journal],
                        "journalVolume": journal_vol,
                        "journalPages": journal_pag,
                    }
                )
            except:
                pass

            # Venue
            venue = normalize(paper["venue"])
            try:
                papers_venues.append(
                    {"S2paperID": paper_id, "venueID": venues_dict[venue]}
                )
            except:
                pass

    return papers_fields, papers_journals, papers_venues


def get_sources(paper, stype="references"):
    """ Use the SemanticScholar API to obtain the requested information.
        Reference/Citation fields:
            -intents
            -isInfluential
            -paperId

        Parameters
        ----------
        paper: String
            Semantic Scholar unique identifier.
        stype: String
            {"references", "citations"}. Default: "references"
            "references": paper1 cites paper2
            "citations": paper1 is cited by paper2

        Returns
        -------
        df_reference: DataFrame
            Dataframe where each row is a referece/citation of the paper.
            
    """

    # Initialize return
    df_reference = pd.DataFrame(
        columns=[
            "S2paperID1",
            "S2paperID2",
            "isInfluential",
            "BackgrIntent",
            "MethodIntent",
            "ResultIntent",
        ]
    )

    # Query configuration
    offset = 0
    limit = 1000
    next = True

    ref_list = []

    while next:
        # Request reference
        query = f"https://api.semanticscholar.org/graph/v1/paper/{paper}/{stype}?offset={offset}&limit={limit}&fields=intents,isInfluential,paperId"
        resp = requests.get(url=query)
        data = resp.json()

        # Keep searching if there is `next` value
        try:
            offset = data["next"]
        except:
            next = False

        # Get references
        try:
            aux = pd.DataFrame.from_dict(data["data"])

            # Get reference paper ID
            if stype == "references":
                aux["S2paperID1"] = paper
                aux["S2paperID2"] = aux["citedPaper"].apply(
                    lambda x: x.get("paperId", np.nan)
                )
            else:
                aux["S2paperID1"] = aux["citingPaper"].apply(
                    lambda x: x.get("paperId", np.nan)
                )
                aux["S2paperID2"] = paper

            # Get intents
            def split_intents(col):
                intents = {
                    "background": [False],
                    "methodology": [False],
                    "result": [False],
                }
                [intents.update({el: [True]}) for el in col]
                return pd.DataFrame.from_dict(intents)

            aux[["BackgrIntent", "MethodIntent", "ResultIntent"]] = pd.concat(
                aux["intents"].apply(split_intents).values.tolist()
            ).reset_index(drop=True)

            aux = aux[
                [
                    "S2paperID1",
                    "S2paperID2",
                    "isInfluential",
                    "BackgrIntent",
                    "MethodIntent",
                    "ResultIntent",
                ]
            ]

            ref_list.append(aux)

        except Exception as e:
            # print(e)
            pass

    if ref_list:
        return df_reference.append(pd.concat(ref_list), ignore_index=True).dropna()
    return df_reference


class S2manager(BaseDMsql):
    # class S2manager:
    #     def __init__(self, dbuser, dbpass, dbhost, dbport, dbname):

    #         # Database configuration
    #         self.dbuser = dbuser
    #         self.dbpass = dbpass
    #         self.dbhost = dbhost
    #         self.dbport = dbport
    #         self.dbname = dbname
    #         self.engine = create_engine(
    #             f"postgresql://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}"
    #         )

    # def create_database(self, file):
    #     """ Create database from file """

    #     with open(file, "r") as f:
    #         self.execute(f.read())
    # with self.engine.connect() as con:
    #     file = open(file)
    #     query = sql.text(file.read()).execution_options(autocommit=True)

    #     con.execute(query)

    def read_table_set(self, table, col):
        """ Read a table column and obtain all its unique values """

        df = self.readDBtable(table, selectOptions=col)
        values = set(df[col].tolist())
        return values

    def importPapers(self, dir_data, ncpu, chunksize=None):
        """
        Import data from Semantic Scholar compressed data files
        available at the indicated location
        Paper data, venues, journals and fields will be imported.
        """

        print("Filling in table S2papers")

        # Get data files
        gz_files = get_gzfiles(dir_data)

        # Read tables to avoid repeated values
        papers_set = self.read_table_set("S2papers", "S2paperID")
        venues_set = self.read_table_set("S2venues", "venueName")
        journs_set = self.read_table_set("S2journals", "journalName")
        fields_set = self.read_table_set("S2fields", "fieldName")

        # Aux function that will insert data into each table
        # Defined here because it's the same wheter multiple cpus are used or not
        def populate(file_data):
            """ Aux function to insert data into database """
            (
                thisfile_papers,
                thisfile_venues,
                thisfile_journals,
                thisfile_fields,
            ) = file_data

            # S2papers
            columns = [
                "S2paperID",
                "title",
                "lowertitle",
                "paperAbstract",
                "s2Url",
                "pdfUrls",
                "year",
                "isDBLP",
                "isMEDLINE",
                "doi",
                "doiUrl",
                "pmid",
                "magId",
            ]
            set_new_data = set(p[0] for p in thisfile_papers)
            set_new_data = set_new_data.difference(papers_set)
            thisfile_papers = [p for p in thisfile_papers if p[0] in set_new_data]
            if thisfile_papers:
                self.insertInTable("S2papers", columns, thisfile_papers, chunksize)
            papers_set.update(set_new_data)

            # S2venues
            columns = ["venueName"]
            set_new_data = set(p[0] for p in thisfile_venues)
            set_new_data = set_new_data.difference(venues_set)
            thisfile_venues = [p for p in thisfile_venues if p[0] in set_new_data]
            if thisfile_venues:
                self.insertInTable("S2venues", columns, thisfile_venues, chunksize)
            venues_set.update(set_new_data)

            # S2journals
            columns = ["journalName"]
            set_new_data = set(p[0] for p in thisfile_journals)
            set_new_data = set_new_data.difference(journs_set)
            thisfile_journals = [p for p in thisfile_journals if p[0] in set_new_data]
            if thisfile_journals:
                self.insertInTable("S2journals", columns, thisfile_journals, chunksize)
            journs_set.update(set_new_data)

            # S2fields
            columns = ["fieldName"]
            set_new_data = set(p[0] for p in thisfile_fields)
            set_new_data = set_new_data.difference(fields_set)
            thisfile_fields = [p for p in thisfile_fields if p[0] in set_new_data]
            if thisfile_fields:
                self.insertInTable("S2fields", columns, thisfile_fields, chunksize)
            fields_set.update(set_new_data)

        if ncpu:
            # Parallel processing
            with tqdm(total=len(gz_files), leave=None) as pbar:
                with Pool(ncpu) as p:
                    for file_data in p.imap(process_paperFile, gz_files):
                        # Populate tables with the new data
                        populate(file_data)
                        pbar.update()

        else:
            with tqdm(total=len(gz_files), leave=None) as pbar:
                for gzf in gz_files:
                    file_data = process_paperFile(gzf)
                    # Populate tables with the new data
                    populate(file_data)
                    pbar.update()

    def importAllSources(self, ncpu, stype="references", chunksize=None):
        """ Imports References/Citation information from API """

        print("Obtaining S2paperIDs")

        # Read tables to avoid repeated values
        papers_set = self.read_table_set("S2papers", "S2paperID")

        def chunks(l, n):
            """Yields successive n-sized chunks from list l."""
            for i in range(0, len(l), n):
                yield l[i : i + n]

        ch_size = 100  # Number of papers to process at a time
        remaining = len(papers_set)

        with tqdm(total=np.ceil(len(papers_set) / ch_size), leave=None) as chunk_bar:
            chunk_bar.set_description("Processing papers chunks")
            for chk in chunks(list(papers_set), ch_size):
                papers_references = []
                if ncpu:
                    # Parallel processing
                    with tqdm(total=min(ch_size, remaining), leave=None) as pbar:
                        with Pool(ncpu) as p:
                            for df_references in p.imap(
                                partial(get_sources, stype=stype,), chk
                            ):
                                papers_references.append(df_references)
                                pbar.update()
                else:
                    with tqdm(total=min(ch_size, remaining), leave=None) as pbar:
                        for paper in chk:
                            papers_references.append(get_sources(paper, stype=stype))
                            pbar.update()

                # Concat all references
                df_papers_references = pd.concat(papers_references)
                # Insert into table
                if not df_papers_references.empty:
                    # Remove papers not present in database
                    df_papers_references = df_papers_references[
                        df_papers_references["S2paperID2"].isin(papers_set)
                    ]
                    # Introduce new data
                    self.insertInTable(
                        "citations",
                        df_papers_references.columns.tolist(),
                        df_papers_references.values,
                        chunksize,
                    )
                remaining = remaining - ch_size
                chunk_bar.update()

    def importSourceTypes(self, ncpu, stype="references", chunksize=None):
        """ Imports References/Citation types for citations already in database """

        def implode(df, by="S2paperID1", col="S2paperID2"):
            """
            Convert separate rows with same 'by' and multiple 'col'
            to single row of one 'by' and a list of 'col'

            E.g.\\
            |   |   by  |  col  ||\\
            |:-:|:-----:|:-----:|\\
            | 1 | ref_1 | ref_x |\\
            | 2 | ref_1 | ref_y |\\
            | 3 | ref_2 | ref_x |\n
            Is transformed into:\n
            |   |   by  |       col      |\\
            |:-:|:-----:|:--------------:|\\
            | 1 | ref_1 | [ref_x, ref_y] |\\
            | 2 | ref_2 |      ref_x     |
            """
            return df.groupby(by).agg({col: lambda x: x.tolist()}).reset_index()

        print("Processing citations in database")
        # Read citations table by chunks
        for chunk in self.readDBchunks(
            "citations",
            "citationID",
            selectOptions=["S2paperID1", "S2paperID2"],
            chunksize=chunksize,
            limit=None,
        ):

            cit_df = implode(chunk).set_index("S2paperID1")
            cit_df = cit_df["S2paperID2"].to_dict()

            papers_references = []
            if ncpu:
                # Parallel processing
                with Pool(ncpu) as p:
                    for df_sources in p.imap(
                        partial(get_sources, stype=stype,), cit_df.keys()
                    ):
                        # Remove references not present in database
                        paper = df_sources.iloc[0, 0]
                        sources = cit_df[paper]
                        df_sources = df_sources.loc[
                            df_sources["S2paperID2"].isin(sources)
                        ]
                        papers_references.append(df_sources)
            else:
                for paper, sources in cit_df.items():
                    df_sources = get_sources(paper, stype=stype)
                    # Remove references not present in database
                    df_sources = df_sources.loc[df_sources["S2paperID2"].isin(sources)]
                    papers_references.append(df_sources)

            # Concat all references
            df_papers_references = pd.concat(papers_references)
            # Insert into table
            if not df_papers_references.empty:
                # Update table with new data
                self.setField(
                    "citations",
                    ["S2paperID1", "S2paperID2"],
                    ["isInfluential", "BackgrIntent", "MethodIntent", "ResultIntent"],
                    df_papers_references.values,
                )

    def importCitations(self, dir_data, ncpu, chunksize=None, stype="references"):
        """ Imports Citation information from zip files.
        
        stype: String
            {"references", "citations"}. Default: "references"
            "references": paper1 cites paper2
            "citations": paper1 is cited by paper2
        """

        print("Obtaining S2paperIDs")

        # Read tables to avoid repeated values
        papers_set = self.read_table_set("S2papers", "S2paperID")

        # Get data files
        gz_files = get_gzfiles(dir_data)

        def populate(cite_list):
            """ Aux function to insert data into database """
            # Ensure all papers exist in database
            aux_list = [
                (c0, c1)
                for c0, c1 in cite_list
                if c0 in papers_set and c1 in papers_set
            ]

            columns = ["S2paperID1", "S2paperID2"]
            # Introduce new data
            self.insertInTable("citations", columns, aux_list, chunksize)

        if ncpu:
            # Parallel processing
            with tqdm(total=len(gz_files), leave=None) as pbar:
                with Pool(ncpu) as p:
                    for cite_list in p.imap(
                        partial(process_Citations, stype=stype,), gz_files
                    ):
                        populate(cite_list)
                        pbar.update()
        else:
            with tqdm(total=len(gz_files), leave=None) as pbar:
                for gzf in gz_files:
                    cite_list = process_Citations(gzf, stype)
                    populate(cite_list)
                    pbar.update()

    def importFields(self, dir_data, ncpu, chunksize=None):
        """ Imports Fields, Journals and Volumes of Study associated to each paper """

        # We extract venues, journals and fields as dictionaries
        # to name-id
        print("Obtaining venues, journals and fields dictionaries")
        venues_dict = self.readDBtable("S2venues", ["venueName", "venueID"])
        venues_dict = dict(venues_dict.values.tolist())

        journals_dict = self.readDBtable("S2journals", ["journalName", "journalID"])
        journals_dict = dict(journals_dict.values.tolist())

        fields_dict = self.readDBtable("S2fields", ["fieldName", "fieldID"])
        fields_dict = dict(fields_dict.values.tolist())

        print("Obtaining S2paperIDs")
        papers_set = self.read_table_set("S2papers", "S2paperID")

        # Get data files
        gz_files = get_gzfiles(dir_data)

        def populate(all_data):
            """ Aux function to insert data into database """
            papers_fields, papers_journals, papers_venues = all_data

            # Introduce new data
            # FIELDS
            df = pd.DataFrame(papers_fields)
            self.insertInTable("paperField", df.columns.tolist(), df.values, chunksize)
            # VENUES
            df = pd.DataFrame(papers_venues)
            self.insertInTable("paperVenue", df.columns.tolist(), df.values, chunksize)
            # JOURNALS
            df = pd.DataFrame(papers_journals)
            self.insertInTable(
                "paperJournal", df.columns.tolist(), df.values, chunksize
            )

        print("Filling in venue, journal and field of study data...")
        if ncpu:
            # Parallel processing
            with tqdm(total=len(gz_files), leave=None) as pbar:
                with Pool(ncpu) as p:
                    for all_data in p.imap(
                        partial(
                            process_Fields,
                            venues_dict=venues_dict,
                            journals_dict=journals_dict,
                            fields_dict=fields_dict,
                            papers_set=papers_set,
                        ),
                        gz_files,
                    ):
                        populate(all_data)
                        pbar.update()
        else:
            with tqdm(total=len(gz_files), leave=None) as pbar:
                for gzf in gz_files:
                    all_data = process_Fields(
                        gzf, venues_dict, journals_dict, fields_dict, papers_set
                    )
                    populate(all_data)
                    pbar.update()

    def importAuthorsData(self, dir_data, ncpu, chunksize=None):
        """ Imports authors' information """

        print("Filling authors information")
        # Get data files
        gz_files = get_gzfiles(dir_data)

        # Read tables to avoid repeated values
        authors_set = self.read_table_set("S2authors", "S2authorID")

        def chunks(l, n):
            """Yields successive n-sized chunks from list l."""
            for i in range(0, len(l), n):
                yield l[i : i + n]

        columns = ["S2authorID", "name"]
        ch_size = 100  # Number of files to process at a time
        remaining = len(gz_files)

        with tqdm(total=np.ceil(len(gz_files) / ch_size), leave=None) as chunk_bar:
            chunk_bar.set_description("Processing file chunks")
            for gz_chunk in chunks(gz_files, ch_size):
                author_counts = []
                if ncpu:
                    # Parallel processing
                    with tqdm(total=min(ch_size, remaining), leave=None) as pbar:
                        with Pool(ncpu) as p:
                            for thisfile_authors in p.imap(process_Authors, gz_chunk):
                                author_counts += thisfile_authors
                                pbar.update()
                else:
                    with tqdm(total=min(ch_size, remaining), leave=None) as pbar:
                        for gzf in gz_chunk:
                            author_counts += process_Authors(gzf)
                            pbar.update()
                remaining = remaining - ch_size

                # We need to get rid of duplicated ids
                # If an ID is repeated, keep the longest name
                author_counts = set(author_counts)
                author_counts = [
                    author for author in author_counts if author[0] not in authors_set
                ]
                df = pd.DataFrame(author_counts, columns=columns)
                df["length"] = df["name"].str.len()
                df.sort_values("length", ascending=False, inplace=True)
                df.drop_duplicates(subset="S2authorID", inplace=True)

                if len(df):
                    # Populate tables with the new data
                    self.insertInTable(
                        "S2authors", columns, df[columns].values, chunksize
                    )
                    authors_set.update(df["S2authorID"].values)

                chunk_bar.update()

    def importAuthorship(self, dir_data, ncpu, chunksize=None):
        """ Imports Authorship information (paper-author data) """

        print("Processing paper-authors information")

        # Get data files
        gz_files = get_gzfiles(dir_data)

        # Get all papers and authors IDs present in database
        print("Obtaining all papers and authors IDs")
        papers_set = self.read_table_set("S2papers", "S2paperID")
        authors_set = self.read_table_set("S2authors", "S2authorID")

        def populate(lista_paper_author):
            """ Aux function to insert data into database """

            # Ensure all papers and authors exist in database
            aux_list = [
                (c0, c1)
                for c0, c1 in lista_paper_author
                if c0 in papers_set and c1 in authors_set
            ]

            columns = ["S2paperID", "S2authorID"]
            df = pd.DataFrame(aux_list, columns=columns)

            # Introduce new data
            self.insertInTable("paperAuthor", columns, df.values, chunksize)
            authors_set.update(df["S2authorID"].values)

        if ncpu:
            # Parallel processing
            with tqdm(total=len(gz_files), leave=None) as pbar:
                with Pool(ncpu) as p:
                    for lista_paper_author in p.imap(process_Authorship, gz_files):
                        # Populate tables with the new data
                        populate(lista_paper_author)
                        pbar.update()

        else:
            with tqdm(total=len(gz_files), leave=None) as pbar:
                for gzf in gz_files:
                    lista_paper_author = process_Authorship(gzf)
                    # Populate tables with the new data
                    populate(lista_paper_author)
                    pbar.update()
