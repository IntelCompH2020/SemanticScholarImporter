-- CREATE TABLE "S2papers"(
--     "paperID" SERIAL,
--     "S2paperID" CHAR(40) PRIMARY KEY, --[str]
--     "title" TEXT COLLATE "default", --[str]
--     "lowertitle" TEXT COLLATE "default",
--     "paperAbstract" TEXT COLLATE "default", --[str]
--     -- "entities" TEXT COLLATE "default", -- DEPRECATED --[list]
--     "s2Url" TEXT COLLATE "default", --[str]
--     "pdfUrls" TEXT COLLATE "default", --[list]
--     -- "s2PdfUrl" TEXT COLLATE "default", -- DEPRECATED --[str]
--     -- "authors" , -- DIFFERENT TABLE --[list]
--     -- "inCitations" , -- DIFFERENT TABLE --[list]
--     -- "outCitations" , -- DIFFERENT TABLE --[list]
--     -- "fieldsOfStudy" , -- DIFFERENT TABLE --[list]
--     "year" SMALLINT, --[int]
--     -- "venue" , -- DIFFERENT TABLE --[str]
--     -- "journalName" , -- DIFFERENT TABLE --[str]
--     -- "journalVolume" , -- DIFFERENT TABLE --[str]
--     -- "journalPages" , -- DIFFERENT TABLE --[str]
--     "isDBLP" TEXT COLLATE "default", -- ("sources" field) --[list]
--     "doi" VARCHAR(128) COLLATE "default", --[str]
--     "doiUrl" VARCHAR(256) COLLATE "default", --[str]
--     "pmid" VARCHAR(16) COLLATE "default", --[str]
--     "magId" BIGINT --[str]
-- );
-- CREATE TABLE "S2papers"(
--     "paperID" SERIAL,
--     "S2paperID" CHAR(40) PRIMARY KEY,
--     "title" TEXT,
--     "lowertitle" TEXT,
--     "paperAbstract" TEXT,
--     "entities" TEXT,
--     "fieldsOfStudy" TEXT,
--     "s2PdfUrl" VARCHAR(77),
--     "pdfUrls" TEXT,
--     "year" SMALLINT,
--     "journalVolume" VARCHAR(300),
--     "journalPages" VARCHAR(100),
--     "isDBLP" BOOLEAN,
--     "isMedline" BOOLEAN,
--     "doi" VARCHAR(128),
--     "doiUrl" VARCHAR(256),
--     "pmid" VARCHAR(16),
--     "magid" BIGINT
--     -- "ESP_contri" BOOLEAN,
--     -- "AIselection" BOOLEAN,
--     -- "langid" VARCHAR(3)
-- ) ;
-- CREATE TABLE "paperLemas"(
--     "S2paperID" CHAR(40) COLLATE "default",
--     "lemas" TEXT COLLATE "default",
--     FOREIGN KEY ("S2paperID") REFERENCES "S2papers" ("S2paperID") ON DELETE CASCADE
-- );

CREATE TABLE "S2papers"(
    "paperID" SERIAL,
    "S2paperID" CHAR(40) PRIMARY KEY, --[str]
    "title" TEXT COLLATE "default", --[str]
    "lowertitle" TEXT COLLATE "default",
    "paperAbstract" TEXT COLLATE "default", --[str]
    "s2Url" TEXT COLLATE "default", --[str]
    "pdfUrls" TEXT COLLATE "default", --[list]
    "year" SMALLINT, --[int]
    "isDBLP" BOOLEAN, -- ("sources" field) --[list]
    "isMEDLINE" BOOLEAN, -- ("sources" field) --[list]
    "doi" VARCHAR(128) COLLATE "default", --[str]
    "doiUrl" VARCHAR(256) COLLATE "default", --[str]
    "pmid" INT, --[str]
    "magId" BIGINT --[str]
);

CREATE TABLE "S2authors"(
    "authorID" SERIAL,
    "S2authorID" INT PRIMARY KEY,
    "orcidID" VARCHAR(20) COLLATE "default",
    "orcidGivenName" VARCHAR(40) COLLATE "default",
    "orcidFamilyName" VARCHAR(100) COLLATE "default",
    "scopusID" BIGINT,
    "name" VARCHAR(256) COLLATE "default",
    "influentialCitationCount" SMALLINT,
    "ESP_affiliation" BOOLEAN
);

CREATE TABLE "paperAuthor"(
    "paperAuthorID" SERIAL PRIMARY KEY,
    "S2paperID" CHAR(40) COLLATE "default",
    "S2authorID" INT,
    FOREIGN KEY ("S2paperID") REFERENCES "S2papers" ("S2paperID") ON DELETE CASCADE,
    FOREIGN KEY ("S2authorID") REFERENCES "S2authors" ("S2authorID") ON DELETE CASCADE
);

CREATE TABLE "S2fields"(
    "fieldID" SERIAL PRIMARY KEY,
    "fieldName" VARCHAR(32) COLLATE "default"
);

CREATE TABLE "paperField"(
    "paperFieldID" SERIAL PRIMARY KEY,
    "S2paperID" CHAR(40) COLLATE "default",
    "fieldID" INT,
    FOREIGN KEY ("S2paperID") REFERENCES "S2papers" ("S2paperID") ON DELETE CASCADE,
    FOREIGN KEY ("fieldID") REFERENCES "S2fields" ("fieldID") ON DELETE CASCADE
);

CREATE TABLE "S2venues"(
    "venueID" SERIAL PRIMARY KEY,
    "venueName" VARCHAR(320) COLLATE "default"
);

CREATE TABLE "paperVenue"(
    "paperVenueID" SERIAL PRIMARY KEY,
    "S2paperID" CHAR(40) COLLATE "default",
    "venueID" INT,
    FOREIGN KEY ("S2paperID") REFERENCES "S2papers" ("S2paperID") ON DELETE CASCADE,
    FOREIGN KEY ("venueID") REFERENCES "S2venues" ("venueID") ON DELETE CASCADE
);

CREATE TABLE "S2journals"(
    "journalID" SERIAL PRIMARY KEY,
    "journalName" VARCHAR(320) COLLATE "default"
);

CREATE TABLE "paperJournal"(
    "paperJournalID" SERIAL PRIMARY KEY,
    "S2paperID" CHAR(40) COLLATE "default",
    "journalID" INT,
    "journalVolume" VARCHAR(300) COLLATE "default",
    "journalPages" VARCHAR(100) COLLATE "default",
    FOREIGN KEY ("S2paperID") REFERENCES "S2papers" ("S2paperID") ON DELETE CASCADE,
    FOREIGN KEY ("journalID") REFERENCES "S2journals" ("journalID") ON DELETE CASCADE
);

CREATE TABLE "citations"(
    "citationID" SERIAL PRIMARY KEY,
    "S2paperID1" CHAR(40) COLLATE "default",
    "S2paperID2" CHAR(40) COLLATE "default",
    "isInfluential" BOOLEAN,
    "BackgrIntent" BOOLEAN,
    "MethodIntent" BOOLEAN,
    "ResultIntent" BOOLEAN,
    FOREIGN KEY ("S2paperID1") REFERENCES "S2papers" ("S2paperID") ON DELETE CASCADE,
    FOREIGN KEY ("S2paperID2") REFERENCES "S2papers" ("S2paperID") ON DELETE CASCADE
);

-- CREATE INDEX S2idPaper ON "S2papers" ("S2paperID");
-- CREATE INDEX S2idAuth ON "S2authors" ("S2authorID");
-- CREATE INDEX paper1 ON "citations" ("S2paperID1");
-- CREATE INDEX paper2 ON "citations" ("S2paperID2");
-- CREATE INDEX paper ON "paperAuthor" ("S2paperID");
-- CREATE INDEX author ON "paperAuthor" ("S2authorID");