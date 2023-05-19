CREATE INDEX S2idPaper ON "S2papers" ("S2paperID");
CREATE INDEX S2idAuth ON "S2authors" ("S2authorID");
CREATE INDEX paper1 ON "citations" ("S2paperID1");
CREATE INDEX paper2 ON "citations" ("S2paperID2");
CREATE INDEX paper ON "paperAuthor" ("S2paperID");
CREATE INDEX author ON "paperAuthor" ("S2authorID");
