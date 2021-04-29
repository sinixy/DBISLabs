DROP TABLE IF EXISTS Location CASCADE;
DROP TABLE IF EXISTS Participant CASCADE;
DROP TABLE IF EXISTS Institution CASCADE;
DROP TABLE IF EXISTS Test CASCADE;



CREATE TABLE Location (
	locid SERIAL PRIMARY KEY NOT NULL,
	TerName VARCHAR(255),
	AreaName VARCHAR(255),
    RegName VARCHAR(255)
);

CREATE TABLE Institution (
    EOName VARCHAR(255) PRIMARY KEY NOT NULL,
    EOTypeName VARCHAR(255),
    EOParent VARCHAR(255),
    locid SERIAL REFERENCES Location(locid)
);

CREATE TABLE Participant (
    OUTID VARCHAR(255) PRIMARY KEY NOT NULL,
    Birth VARCHAR(255),
    SexTypeName VARCHAR(255),
	RegTypeName VARCHAR(255),
    ClassProfileName VARCHAR(255),
    ClassLangName VARCHAR(255),
    locid SERIAL REFERENCES Location(locid),
    EOName VARCHAR(255) REFERENCES Institution(EOName)
);

CREATE TABLE Test (
    testid SERIAL PRIMARY KEY NOT NULL,
    TestName VARCHAR(255),
    Lang VARCHAR(255),
    TestStatus VARCHAR(255),
    AdaptScale INTEGER,
    year INTEGER,
	ball12 INTEGER,
	ball100 REAL,
	ball REAL,
    OUTID VARCHAR(255) REFERENCES Participant(OUTID),
    EOName VARCHAR(255) REFERENCES Institution(EOName)
);