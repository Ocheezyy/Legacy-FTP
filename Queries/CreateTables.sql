USE #####;
GO

CREATE TABLE #####;
(
	ID INT NOT NULL PRIMARY KEY,
	Obit_Date SMALLDATETIME NOT NULL,
	SSN VARCHAR(MAX),
	Salutation VARCHAR(MAX),
	FName VARCHAR(MAX) NOT NULL,
	MName VARCHAR(MAX),
	LName VARCHAR(MAX) NOT NULL,
	MaidenName VARCHAR(MAX),
	NickName VARCHAR(MAX),
	DOB DATETIME,
	DOD DATETIME,
	Age SMALLINT,
	Gender VARCHAR(MAX),
	CurrentCity VARCHAR(MAX),
	CurrentState VARCHAR(MAX),
	FuneralServiceInCity VARCHAR(MAX),
	FuneralServiceInState VARCHAR(MAX),
	ObituaryLink VARCHAR(MAX),
	AssociatedFuneralHome VARCHAR(MAX),
	NewspaperSource VARCHAR(MAX),
	NewspaperCity VARCHAR(MAX),
	NewspaperZip VARCHAR(MAX),
	Education VARCHAR(MAX),
	Military VARCHAR(MAX),
	Donation VARCHAR(MAX),
	FuneralServices VARCHAR(MAX),
	hasMatch BIT,
	isDead BIT,
	src VARCHAR(MAX),
	Ignore bit,
	LastEditedByUser VARCHAR(MAX),
	IgnoreReason VARCHAR(MAX),
	DateEntered SMALLDATETIME,
	isFH BIT
);

CREATE TABLE #####;
(
	ID INT NOT NULL PRIMARY KEY,
	ObitText VARCHAR(MAX),
);