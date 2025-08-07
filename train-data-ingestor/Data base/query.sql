-- Table Dimension : Train
CREATE TABLE Train (
    TrainID INT PRIMARY KEY,
    TrainNumber VARCHAR(50) NOT NULL,
    TrainType VARCHAR(50),
    Operator VARCHAR(100),
    id VARCHAR (50) NOT NULL,
    CONSTRAINT Train_id_unique UNIQUE (id)
);
CREATE INDEX idx_TrainNumber ON Train(TrainNumber);


-- Table Dimension : Station
CREATE TABLE Station (
    StationID INT PRIMARY KEY,
    StationName VARCHAR(100) NOT NULL,
    Latitude FLOAT,
    Longitude FLOAT,
    id VARCHAR (50) NOT NULL,
    iri_url VARCHAR (100) NOT NULL,
    CONSTRAINT Station_id_unique UNIQUE (id)
);
CREATE INDEX idx_StationName ON Station(StationName);


-- Table Dimension : Date
CREATE TABLE DateDimension (
    DateID INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    Day INT,
    Month INT,
    Year INT,
    Hour INT,
    Minute INT,
    Second INT
);
CREATE INDEX idx_FullDate ON DateDimension(FullDate);


-- Table de faits : TrainMovements
CREATE TABLE TrainMovements (
    MovementID INT PRIMARY KEY IDENTITY(1,1),
    TrainID INT NOT NULL,
    DepartureStationID INT NOT NULL,
    ArrivalStationID INT NOT NULL,
    DepartureDateID INT NOT NULL,
    ArrivalDateID INT NOT NULL,
    ScheduledDepartureTime DATETIME,
    ActualDepartureTime DATETIME,
    ScheduledArrivalTime DATETIME,
    ActualArrivalTime DATETIME,
    DelayMinutes INT,
    Platform VARCHAR(50),
    
    -- Foreign Keys
    FOREIGN KEY (TrainID) REFERENCES Train(TrainID),
    FOREIGN KEY (DepartureStationID) REFERENCES Station(StationID),
    FOREIGN KEY (ArrivalStationID) REFERENCES Station(StationID),
    FOREIGN KEY (DepartureDateID) REFERENCES DateDimension(DateID),
    FOREIGN KEY (ArrivalDateID) REFERENCES DateDimension(DateID)
);

--Fact Table TrainFeedback
CREATE TABLE TrainFeedback (
    ID INT PRIMARY KEY IDENTITY(1,1),
    connectionUrl VARCHAR(100),
    stationUrl VARCHAR(100),
    feedbackDate DATETIME NOT NULL,
    vehicleUrl VARCHAR(100), 
    occupancy VARCHAR(50)
    );
-- Fact Table TrainCompositionUnit

CREATE TABLE TrainCompositionUnit (
                ID INT PRIMARY KEY IDENTITY(1,1),
                TrainID INT,
                SegmentOriginId INT,
                SegmentDestinationId INT,
                UnitId INT,
                ParentType VARCHAR(50),
                SubType VARCHAR(50),
                Orientation VARCHAR(50),
                HasToilets BIT,
                HasTables BIT,
                HasSecondClassOutlets BIT,
                HasFirstClassOutlets BIT,
                HasHeating BIT,
                HasAirco BIT,
                MaterialNumber VARCHAR(50),
                TractionType VARCHAR(50),
                CanPassToNextUnit BIT,
                StandingPlacesSecondClass INT,
                StandingPlacesFirstClass INT,
                SeatsSecondClass INT,
                SeatsFirstClass INT,
                LengthInMeter FLOAT,
                HasSemiAutomaticInteriorDoors BIT, 
                HasLuggageSection BIT,
                MaterialSubTypeName VARCHAR(50),
                TractionPosition VARCHAR(50),
                HasPrmSection BIT,
                HasPriorityPlaces BIT,
                HasBikeSection BIT,
       -- Foreign Keys
    FOREIGN KEY (TrainID) REFERENCES Train(TrainID),
    FOREIGN KEY (SegmentOriginId) REFERENCES Station(StationID),
    FOREIGN KEY (SegmentDestinationId) REFERENCES Station(StationID),
    FOREIGN KEY (UnitId) REFERENCES TrainCompositionUnit(UnitId)
);

--Table Disturbance
CREATE TABLE Disturbance (
                DisturbanceId INT PRIMARY KEY IDENTITY(1,1),
                Title VARCHAR(100) NOT NULL,
                Description TEXT,
                Type VARCHAR(50),
                Timestamp DATETIME NOT NULL,
                Link VARCHAR(100),
                Attachment VARCHAR(100)
);
-- Indexes pour les colonnes souvent filtrées ou jointes
CREATE INDEX idx_DepartureDateID ON TrainMovements(DepartureDateID);
CREATE INDEX idx_ArrivalDateID ON TrainMovements(ArrivalDateID);
CREATE INDEX idx_TrainID ON TrainMovements(TrainID);
CREATE INDEX idx_DepartureStationID ON TrainMovements(DepartureStationID);
CREATE INDEX idx_ArrivalStationID ON TrainMovements(ArrivalStationID);
