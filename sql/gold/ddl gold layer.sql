-- ***************************************************************** --
-- 1. DDL DES DIMENSIONS (DIMs)
-- ***************************************************************** --
IF OBJECT_ID('gold.DimTime','U') IS NOT NULL DROP TABLE gold.DimTime;
IF OBJECT_ID('gold.DimTeam','U') IS NOT NULL DROP TABLE gold.DimTeam;
IF OBJECT_ID('gold.DimNation','U') IS NOT NULL DROP TABLE gold.DimNation;
IF OBJECT_ID('gold.DimPlayer','U') IS NOT NULL DROP TABLE gold.DimPlayer;
IF OBJECT_ID('gold.DimNotes','U') IS NOT NULL DROP TABLE gold.DimNotes;
IF OBJECT_ID('gold.FactTeamPerformance','U') IS NOT NULL DROP TABLE gold.FactTeamPerformance;
IF OBJECT_ID('gold.FactPlayerPerformance','U') IS NOT NULL DROP TABLE gold.FactPlayerPerformance;
IF OBJECT_ID('gold.FactMatchEvent','U') IS NOT NULL DROP TABLE gold.FactMatchEvent;
GO

-----------------------------------
-- 1.1. DimTime (Basé sur Season)
-----------------------------------
-- Source: silver.League_Table_Conformed, silver.Match_Odds_Conformed, etc.
CREATE TABLE gold.DimTime (
    Time_SK             INT IDENTITY(1,1) PRIMARY KEY,
    
    Season_BK           VARCHAR(10) NOT NULL UNIQUE,  -- Clé métier (Ex: '2014/15')
    Season_Start_Year   INT NOT NULL,                 -- 2014
    Season_End_Year     INT NOT NULL                  -- 2015
);
GO

-----------------------------------
-- 1.2. DimTeam (Basé sur Squad_Conformed)
-----------------------------------
-- Source: silver.Team_Mapping
CREATE TABLE gold.DimTeam (
    Team_SK             INT IDENTITY(1,1) PRIMARY KEY,
    
    Squad_Conformed_BK  VARCHAR(100) NOT NULL UNIQUE -- Clé métier (Ex: 'Manchester City')
);
GO

-----------------------------------
-- 1.3. DimNation
-----------------------------------
-- Source: silver.Nation_Mapping
CREATE TABLE gold.DimNation (
    Nation_SK           INT IDENTITY(1,1) PRIMARY KEY,
    
    Nation_Standard_Name_BK VARCHAR(100) NOT NULL UNIQUE
);
GO

-----------------------------------
-- 1.4. DimPlayer (Basé sur Player_Name et BirthYear)
-----------------------------------
-- Source: silver.Player_Stats_Conformed
CREATE TABLE gold.DimPlayer (
    Player_SK           INT IDENTITY(1,1) PRIMARY KEY,
    
    Player_Name_BK      VARCHAR(100) NOT NULL,          -- Clé métier (Nom du joueur)
    BirthYear           INT NULL,
    
    -- Attributs statiques/semi-statiques
    Current_Position    VARCHAR(10) NULL,
    Nation_SK           INT NULL REFERENCES gold.DimNation(Nation_SK), -- FK vers Nation
    
    -- Clé naturelle composée unique (pour gestion SCD Type 1)
    UNIQUE (Player_Name_BK, BirthYear)
);
GO

-----------------------------------
-- 1.5. DimNotes (Qualification/Relégation)
-----------------------------------
-- Source: silver.Notes_Mapping
CREATE TABLE gold.DimNotes (
    Notes_SK            INT IDENTITY(1,1) PRIMARY KEY,
    
    Notes_Standard_Name_BK VARCHAR(255) NOT NULL UNIQUE, -- Clé métier (Ex: 'Champions League')
    Notes_Category      VARCHAR(50) NULL                -- Ex: 'Qualification', 'Relegation'
);
GO

-- ***************************************************************** --
-- 2. DDL DES TABLES DE FAITS (FACTs)
-- ***************************************************************** --

-----------------------------------
-- 2.1. FactTeamPerformance (Agrégé par Saison/Équipe)
-----------------------------------
-- Grain: 1 ligne par Équipe et par Saison
-- Sources: silver.League_Table_Conformed (pour le classement), silver.Team_extra_details (pour les détails)
CREATE TABLE gold.FactTeamPerformance (
    -- Clés de Substitution (Foreign Keys)
    Time_SK             INT NOT NULL REFERENCES gold.DimTime(Time_SK),
    Team_SK             INT NOT NULL REFERENCES gold.DimTeam(Team_SK),
    Notes_SK            INT NOT NULL REFERENCES gold.DimNotes(Notes_SK), -- Qualification / Relégation
    
    -- Clé Primaire Composite
    PRIMARY KEY (Time_SK, Team_SK), 

    -- Mesures de League Table (Type = 'Overall')
    Final_Rank          INT NULL,           -- Rk
    MatchesPlayed       INT NULL,           -- MP
    Wins                INT NULL,           -- W
    Draws               INT NULL,           -- D
    Losses              INT NULL,           -- L
    GoalsFor            INT NULL,           -- GF
    GoalsAgainst        INT NULL,           -- GA
    GoalDifference      INT NULL,           -- GD
    Points              INT NULL,           -- Pts
    PointsPerMatch      DECIMAL(4,2) NULL,  -- Pts_per_MP

    -- Mesures de Team Extra Details
    Season_Attendance   INT NULL,
    TopScorer_Goals     INT NULL,
    
    -- Attributs de pont (Pour identification directe sans FactPlayer ou pour un Top Buteur non mappé)
    TopScorer_PlayerName VARCHAR(100) NULL,
    Goalkeeper_PlayerName VARCHAR(100) NULL
);
GO

-----------------------------------
-- 2.2. FactPlayerPerformance (Détaillé par Saison/Équipe/Joueur)
-----------------------------------
-- Grain: 1 ligne par Joueur, par Équipe, par Saison
-- Source: silver.Player_Stats_Conformed
CREATE TABLE gold.FactPlayerPerformance (
    -- Clés de Substitution (Foreign Keys)
    Time_SK             INT NOT NULL REFERENCES gold.DimTime(Time_SK),
    Team_SK             INT NOT NULL REFERENCES gold.DimTeam(Team_SK),
    Player_SK           INT NOT NULL REFERENCES gold.DimPlayer(Player_SK),
    
    -- Clé Primaire Composite
    PRIMARY KEY (Time_SK, Team_SK, Player_SK), 
    
    -- Mesures de Performance
    MatchesPlayed       INT NULL,
    MinutesPlayed       INT NULL,
    Goals               INT NULL,
    Assists             INT NULL,
    YellowCards         INT NULL,
    RedCards            INT NULL,

    Goals_Per_90        DECIMAL(5,2) NULL,
    Assists_Per_90      DECIMAL(5,2) NULL,
    
    -- Mesures du Joueur
    MarketValue_Euro_k  INT NULL
);
GO

-----------------------------------
-- 2.3. FactMatchEvent (Granulaire par Match)
-----------------------------------
-- Grain: 1 ligne par Match
-- Sources: silver.EPL_Match_History_Conformed (Stats primaires), silver.Match_Odds_Conformed (Cotes)
CREATE TABLE gold.FactMatchEvent (
    -- Clés de Substitution (Foreign Keys)
    Time_SK             INT NOT NULL REFERENCES gold.DimTime(Time_SK),
    HomeTeam_SK         INT NOT NULL REFERENCES gold.DimTeam(Team_SK),
    AwayTeam_SK         INT NOT NULL REFERENCES gold.DimTeam(Team_SK),
    
    -- Clé Primaire Composite (Clé de substitution du match si vous en aviez une, sinon clés naturelles)
    PRIMARY KEY (Time_SK, HomeTeam_SK, AwayTeam_SK), 

    -- Attributs de Date
    MatchDate           DATE NOT NULL, -- Clé de la date exacte (pour la granularité journalière)

    -- Mesures de Résultat (Priorité à EPL_Match_History_Conformed)
    FullTimeHomeGoals   INT NULL,
    FullTimeAwayGoals   INT NULL,
    FullTimeResult      VARCHAR(5) NULL,
    HalfTimeHomeGoals   INT NULL,
    HalfTimeAwayGoals   INT NULL,
    HalfTimeResult      VARCHAR(5) NULL,

    -- Statistiques de Match (Priorité à EPL_Match_History_Conformed)
    HomeShots           INT NULL,
    AwayShots           INT NULL,
    HomeShotsOnTarget   INT NULL,
    AwayShotsOnTarget   INT NULL,
    HomeFouls           INT NULL,
    AwayFouls           INT NULL,
    HomeCorners         INT NULL,
    AwayCorners         INT NULL,
    HomeYellowCards     INT NULL,
    AwayYellowCards     INT NULL,
    HomeRedCards        INT NULL,
    AwayRedCards        INT NULL,

    -- Cotes de Paris (Uniquement de Match_Odds_Conformed)
    B365HomeOdds        DECIMAL(5,2) NULL,
    B365DrawOdds        DECIMAL(5,2) NULL,
    B365AwayOdds        DECIMAL(5,2) NULL
);
GO