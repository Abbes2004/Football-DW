IF OBJECT_ID('silver.League_Table_Conformed','U') IS NOT NULL DROP TABLE silver.League_Table_Conformed;
IF OBJECT_ID('silver.Team_Mapping','U') IS NOT NULL DROP TABLE silver.Team_Mapping;
IF OBJECT_ID('silver.squad_stats_conformed','U') IS NOT NULL DROP TABLE silver.squad_stats_conformed;
IF OBJECT_ID('silver.EPL_Match_History_Conformed','U') IS NOT NULL DROP TABLE silver.EPL_Match_History_Conformed;
IF OBJECT_ID('silver.Nation_Mapping','U') IS NOT NULL DROP TABLE silver.Nation_Mapping;
IF OBJECT_ID('silver.Player_Stats_Conformed','U') IS NOT NULL DROP TABLE silver.Player_Stats_Conformed;
IF OBJECT_ID('silver.Team_extra_details','U') IS NOT NULL DROP TABLE silver.Team_extra_details;
IF OBJECT_ID('silver.Notes_Mapping','U') IS NOT NULL DROP TABLE silver.Notes_Mapping;
IF OBJECT_ID('silver.Match_Odds_Conformed','U') IS NOT NULL DROP TABLE silver.Match_Odds_Conformed;
GO

-- DDL de la table Silver consolidée pour les classements
CREATE TABLE silver.League_Table_Conformed (
    Season              VARCHAR(10) NOT NULL,
    Rk                  INT,
    Squad_Conformed     VARCHAR(50) NOT NULL, -- Nom standardisé après Lookup
    Type                VARCHAR(10) NOT NULL, -- 'Home', 'Away', ou 'Overall'
    MP                  INT,
    W                   INT,
    D                   INT,
    L                   INT,
    GF                  INT,
    GA                  INT,
    GD                  INT,                  -- Converti de VARCHAR à INT
    Pts                 INT,
    Pts_per_MP          DECIMAL(4,2)
);

CREATE TABLE silver.Team_Mapping (
    Team_Source_Name    VARCHAR(100) PRIMARY KEY, -- Le nom tel qu'il apparaît dans les fichiers JSON (ex: 'Man City')
    Team_Standard_Name  VARCHAR(100) NOT NULL     -- Le nom standardisé pour le DW (ex: 'Manchester City')
);

CREATE TABLE silver.Match_Odds_Conformed (
    -- Clé du temps et standardisation de l'équipe
    [MatchDate]                 DATE NOT NULL,
    [Season]                    VARCHAR(10) NULL,           -- Nouvelle colonne à dériver
    
    [HomeTeam_Conformed]        VARCHAR(100) NOT NULL,
    [AwayTeam_Conformed]        VARCHAR(100) NOT NULL,

    -- Score et Résultat
    [FullTimeHomeGoals]         INT NULL,                   -- FTHG (FLOAT -> INT)
    [FullTimeAwayGoals]         INT NULL,                   -- FTAG (FLOAT -> INT)
    [FullTimeResult]            VARCHAR(5) NULL,            -- FTR
    
    [HalfTimeHomeGoals]         INT NULL,                   -- HTHG
    [HalfTimeAwayGoals]         INT NULL,                   -- HTAG
    [HalfTimeResult]            VARCHAR(5) NULL,            -- HTR

    -- Statistiques des tirs
    [HomeShots]                 INT NULL,                   -- HS
    [AwayShots]                 INT NULL,                   -- AS
    [HomeShotsOnTarget]         INT NULL,                   -- HST
    [AwayShotsOnTarget]         INT NULL,                   -- AST

    -- Fautes et Cartons
    [HomeFouls]                 INT NULL,                   -- HF
    [AwayFouls]                 INT NULL,                   -- AF
    [HomeYellowCards]           INT NULL,                   -- HY (FLOAT -> INT)
    [AwayYellowCards]           INT NULL,                   -- AY (FLOAT -> INT)
    [HomeRedCards]              INT NULL,                   -- HR (FLOAT -> INT)
    [AwayRedCards]              INT NULL,                   -- AR (FLOAT -> INT)
    
    -- Autres Statistiques
    [HomeCorners]               INT NULL,                   -- HC
    [AwayCorners]               INT NULL,                   -- AC

    -- Cotes de Paris (Bet365 - Garder en FLOAT/DECIMAL)
    [B365HomeOdds]              DECIMAL(5,2) NULL,          -- B365H
    [B365DrawOdds]              DECIMAL(5,2) NULL,          -- B365D
    [B365AwayOdds]              DECIMAL(5,2) NULL           -- B365A
);
GO

CREATE TABLE silver.squad_stats_conformed (
    Season              VARCHAR(10)      NOT NULL,
    Squad_Conformed     VARCHAR(100)     NOT NULL, -- Clé standardisée
    
    Players_Count       INT,
    Avg_Age                 DECIMAL(4,1),
    Poss                DECIMAL(5,2),

    MP                  INT,
    Starts              INT,
    Min                 INT,
    Ninety_Count        DECIMAL(5,2),

    Gls                 INT,
    Ast                 INT,
    G_plus_A            INT,
    G_minus_PK          INT,
    PK                  INT,
    PKatt               INT,

    CrdY                INT,
    CrdR                INT,

    Gls_per_90          DECIMAL(5,2),
    Ast_per_90          DECIMAL(5,2),
    GA_per_90           DECIMAL(5,2),
    G_minus_PK_90       DECIMAL(5,2),
    GA_minus_PK         DECIMAL(5,2)
);

CREATE TABLE silver.EPL_Match_History_Conformed (
    [Season] VARCHAR(10) NULL,
    [MatchDate]  DATE NULL,   
    [HomeTeam_Conformed] VARCHAR(50) NULL,
    [AwayTeam_Conformed] VARCHAR(50) NULL,

    [FullTimeHomeGoals] INT NULL,
    [FullTimeAwayGoals] INT NULL,
    [FullTimeResult] VARCHAR(5) NULL,

    [HalfTimeHomeGoals] INT NULL,
    [HalfTimeAwayGoals] INT NULL,
    [HalfTimeResult] VARCHAR(5) NULL,

    [HomeShots] INT NULL,
    [AwayShots] INT NULL,

    [HomeShotsOnTarget] INT NULL,
    [AwayShotsOnTarget] INT NULL,

    [HomeCorners] INT NULL,
    [AwayCorners] INT NULL,

    [HomeFouls] INT NULL,
    [AwayFouls] INT NULL,

    [HomeYellowCards] INT NULL,
    [AwayYellowCards] INT NULL,

    [HomeRedCards] INT NULL,
    [AwayRedCards] INT NULL
);

-- DDL de la table de mapping pour la Couche Silver (sans code ISO/Abréviation)
CREATE TABLE silver.Nation_Mapping (
    -- Clé du pays telle qu'elle apparaît dans la Couche Bronze (ex: 'engENG', 'frFRA')
    Nation_Source_Key       VARCHAR(10) PRIMARY KEY, 
    
    -- Nom standardisé et complet du pays (ex: 'Angleterre', 'France')
    Nation_Standard_Name    VARCHAR(100) NOT NULL
);

CREATE TABLE silver.Player_Stats_Conformed (
    -- Clés de dimension standardisées
    [Season_Key]                VARCHAR(10) NOT NULL, 
    [Squad_Conformed]           VARCHAR(100) NOT NULL, -- Clé standardisée via silver.Team_Mapping
    [Nation_Conformed]          VARCHAR(100) NOT NULL, -- Nom complet de la nation via silver.Nation_Mapping
    
    -- Attributs du joueur (Clés naturelles de la future DimPlayer)
    [Player_Name]               VARCHAR(100) NOT NULL, -- Le nom du joueur, clé naturelle
    [Position]                  VARCHAR(10),           -- Remplacement de Pos
    [Age]                       INT,
    [BirthYear]                 INT,                   -- Remplacement de Born
    
    -- Statistiques / Mesures (mesures candidates pour FactPlayerPerformance)
    [MatchesPlayed]             INT,                   -- Remplacement de MP
    [Starts]                    INT,
    [MinutesPlayed]             INT,                   -- Remplacement de Min
    [NinetyMinsPlayed]          DECIMAL(5,2),          -- Remplacement de [90s]
    [Goals]                     INT,                   -- Remplacement de Gls
    [Assists]                   INT,                   -- Remplacement de Ast
    [YellowCards]               INT,                   -- Remplacement de CrdY
    [RedCards]                  INT,                   -- Remplacement de CrdR
    
    -- Statistiques dérivées / Taux
    [Goals_Per_90]              DECIMAL(5,2),          -- Remplacement de Gls_1
    [Assists_Per_90]            DECIMAL(5,2),          -- Remplacement de Ast_1
    
    -- Valeur marchande (mesure candidate)
    [MarketValue_Euro_k]        INT,
    [MarketValue_LastUpdate]    DATE
);
GO

CREATE TABLE silver.Team_extra_details (
    -- Clés standardisées (via Lookups)
    [Season]              VARCHAR(10) NOT NULL,
    [Squad_Conformed]          VARCHAR(100) NOT NULL,  -- Nom d'équipe standardisé via mapping
    
    -- Statistiques / Mesures (pour FactTeamPerformance)
    [Season_Attendance]        INT,                    -- Remplacement de Attendance
    
    -- Attributs et Clés secondaires
    [TopScorer_PlayerName]     VARCHAR(100) NULL,      -- Nom du joueur après extraction
    [TopScorer_Goals]          INT NULL,               -- Nombre de buts après extraction
    [Goalkeeper_PlayerName]    VARCHAR(100) NULL,      -- Nom du gardien
    [Qualification_Notes]      VARCHAR(255) NULL       -- Remplacement de Notes
);
GO


CREATE TABLE silver.Notes_Mapping (
    -- Clé de la note telle qu'elle apparaît dans la Couche Bronze (inclut les chaînes vides)
    Notes_Source_Key        VARCHAR(255) PRIMARY KEY, 
    
    -- Description standardisée de la note ou de la qualification (ex: 'Champions League')
    Notes_Standard_Name     VARCHAR(255) NOT NULL
);
GO
