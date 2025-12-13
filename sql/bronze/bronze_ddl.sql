IF OBJECT_ID('bronze.staging_epl_matchs','U') IS NOT NULL DROP TABLE bronze.staging_epl_matchs;
IF OBJECT_ID('bronze.staging_epl_history','U') IS NOT NULL DROP TABLE bronze.staging_epl_history;
IF OBJECT_ID('bronze.staging_league_table_home','U') IS NOT NULL DROP TABLE bronze.staging_league_table_home;
IF OBJECT_ID('bronze.staging_league_table_away','U') IS NOT NULL DROP TABLE bronze.staging_league_table_away;
IF OBJECT_ID('bronze.staging_league_table_overall','U') IS NOT NULL DROP TABLE bronze.staging_league_table_overall;
IF OBJECT_ID('bronze.staging_player_stats','U') IS NOT NULL DROP TABLE bronze.staging_player_stats;
IF OBJECT_ID('bronze.staging_squad_stats','U') IS NOT NULL DROP TABLE bronze.staging_squad_stats;
GO

CREATE TABLE bronze.staging_epl_matchs(
    [Date] VARCHAR(20) NULL,
    [HomeTeam] VARCHAR(50) NULL,
    [AwayTeam] VARCHAR(50) NULL,
    
    [FTHG] FLOAT NULL,
    [FTAG] FLOAT NULL,
    [FTR] VARCHAR(5) NULL,

    [HTHG] FLOAT NULL,
    [HTAG] FLOAT NULL,
    [HTR] VARCHAR(5) NULL,

    [HS] FLOAT NULL,
    [AS] FLOAT NULL,
    [HST] FLOAT NULL,
    [AST] FLOAT NULL,

    [HF] FLOAT NULL,
    [AF] FLOAT NULL,

    [HC] FLOAT NULL,
    [AC] FLOAT NULL,

    [HY] FLOAT NULL,
    [AY] FLOAT NULL,

    [HR] FLOAT NULL,
    [AR] FLOAT NULL, 
    [B365H] FLOAT NULL,
    [B365D] FLOAT NULL, 
    [B365A] FLOAT NULL
);


CREATE TABLE bronze.staging_epl_history (
    [Season] VARCHAR(10) NULL,
    [MatchDate]  DATE NULL,   
    [HomeTeam] VARCHAR(50) NULL,
    [AwayTeam] VARCHAR(50) NULL,

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





CREATE TABLE bronze.staging_player_stats (
    Season              VARCHAR(10),            
    Player              VARCHAR(100),
    Nation              VARCHAR(20),
    Pos                 VARCHAR(10),
    Squad               VARCHAR(100),
    Age                 INT,
    Born                INT,
    MP                  INT,
    Starts              INT,
    Min                 INT,
    [90s]               DECIMAL(5,2),
    Gls                 INT,
    Ast                 INT,
    CrdY                INT,
    CrdR                INT,
    Gls_1               DECIMAL(5,2),
    Ast_1               DECIMAL(5,2),
    market_value_euro_k INT,
    market_value_last_update DATE
);
GO

CREATE TABLE bronze.staging_league_table_home (
    Season          VARCHAR(10),
    Rk              INT,
    Squad           VARCHAR(50),
    MP              INT,
    W               INT,
    D               INT,
    L               INT,
    GF              INT,
    GA              INT,
    GD              VARCHAR(10),   -- ex: "+14"
    Pts             INT,
    Pts_per_MP      DECIMAL(4,2)
);

CREATE TABLE bronze.staging_league_table_away (
    Season          VARCHAR(10),
    Rk              INT,
    Squad           VARCHAR(50),
    MP              INT,
    W               INT,
    D               INT,
    L               INT,
    GF              INT,
    GA              INT,
    GD              VARCHAR(10),
    Pts             INT,
    Pts_per_MP      DECIMAL(4,2)
);

CREATE TABLE bronze.staging_league_table_overall (
    Season              VARCHAR(10),
    Rk                  INT,
    Squad               VARCHAR(50),
    MP                  INT,
    W                   INT,
    D                   INT,
    L                   INT,
    GF                  INT,
    GA                  INT,
    GD                  VARCHAR(10),
    Pts                 INT,
    Pts_per_MP          DECIMAL(4,2),

    Attendance          INT,             -- cleaned
    Top_Team_Scorer     VARCHAR(100),
    Goalkeeper          VARCHAR(100),
    Notes               VARCHAR(500)
);

CREATE TABLE bronze.staging_squad_stats (
    Season          VARCHAR(10),
    Squad           VARCHAR(50),

    Players_Count   INT,        -- "# Pl"
    Age             DECIMAL(4,1),
    Poss            DECIMAL(5,2),

    MP              INT,
    Starts          INT,
    Min             INT,
    Ninety_Count    DECIMAL(5,2),    -- "90s"

    Gls             INT,
    Ast             INT,
    G_plus_A        INT,
    G_minus_PK      INT,
    PK              INT,
    PKatt           INT,

    CrdY            INT,
    CrdR            INT,

    Gls_per_90      DECIMAL(5,2),
    Ast_per_90      DECIMAL(5,2),
    GA_per_90       DECIMAL(5,2),
    G_minus_PK_90   DECIMAL(5,2),
    GA_minus_PK     DECIMAL(5,2)
);




