-- ==========================================
-- Create Artists Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.artists (
    artist_id    VARCHAR(256) NOT NULL,
    name         VARCHAR(256),
    location     VARCHAR(256),
    latitude     NUMERIC(18,0),
    longitude    NUMERIC(18,0),
    CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
);

-- ==========================================
-- Create Songs Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.songs (
    song_id      VARCHAR(256) NOT NULL,
    title        VARCHAR(256),
    artist_id    VARCHAR(256),
    year         INT,
    duration     NUMERIC(18,0),
    CONSTRAINT songs_pkey PRIMARY KEY (song_id)
);

-- ==========================================
-- Create Users Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.users (
    user_id      INT NOT NULL,
    first_name   VARCHAR(256),
    last_name    VARCHAR(256),
    gender       VARCHAR(256),
    level        VARCHAR(256),
    CONSTRAINT users_pkey PRIMARY KEY (user_id)
);

-- ==========================================
-- Create Time Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.time (
    start_time   TIMESTAMP NOT NULL PRIMARY KEY,
    hour         INT,
    day          INT,
    week         INT,
    month        INT,
    year         INT,
    weekday      INT
);

-- ==========================================
-- Create Songplays Fact Table
-- ==========================================
CREATE TABLE IF NOT EXISTS public.songplays (
    play_id      VARCHAR(32) NOT NULL,
    start_time   TIMESTAMP NOT NULL,
    user_id      INT NOT NULL,
    level        VARCHAR(256),
    song_id      VARCHAR(256),
    artist_id    VARCHAR(256),
    session_id   INT,
    location     VARCHAR(256),
    user_agent   VARCHAR(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (play_id)
);

-- ==========================================
-- Create Staging Tables
-- ==========================================
CREATE TABLE IF NOT EXISTS public.staging_events (
    artist         VARCHAR(256),
    auth           VARCHAR(256),
    first_name     VARCHAR(256),
    gender         VARCHAR(256),
    item_in_session INT,
    last_name      VARCHAR(256),
    length         NUMERIC(18,0),
    level          VARCHAR(256),
    location       VARCHAR(256),
    method         VARCHAR(256),
    page           VARCHAR(256),
    registration   NUMERIC(18,0),
    session_id     INT,
    song           VARCHAR(256),
    status         INT,
    ts             BIGINT,
    user_agent     VARCHAR(256),
    user_id        INT
);

CREATE TABLE IF NOT EXISTS public.staging_songs (
    num_songs        INT,
    artist_id        VARCHAR(256),
    artist_name      VARCHAR(256),
    artist_latitude  NUMERIC(18,0),
    artist_longitude NUMERIC(18,0),
    artist_location  VARCHAR(256),
    song_id          VARCHAR(256),
    title            VARCHAR(256),
    duration         NUMERIC(18,0),
    year             INT
);
