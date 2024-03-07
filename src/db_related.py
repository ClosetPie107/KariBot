"""
Copyright © 2024, ClosetPie107 <closetpie107@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""

from datetime import datetime, timedelta

from lang_db_connection import LangDBConnection
from stat_db_connection import StatDBConnection

column_names = None
create_statdb_query = """
CREATE TABLE IF NOT EXISTS playerstats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guildid INTEGER,
    discordid INTEGER, 
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    playername TEXT,
    level INTEGER,
    ascensionlevel INTEGER,
    kingdom TEXT,
    datecreated DATE,
    playtime INTEGER,
    travelersguild INTEGER,
    anglersguild INTEGER,
    circleofanguish INTEGER,
    titanfelledguild INTEGER,
    bladesoffinesse INTEGER,
    spelunkingguild INTEGER,
    seersguild INTEGER,
    monumentalguild INTEGER,
    globalrank INTEGER,
    regionalrank INTEGER,
    competitiverank INTEGER,
    monstersslain INTEGER,
    bossesslain INTEGER,
    playersdefeated INTEGER,
    questscompleted INTEGER,
    areasexplored INTEGER,
    areastaken INTEGER,
    dungeonscleared INTEGER,
    coliseumwins INTEGER,
    itemsupgraded INTEGER,
    fishcaught INTEGER,
    distancetravelled INTEGER,
    reputation INTEGER,
    endlessrecord INTEGER,
    entriescompleted INTEGER
);
"""
create_langdb_query = """
CREATE TABLE IF NOT EXISTS langprefs (
discordid INTEGER PRIMARY KEY,
language TEXT
);
"""


async def setup_db():
    """
    Asynchronously sets up databases for storing player statistics and language preferences.
    Creates the necessary tables if they do not exist and initializes global variables.
    """
    # Stat DB
    global column_names
    stat_db = await StatDBConnection.get_instance()
    conn = await stat_db.get_connection()
    cur = await conn.cursor()
    await cur.execute(create_statdb_query)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_guildid ON playerstats (guildid)")
    await conn.commit()
    column_names = await fetch_column_names(cur, True)

    # Lang DB
    lang_db = await LangDBConnection.get_instance()
    conn = await lang_db.get_connection()
    cur = await conn.cursor()
    await cur.execute(create_langdb_query)
    await conn.commit()


def get_column_names():
    """
    Returns a list of column names from the player statistics database.
    """
    return column_names


async def check_and_update_record(playerstats, guild_id, playername):
    """
    Checks existing records for a player in the database and updates or inserts data accordingly.

    :param playerstats: Dictionary containing player statistics.
    :param guild_id: The guild ID associated with the player's record.
    :param playername: The name of the player.
    :return: A tuple containing a response message, the changed record and if applicable the differences.
    """
    day_str = datetime.utcnow().strftime('%Y-%m-%d')
    current_time = datetime.utcnow()
    response = "recordinserted"
    changed_row_id = None
    differences = None
    global column_names

    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()

    # check if there are already two records for this day
    await cur.execute("""
                SELECT *
                FROM playerstats
                WHERE guildid = ? AND playername = ?
                ORDER BY timestamp DESC
                LIMIT 2
            """, (guild_id, playername))

    records = await cur.fetchall()

    if not records:
        # Insert a new record if there are no existing records
        changed_row_id = await insert_new_record(cur, playerstats)
    else:
        most_recent_record = records[0]
        most_recent_record_id = most_recent_record[0]
        most_recent_timestamp_str = most_recent_record[3]
        most_recent_timestamp = datetime.strptime(most_recent_timestamp_str, '%Y-%m-%d %H:%M:%S')

        if current_time - most_recent_timestamp < timedelta(minutes=1):
            # merge and update the most recent record if its less than 1 minute old
            current_record_dict = {column_names[i]: most_recent_record[i + 5] for i in range(len(column_names))}
            merged_data = merge_record(current_record_dict, playerstats)

            # update the record in the database
            changed_row_id = await update_merged_record(cur, merged_data, most_recent_record_id)
            response = "recordmerged"
        elif len(records) >= 2 and datetime.strptime(records[1][3], '%Y-%m-%d %H:%M:%S').strftime(
                '%Y-%m-%d') == day_str:
            # if there are two or more records this day, update the most recent
            changed_row_id = await update_record(cur, playerstats, most_recent_record_id)
            response = "recordupdated"
        else:
            # insert a new record if there is only one record today and it's older than 1 minute
            changed_row_id = await insert_new_record(cur, playerstats)

        # if we updated or inserted a new record, we show the differences compared to the last record
        if response != "recordmerged":
            differences = calc_latest_difference(playerstats, most_recent_record)

    await conn.commit()
    await cur.execute("SELECT * FROM playerstats WHERE id = ?", (changed_row_id,))
    changed_record = await cur.fetchone()
    changed_record = changed_record[5:]  # extract the correct column values

    return response, changed_record, differences


def calc_latest_difference(playerstats, latest_record):
    """
    Calculates differences between inserted record and the previous record.

    :param playerstats: Dictionary containing the inserted record player stats.
    :param latest_record: Latest record from the database
    :return: Dictionary containing differences between inserted record and the previous
    """
    differences = dict()
    for column, value in zip(column_names, latest_record[5:]):
        try:
            safe_value = int(value) if value is not None else 0
        except ValueError:
            continue

        playerstat_value = playerstats.get(column, 0)
        if isinstance(playerstat_value, int):
            differences[column] = playerstat_value - safe_value
    return differences


async def fetch_column_names(cur, data_columns=False):
    """
    Fetches column names from the 'playerstats' table.

    :param cur: The database cursor.
    :param data_columns: Boolean indicating whether to fetch all columns or data columns only.
    :return: A list of column names.
    """
    await cur.execute(f'PRAGMA table_info(playerstats)')
    columns = await cur.fetchall()
    if data_columns:
        # Start at column 'level'
        return [column[1] for column in columns][5:]
    else:
        return [column[1] for column in columns]


async def update_record(cur, playerstats, record_id):
    """
    Updates an existing record with new player statistics.

    :param cur: The database cursor.
    :param playerstats: Dictionary containing new player statistics.
    :param record_id: The ID of the record to update.
    :return: The ID of the updated record.
    """
    # update existing record
    update_columns = ', '.join([f'{key} = :{key}' for key in playerstats.keys()])
    playerstats['id'] = record_id
    update_query = f'UPDATE playerstats SET {update_columns} WHERE id = :id'
    await cur.execute(update_query, playerstats)
    return record_id


async def update_latest_record(discord_id, category, new_value):
    """
    Updates the latest record for a given Discord ID with a new value for a specified category.

    :param discord_id: The Discord ID associated with the record.
    :param category: The name of the statistic to update.
    :param new_value: The new value for the category.
    :return: The updated latest record, if any.
    """
    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()

    cur = await conn.cursor()
    await cur.execute("""
                SELECT id FROM playerstats
                WHERE discordid = ?
                ORDER BY timestamp DESC
                LIMIT 1
    """, (discord_id,))
    latest_record = await cur.fetchone()
    if latest_record:
        latest_record_id = latest_record[0]

        # Update the specific column in the latest record
        update_query = f"UPDATE playerstats SET {category} = ? WHERE id = ?"
        await cur.execute(update_query, (new_value, latest_record_id))
        await conn.commit()
    return latest_record


async def update_specific_record(guild_id, playername, year, month, day, which, category, new_value):
    """
    Updates a specific record for a player with a new value for a specified category.

    :param guild_id: The guild ID associated with the record.
    :param playername: The name of the player.
    :param year: The year of the record to update.
    :param month: The month of the record to update.
    :param day: The day of the record to update.
    :param which: Indicates whether to update the 'first' or 'last' record for the given time frame.
    :param category: The name of the statistic to update.
    :param new_value: The new value for the category.
    :return: The updated record, if any.
    """
    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()

    time_frame_filter = construct_time_frame_filter(1, year=year, month=month, day=day)
    order_by = "ASC" if which == "first" else "DESC"

    # query to find the specific record ID
    query_find_id = f"""
        SELECT id FROM playerstats p1
        WHERE guildid = ? AND playername = ? AND {time_frame_filter}
        ORDER BY timestamp {order_by}
        LIMIT 1
    """
    cur = await conn.cursor()
    await cur.execute(query_find_id, (guild_id, playername))
    record = await cur.fetchone()
    if record:
        record_id = record[0]

        # update the specific record
        query_update_record = f"UPDATE playerstats SET {category} = ? WHERE id = ?"
        await cur.execute(query_update_record, (new_value, record_id))
        await conn.commit()
    return record


async def delete_specific_record(record_id):
    """
    deletes a specific record for a player

    :param record_id: the id of the record to delete
    """
    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()
    query_delete_record = f"DELETE FROM playerstats WHERE id = ?"
    await cur.execute(query_delete_record, (record_id,))
    await conn.commit()


async def fetch_specific_record(guild_id, playername, year, month, day, which):
    """
    fetch a specific record for a player

    :param guild_id: The guild ID associated with the record.
    :param playername: The name of the player.
    :param year: The year of the record to update.
    :param month: The month of the record to update.
    :param day: The day of the record to update.
    :param which: Indicates whether to update the 'first' or 'last' record for the given time frame.
    :return: The record and its id if found
    """
    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()

    time_frame_filter = construct_time_frame_filter(1, year=year, month=month, day=day)
    order_by = "ASC" if which == "first" else "DESC"

    # query to find the specific record
    query_find_record = f"""
           SELECT * FROM playerstats p1
           WHERE guildid = ? AND playername = ? AND {time_frame_filter}
           ORDER BY timestamp {order_by}
           LIMIT 1
       """
    cur = await conn.cursor()
    await cur.execute(query_find_record, (guild_id, playername))
    record = await cur.fetchone()
    if record:
        return record[5:], record[0]
    else:
        return None


async def purge_player_records(guild_id, playername):
    """
    deletes all records of a player

    :param guild_id: the id of the discord server
    :param playername: the name of the player
    :return deleted_count: the number of deleted rows

    """
    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()
    query_delete_records = f"DELETE FROM playerstats WHERE guildid = ? AND playername=?"
    await cur.execute(query_delete_records, (guild_id, playername))
    await conn.commit()
    deleted_count = cur.rowcount
    return deleted_count


async def update_merged_record(cur, merged_data, record_id):
    """
    Updates a record with merged data from two different sources.

    :param cur: The database cursor.
    :param merged_data: Dictionary containing the merged data to update the record with.
    :param record_id: The ID of the record to update.
    :return: The ID of the updated record.
    """
    update_columns = ', '.join([f"{key} = ?" for key in merged_data])
    values = list(merged_data.values()) + [record_id]

    update_query = f"UPDATE playerstats SET {update_columns} WHERE id = ?"
    await cur.execute(update_query, values)
    return record_id


async def insert_new_record(cur, playerstats):
    """
    Inserts a new record into the database with player statistics.

    :param cur: The database cursor.
    :param playerstats: Dictionary containing player statistics to insert.
    :return: The ID of the newly inserted record.
    """
    quoted_columns = ', '.join([f'"{key}"' for key in playerstats.keys()])
    quoted_placeholders = ', '.join([f':{key}' for key in playerstats.keys()])
    insert_query = f'INSERT INTO playerstats ({quoted_columns}) VALUES ({quoted_placeholders})'
    await cur.execute(insert_query, playerstats)
    return cur.lastrowid


async def calculate_changes(guild_id, category, scope, year, month=None, day=None, week=None, kingdom=None):
    """
    Calculates changes in player statistics over a specified time frame.

    :param guild_id: The guild ID for which to calculate changes.
    :param category: The category of statistics to calculate changes for.
    :param scope: The scope boolean of the scoreboard, either all servers or this server only where true is all servers
    :param year: The year of the time frame.
    :param month: Optional; the month of the time frame.
    :param day: Optional; the day of the time frame.
    :param week: Optional; the week number of the time frame.
    :param kingdom: Optional; the kingdom to filter records by.
    :return: A list of dictionaries detailing the changes in player statistics.
    """
    params = [] if scope else [guild_id]

    # create the query
    query = f"""
    SELECT p1.playername, p1.guildid, p1.{category} as before, p2.{category} as after
    FROM playerstats p1
    INNER JOIN playerstats p2 ON p1.playername = p2.playername {("" if scope else "AND p1.guildid = p2.guildid")}
    WHERE {construct_time_frame_filter(1, year, month, day, week)} {("" if scope else "AND p1.guildid = ?")}
  AND p1.id = (
        SELECT MIN(p3.id)
        FROM playerstats p3
        WHERE p3.playername = p1.playername {("" if scope else "AND p3.guildid = p1.guildid")}
        AND {construct_time_frame_filter(3, year, month, day, week)} {("" if not kingdom else "AND p3.kingdom = ?")}
    )
    AND p2.id = (
        SELECT MAX(p4.id)
        FROM playerstats p4
        WHERE p4.playername = p2.playername {("" if scope else "AND p4.guildid = p2.guildid")}
        AND {construct_time_frame_filter(4, year, month, day, week)} {("" if not kingdom else "AND p4.kingdom = ?")}
    )
    """

    # append kingdom to params if specified for subqueries
    if kingdom:
        params.extend([kingdom, kingdom])

    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()

    await cur.execute(query, params)
    records = await cur.fetchall()

    # process the records to calculate differences
    differences = []
    for record in records:
        start_value = 0
        end_value = 0

        # check if start_value is numeric
        if record[2] is not None and isinstance(record[2], int):
            start_value = record[2]

        # check if end_value is numeric
        if record[3] is not None and isinstance(record[3], int):
            end_value = record[3]

        # calculate the difference
        diff = {category: end_value - start_value}
        differences.append({'playername': record[0], 'guildid': record[1], 'differences': diff})

    return differences


async def get_scoreboard(guild_id, category, scope, ascending, limit, kingdom=None):
    """
    Retrieves a scoreboard of player statistics for a given guild.

    :param guild_id: The ID of the guild for which to retrieve the scoreboard.
    :param category: The statistic to generate the scoreboard for.
    :param ascending: Boolean indicating whether to sort the scoreboard in ascending order.
    :param limit: The maximum number of entries to include in the scoreboard.
    :param kingdom: Optional; the kingdom to filter the scoreboard by.
    :return: A list of dictionaries representing the scoreboard, or None if no records were found.
    """
    order = 'ASC' if ascending else 'DESC'

    where_clause = f'WHERE p.{category} IS NOT NULL {("" if scope else "AND p.guildid = ? ")}'

    # if kingdom is specified, add it to the WHERE clause
    if kingdom:
        where_clause += "AND p.kingdom = ?"

    query = f"""
            SELECT p.playername, p.{category}
            FROM playerstats p
            INNER JOIN (
                SELECT playername, MAX(id) as latest
                FROM playerstats
                {("" if scope else "WHERE guildid = ?")}
                GROUP BY playername
            ) as latest_record ON p.playername = latest_record.playername AND p.id = latest_record.latest
            {where_clause}
            ORDER BY p.{category} {order}
            LIMIT ?
        """

    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()

    # lrepare the parameters for the query
    params = [] if scope else [guild_id, guild_id]
    if kingdom:
        params.extend([kingdom, limit])
    else:
        params.append(limit)

    await cur.execute(query, params)
    records = await cur.fetchall()

    if records:  # check if any records were returned
        scoreboard = [{'playername': record[0], "value": record[1]} for record in records]
        return scoreboard
    else:
        return None


async def update_language(discord_id, language):
    """
    Updates the language preference for a given Discord ID.

    :param discord_id: The Discord ID to update the language preference for.
    :param language: The new language preference.
    """
    db = await LangDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()

    await cur.execute(
        "INSERT OR REPLACE INTO langprefs (discordid, language) VALUES (?, ?)",
        (discord_id, language)
    )
    await conn.commit()


async def get_language(discord_id):
    """
    Retrieves the language preference for a given Discord ID.

    :param discord_id: The Discord ID to get the language preference for.
    :return: The language preference if found, otherwise returns "en" (English) as default.
    """
    db = await LangDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()

    await cur.execute(
        "SELECT language FROM langprefs WHERE discordid = ?",
        (discord_id,)
    )
    record = await cur.fetchone()
    if record:
        language, = record
        return language
    else:
        return "en"


async def get_latest_record(guild_id, playername):
    """
    Retrieves the latest stats of a player.

    :param guild_id: Guild id of the discord server
    :param playername: The playername of the player to get the stats for
    :return: The latest record if found else none
    """
    db = await StatDBConnection.get_instance()
    conn = await db.get_connection()
    cur = await conn.cursor()

    await cur.execute("""
                   SELECT *
                   FROM playerstats
                   WHERE guildid = ? AND playername = ?
                   ORDER BY timestamp DESC
               """, (guild_id, playername))

    record = await cur.fetchone()
    if record:
        return record[5:]
    else:
        return None


# Helper functions
def merge_record(old_record, new_record):
    """
    Merges values from a new record into an old record, prioritizing non-zero and non-None values.

    :param old_record: The original record as a dictionary.
    :param new_record: The new record with potential updates.
    :return: A merged record dictionary.
    """
    merged_record = {}
    for key in old_record:
        old_value = old_record[key]
        new_value = new_record.get(key)

        if not old_value and new_value:
            merged_record[key] = new_value
        else:
            merged_record[key] = old_value
    return merged_record


def construct_time_frame_filter(i, year, month=None, day=None, week=None):
    """
    Constructs a SQL WHERE clause for filtering records by a specific time frame.

    :param i: An index used to differentiate multiple uses within a query.
    :param year: The year to filter by.
    :param month: Optional; the month to filter by.
    :param day: Optional; the day to filter by.
    :param week: Optional; the week number to filter by.
    :return: A SQL WHERE clause string.
    """
    if day and month:
        # Daily calculation
        return f"strftime('%Y-%m-%d', p{i}.timestamp) = '{year}-{str(month).zfill(2)}-{str(day).zfill(2)}'"
    elif month:
        # Monthly calculation
        return f"strftime('%Y-%m', p{i}.timestamp) = '{year}-{str(month).zfill(2)}'"
    elif week:
        # Weekly calculation
        start_date, end_date = get_start_end_dates(year, week=week)  # Get start and end dates for the week
        return f"p{i}.timestamp >= '{start_date}' AND p{i}.timestamp <= '{end_date}'"
    else:
        # Yearly calculation
        return f"strftime('%Y', p{i}.timestamp) = '{year}'"


def get_start_end_dates(year, month=None, day=None, week=None):
    """
    Calculates the start and end dates for a given year, month, day, or week.

    :param year: The year to calculate dates for.
    :param month: Optional; the month to calculate dates for.
    :param day: Optional; the day to calculate dates for.
    :param week: Optional; the week to calculate
    :return: Tuple with start and end dates
    """

    if week:
        # calculate the first day of the year and then add the number of weeks
        start_date = datetime(year, 1, 4) - timedelta(days=datetime(year, 1, 4).weekday()) + timedelta(weeks=week - 1)
        end_date = start_date + timedelta(days=6)
    elif day and month:
        # daily interval
        start_date = end_date = datetime(year, month, day)
    elif month:
        # monthly interval
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)  # Handle December case
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    else:
        # yearly interval
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
