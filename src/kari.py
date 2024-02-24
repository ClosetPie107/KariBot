"""
Copyright © 2024, ClosetPie107 <closetpie107@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""

import calendar
import os
import discord
import json
import time
from pytesseract import pytesseract
from db_related import *
from dotenv import load_dotenv
from discord.ext.commands import has_permissions
from discord import guild_only, Option
from ocr_related import process_images_tess
from fuzzywuzzy import process

intents = discord.Intents.default()
bot = discord.Bot(intents=intents)
load_dotenv()
token = str(os.getenv('TOKEN'))
translation_cache = dict()  # dictionary to store loaded translations

categories = [
    discord.OptionChoice(name="Ascension Level", value="ascensionlevel"),
    discord.OptionChoice(name="Playtime", value="playtime"),
    discord.OptionChoice(name="Travelers's Guild", value="travelersguild"),
    discord.OptionChoice(name="Angler's Guild", value="anglersguild"),
    discord.OptionChoice(name="Circle of Anguish", value="circleofanguish"),
    discord.OptionChoice(name="Titanfelled Guild", value="titanfelledguild"),
    discord.OptionChoice(name="Blades of Finesse", value="bladesoffinesse"),
    discord.OptionChoice(name="Spelunking Guild", value="spelunkingguild"),
    discord.OptionChoice(name="Seer's Guild", value="seersguild"),
    discord.OptionChoice(name="Monumental Guild", value="monumentalguild"),
    discord.OptionChoice(name="Global Rank", value="globalrank"),
    discord.OptionChoice(name="Regional Rank", value="regionalrank"),
    discord.OptionChoice(name="Competitive Rank", value="competitiverank"),
    discord.OptionChoice(name="Monsters Slain", value="monstersslain"),
    discord.OptionChoice(name="Bosses Slain", value="bossesslain"),
    discord.OptionChoice(name="Players Defeated", value="playersdefeated"),
    discord.OptionChoice(name="Quests Completed", value="questscompleted"),
    discord.OptionChoice(name="Dungeons Cleared", value="dungeonscleared"),
    discord.OptionChoice(name="Coliseum Wins", value="coliseumwins"),
    discord.OptionChoice(name="Items Upgraded", value="itemsupgraded"),
    discord.OptionChoice(name="Fish Caught", value="fishcaught"),
    discord.OptionChoice(name="Distance Travelled", value="distancetravelled"),
    discord.OptionChoice(name="Reputation", value="reputation"),
    discord.OptionChoice(name="Endless Record", value="endlessrecord"),
    discord.OptionChoice(name="Entries Completed", value="entriescompleted")]


@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}!')
    await setup_db()
    create_translation_cache()


@bot.slash_command(name="upload_stats", description="Upload your player stats by providing screenshots")
@guild_only()
async def upload_stats(ctx,
                       playername: Option(str, "Enter your name", max_length=40),
                       image: Option(discord.Attachment, "First image"),
                       secondimage: Option(discord.Attachment, "Second image", required=False)):
    await ctx.defer()  # avoid timeout

    # fetch user preferred language
    language = await get_language(ctx.author.id)
    language_file = translation_cache[language]

    # check image validity
    if image is None or not image.content_type.startswith("image/"):
        await ctx.respond(language_file.get("invalidimage"))
        return

    # attempt to process the images to extract the player stats information
    playerstats = dict()
    st = time.time()

    try:
        if secondimage and secondimage.content_type.startswith("image/"):
            playerstats = await process_images_tess(image.url, playername, ctx.guild.id, ctx.author.id, language_file,
                                                    secondimage.url)
        else:
            playerstats = await process_images_tess(image.url, playername, ctx.guild.id, ctx.author.id, language_file)
    except Exception as e:
        await ctx.followup.send(language_file.get("errorimageprocess"))
        print(e)

    print(f"{time.time() - st} seconds")

    # attempt to insert / update the record in the database
    response, changed_record, differences = await check_and_update_record(playerstats, ctx.guild.id, playername)
    column_names = get_column_names()
    localized_column_names = [language_file.get(column, column) for column in column_names]
    message = f"{language_file.get(response)}\n```"
    if differences:
        for localized_attribute, attribute, value in zip(localized_column_names, column_names, changed_record):
            difference = differences.get(attribute, None)
            if not difference:
                formatted_difference = ""
            elif difference > 0:
                formatted_difference = f"(+{difference})"
            else:
                formatted_difference = f"({difference})"
            message += f"\n{localized_attribute}: {value}   {formatted_difference}"
    else:
        for localized_attribute, value in zip(localized_column_names, changed_record):
            message += f"\n{localized_attribute}: {value}"
    message += "```"
    await ctx.followup.send(message)


@bot.slash_command(name="correct_latest", description="Update a category in your latest record")
@guild_only()
async def correct_latest(ctx,
                         category: Option(str, "Specify the category (e.g., bosses slain)", max_length=40),
                         new_value: Option(str, "Enter the new value for the stat", max_length=40)):
    await ctx.defer()  # avoid timeout

    # fetch user preferred language and obtain localized column names
    language = await get_language(ctx.author.id)
    language_file = translation_cache[language]

    column_names = get_column_names()
    localized_column_names = dict()
    for column in column_names:
        localized_column_names[''.join(language_file.get(column,
                                                         column).split()).lower()] = column  # dicitonary that maps localized column names to original column names

    # fuzzy match the input category
    match = process.extractOne(category, localized_column_names.keys())
    best_match = None
    if match:
        best_match, score = match
        if score < 90:
            await ctx.followup.send(language_file.get("invalidcategoryname"))
            return
    else:
        await ctx.followup.send(language_file.get("invalidcategoryname"))
        return

    # check some input values
    error_message = is_valid_input(localized_column_names[best_match], new_value)
    if error_message != "":
        await ctx.followup.send(language_file.get(error_message))
        return

    # try to update the record
    latest_record = await update_latest_record(ctx.author.id, localized_column_names[best_match], new_value)
    if latest_record:
        await ctx.followup.send(
            f'{language_file.get("recordupdated")} {language_file.get(localized_column_names[best_match])} → {new_value}')
    else:
        await ctx.followup.send(language_file.get("norecordfound"))


@bot.slash_command(name="alter_record", description="Alter a specific player's record")
@guild_only()
@has_permissions(administrator=True)  # This ensures only admins can use this command
async def alter_record(ctx,
                       playername: Option(str, "Enter the player's name", max_length=40),
                       date: Option(str, "Enter the date of the record in yyyy-mm-dd format"),
                       record_option: Option(str, "Choose first (oldest) or second (latest) record on this date",
                                             choices=["first", "second"]),
                       category: Option(str, "Specify the category (e.g., bosses slain)", max_length=40),
                       new_value: Option(str, "Enter the new value", max_length=40)):
    await ctx.defer()

    # fetch user preferred language
    language = await get_language(ctx.author.id)
    language_file = translation_cache[language]

    # validate the record date
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        await ctx.followup.send(language_file.get("invaliddate"))
        return

    #  obtain localized column names
    column_names = get_column_names()
    localized_column_names = dict()
    for column in column_names:
        localized_column_names[''.join(language_file.get(column,
                                                         column).split()).lower()] = column  # dicitonary that maps localized column names to original column names

    # fuzzy match the input category
    match = process.extractOne(category, localized_column_names.keys())
    best_match = None
    if match:
        best_match, score = match
        if score < 90:
            await ctx.followup.send(language_file.get("invalidcategoryname"))
            return
    else:
        await ctx.followup.send(language_file.get("invalidcategoryname"))
        return

    # check some input values
    error_message = is_valid_input(localized_column_names[best_match], new_value)
    if error_message != "":
        await ctx.followup.send(language_file.get(error_message))
        return

    # extract year, month, and day from the date object
    year, month, day = date_obj.year, date_obj.month, date_obj.day

    # try to update the record
    result = await update_specific_record(ctx.guild.id, playername, year, month, day, record_option,
                                          localized_column_names[best_match], new_value)
    if result:
        await ctx.followup.send(
            f'{language_file.get("recordaltered")} {language_file.get(localized_column_names[best_match])} → {new_value}')
    else:
        await ctx.followup.send(f'{language_file.get("norecordfound")}')


@bot.slash_command(name="scoreboard", description="Get the scoreboard for a specific category")
@guild_only()
async def scoreboard(ctx,
                     category: Option(str, "The leaderboard category", choices=categories),
                     kingdom: Option(str, "Name of the kingdom", default=None, max_length=40),
                     scope: Option(str, "The scope of the leaderboard", choices=["This Server", "All Servers"],
                                   default="This Server"),
                     n: Option(int, "Number of players on the leaderboard (max 25)", default=10, min_value=1,
                               max_value=25)):
    await ctx.defer()  # avoid timeout

    # fetch user preferred language
    language = await get_language(ctx.author.id)
    language_file = translation_cache[language]

    # categories for which the order should be reversed
    asc_categories = ["globalrank", "regionalrank", "competitiverank"]
    asc = category in asc_categories

    # fetch scoreboard
    scoreboard = await get_scoreboard(ctx.guild.id, category, scope == "All Servers", asc, n,kingdom)
    title = f"{language_file.get('scoreboardfor')} {language_file.get(category)}"
    if kingdom:
        title = f"({kingdom}) {title}"

    if scoreboard:  # Check if a valid scoreboard was returned
        embed = discord.Embed(title=title, color=0xa84232)
        for entry in scoreboard:
            embed.add_field(name=entry['playername'], value=entry['value'], inline=False)
        await ctx.followup.send(embed=embed)
    else:
        await ctx.followup.send(language_file.get("noscoreboarddata"))


@bot.slash_command(name="year_scoreboard", description="Get the yearly scoreboard for a specific category")
@guild_only()
async def yearly_scoreboard(ctx, category: Option(str, "The leaderboard category", choices=categories),
                            kingdom: Option(str, "Name of the kingdom", default=None, max_length=40),
                            scope: Option(str, "The scope of the leaderboard", choices=["This Server", "All Servers"],
                                          default="This Server"),
                            year: Option(int, "Enter the year", default=None, min_value=1, max_value=999999),
                            n: Option(int, "Number of players on the leaderboard (max 25)", default=10, min_value=1,
                                      max_value=25)):
    await generate_scoreboard(ctx, "yearly", category, kingdom=kingdom, scope=scope, year=year, n=n)


@bot.slash_command(name="month_scoreboard", description="Get the monthly scoreboard for a specific category")
@guild_only()
async def monthly_scoreboard(ctx, category: Option(str, "The leaderboard category", choices=categories),
                             kingdom: Option(str, "Name of the kingdom", default=None, max_length=40),
                             scope: Option(str, "The scope of the leaderboard", choices=["This Server", "All Servers"],
                                           default="This Server"),
                             year: Option(int, "Enter the year", default=None, min_value=1, max_value=999999),
                             month: Option(int, "Enter the month number", default=None, min_value=1, max_value=12),
                             n: Option(int, "Number of players on the leaderboard (max 25)", default=10, min_value=1,
                                       max_value=25)):
    await generate_scoreboard(ctx, "monthly", category, kingdom=kingdom, scope=scope, year=year, month=month, n=n)


@bot.slash_command(name="day_scoreboard", description="Get the daily scoreboard for a specific category")
@guild_only()
async def day_scoreboard(ctx, category: Option(str, "The leaderboard category", choices=categories),
                         kingdom: Option(str, "Name of the kingdom", default=None, max_length=40),
                         scope: Option(str, "The scope of the leaderboard", choices=["This Server", "All Servers"],
                                       default="This Server"),
                         year: Option(int, "Enter the year", default=None, min_value=1, max_value=999999),
                         month: Option(int, "Enter the month number", default=None, min_value=1, max_value=12),
                         day: Option(int, "Enter the day number", default=None, min_value=1, max_value=31),
                         n: Option(int, "Number of players on the leaderboard (max 25)", default=10, min_value=1,
                                   max_value=25)):
    await generate_scoreboard(ctx, "daily", category, kingdom=kingdom, scope=scope, year=year, month=month, day=day,
                              n=n)


@bot.slash_command(name="week_scoreboard", description="Get the weekly scoreboard for a specific category")
@guild_only()
async def weekly_scoreboard(ctx, category: Option(str, "The leaderboard category", choices=categories),
                            kingdom: Option(str, "Name of the kingdom", default=None, max_length=40),
                            scope: Option(str, "The scope of the leaderboard", choices=["This Server", "All Servers"],
                                          default="This Server"),
                            year: Option(int, "Enter the year", default=None, min_value=1, max_value=999999),
                            week: Option(int, "Enter the week number", default=None, min_value=1, max_value=53),
                            n: Option(int, "Number of players on the leaderboard (max 25)", default=10, min_value=1,
                                      max_value=25)):
    await generate_scoreboard(ctx, "weekly", category, kingdom=kingdom, scope=scope, year=year, week=week, n=n)


@bot.slash_command(name="set_language", description="Set your language preference")
async def set_langauge(ctx, language: Option(str, "Language", choices=[
    discord.OptionChoice(name="English", value="en"),
    discord.OptionChoice(name="Deutsch", value="de"),
    discord.OptionChoice(name="Français", value="fr")])):
    await update_language(ctx.author.id, language)
    language_file = translation_cache[language]
    await ctx.respond(language_file.get("languageupdated"))


@bot.slash_command(name="help", description="Shows help information for commands")
async def help_command(ctx):
    language = await get_language(ctx.author.id)
    language_file = translation_cache[language]

    embed = discord.Embed(title=language_file.get("help"), description=language_file.get("help_title"), color=0x00ff00)
    # Example of adding commands and their descriptions to the embed
    embed.add_field(name="/set_language",
                    value=language_file.get("help_setlanguage"), inline=False)
    embed.add_field(name="/upload_stats",
                    value=language_file.get("help_uploadstats"), inline=False)
    embed.add_field(name="/correct_latest",
                    value=language_file.get("help_correctlatest"), inline=False)
    embed.add_field(name="/alter_record",
                    value=language_file.get("help_alterrecord"), inline=False)
    embed.add_field(name="/get_record",
                    value=language_file.get("help_getrecord"), inline=False)
    embed.add_field(name="/scoreboard",
                    value=language_file.get("help_scoreboard"), inline=False)
    embed.add_field(name="/year_scoreboard",
                    value=language_file.get("help_year_scoreboard"), inline=False)
    embed.add_field(name="/month_scoreboard",
                    value=language_file.get("help_month_scoreboard"), inline=False)
    embed.add_field(name="/week_scoreboard",
                    value=language_file.get("help_week_scoreboard"), inline=False)
    embed.add_field(name="/day_scoreboard",
                    value=language_file.get("help_day_scoreboard"), inline=False)
    await ctx.respond(embed=embed)


@bot.slash_command(name="get_record", description="Shows the latest record of a specific player")
@guild_only()
async def get_record(ctx,
                     playername: Option(str, "Enter the player's name", max_length=40)):
    await ctx.defer()
    # fetch user preferred language and obtain localized column names
    language = await get_language(ctx.author.id)
    language_file = translation_cache[language]

    latest_record = await get_latest_record(ctx.guild.id, playername)
    column_names = get_column_names()
    localized_column_names = [language_file.get(column, column) for column in column_names]
    if latest_record:
        message = f"{language_file.get('showingrecord')} {playername}\n```"
        for localized_attribute, value in zip(localized_column_names, latest_record):
            message += f"\n{localized_attribute}: {value}"
        message += "```"
    else:
        message = f"{language_file.get('norecordfound')}"
    await ctx.followup.send(message)


# Helper functions
async def generate_scoreboard(ctx, scoreboard_type, category, scope, kingdom=None, year=None, month=None, day=None,
                              week=None,
                              n=10):
    """
        Asynchronously generates and sends a scoreboard embed to a Discord context based on the given parameters.

        :param ctx: The Discord context where the command was invoked.
        :param scoreboard_type: The type of scoreboard to generate (e.g., "monthly", "daily", "weekly").
        :param category: The category for which the scoreboard is being generated.
        :param kingdom: Optional; the kingdom to filter the scoreboard by.
        :param year: Optional; the year to filter the scoreboard by. Defaults to the current year.
        :param month: Optional; the month to filter the scoreboard by. Relevant for "monthly" and "daily" types.
        :param day: Optional; the day to filter the scoreboard by. Relevant for "daily" type.
        :param week: Optional; the week to filter the scoreboard by. Relevant for "weekly" type.
        :param n: Optional; the number of top entries to display. Defaults to 10.
        :return: None.
        """
    await ctx.defer()  # avoid timeout

    # fetch user preferred language
    language = await get_language(ctx.author.id)
    language_file = translation_cache[language]

    # default values based on current date and scoreboard_type
    current_date = datetime.utcnow()
    year = abs(year) if year is not None else current_date.year
    if scoreboard_type in ("monthly", "daily"):
        month = month or current_date.month
    if scoreboard_type == "daily":
        day = day or current_date.day
    if scoreboard_type == "weekly":
        week = week or current_date.isocalendar()[1]

    # check if date is valid
    message_key = is_valid_date(year, month, week, day)
    if message_key != "":
        await ctx.respond(language_file.get(message_key))
        return

    asc_categories = ["globalrank", "regionalrank", "competitiverank"]
    asc = category in asc_categories

    # Calculate the progress and sort the records
    changes = await calculate_changes(ctx.guild.id, category, scope == "All Servers", year, month, day, week, kingdom)
    sorted_changes = sorted(changes, key=lambda x: x['differences'].get(category, 0), reverse=not asc)[:n]

    # determine start and end dates for the embed title, then add the embed fields
    start_date, end_date = get_start_end_dates(year, month, day, week)
    title = f"{language_file.get(scoreboard_type)} {language_file.get('scoreboardfor')} {language_file.get(category)} {start_date} — {end_date}"
    if kingdom:
        title = f"({kingdom}) {title}"
    embed = discord.Embed(title=title, color=0x328ba8)
    for entry in sorted_changes:
        player_name = entry['playername']
        category_progress = entry['differences'].get(category, 0)
        embed.add_field(name=player_name, value=f"{language_file.get(category)}: {category_progress}", inline=False)
    await ctx.followup.send(embed=embed)


def is_valid_date(year, month=None, week=None, day=None):
    """
        Validates the provided date components for logical correctness and conformity to calendar rules.

        :param year: The year component of the date.
        :param month: Optional; the month component of the date.
        :param week: Optional; the week number component of the date.
        :param day: Optional; the day component of the date.
        :return: A string indicating the type of date validation error, if any; an empty string indicates a valid date.
        """
    # check if the week is valid
    if week is not None and week == 53:
        last_day_of_year = datetime(year, 1, 4)
        week_number = last_day_of_year.isocalendar()[1]
        if week_number != 53:
            return "53weeks"

    # check if the day is valid
    if day is not None and month is not None:
        # Check if the day exists in the given month
        max_day = calendar.monthrange(year, month)[1]
        if day < 1 or day > max_day:
            return "invalidday"

    # the inputs are valid
    return ""


def is_valid_input(category, new_value):
    """
       Validates a new value for a specified category against predefined rules and formats.

       :param category: The category of the input to validate (e.g., "kingdom", "datecreated").
       :param new_value: The new value to validate for the specified category.
       :return: A string indicating the type of validation error, if any; an empty string indicates valid input.
       """
    if category == "kingdom":
        return ""
    elif category == "datecreated":
        try:
            # Attempt to convert the string to a datetime object
            date_obj = datetime.strptime(new_value, "%Y-%m-%d")
        except ValueError:
            return "invaliddate"

    # general integer conversion for the other categories
    try:
        new_value = int(new_value)
    except ValueError:
        return "invalidinput"

    if category == "level":
        return "" if 1 <= int(new_value) <= 250 else "invalidnumber"
    elif category in (
            "travelersguild", "anglersguild", "circleofanguish", "titanfelledguild", "bladesoffinesse",
            "spelunkingguild",
            "seersguild", "monumentalguild", "endlessrecord"):
        return "" if 0 <= int(new_value) <= 9999 else "invalidnumber"
    else:
        return "" if 0 <= int(new_value) <= 99999999 else "invalidnumber"


def create_translation_cache():
    """
    Loads translation files from a specified folder into a cache for quick access.

    This function iterates over JSON files in a "locales" folder, assuming each file's name (minus the extension) corresponds to a language code. Each file's contents are loaded into a dictionary, keyed by the language code, facilitating quick lookups for translations.

    :return: None.
    """
    locales_folder = "../locales"
    for filename in os.listdir(locales_folder):
        if filename.endswith(".json"):
            language_code = filename.split('.')[0]  # language code from filename
            with open(os.path.join(locales_folder, filename), "r", encoding="utf-8") as f:
                translation_cache[language_code] = json.load(f)


if __name__ == "__main__":
    bot.run(token)
