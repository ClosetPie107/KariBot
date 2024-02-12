"""
Copyright © 2024, ClosetPie107 <closetpie107@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""

import re
from datetime import datetime
import concurrent.futures
import aiohttp
import pytesseract
import asyncio
import io
# from paddleocr import PaddleOCR, draw_ocr
from fuzzywuzzy import fuzz
from PIL import Image, ImageOps, ImageEnhance
from pytesseract import image_to_string
from src import db_related

executor = concurrent.futures.ThreadPoolExecutor()  # global executor


async def process_images_tess(image_url, playername, guild_id, discord_id, language_file, second_image_url=None):
    """
        Asynchronously processes one or two images for OCR to extract player stats.
        Handles image fetching, OCR, and post-processing.

        :param image_url: URL of the first image to process.
        :param playername: Player's name.
        :param guild_id: ID of the discord server.
        :param discord_id: Discord ID of the player.
        :param language_file: A dict containing OCR language and mappings for column names.
        :param second_image_url: Optional URL of the second image to process.
        :return: A dictionary containing sanitized OCR results including player stats.
        """
    playerstats = {}
    async with aiohttp.ClientSession() as session:
        playerstats, visited = await fetch_and_process_image(session, image_url, ocr_processing, language_file)
        playerstats = sanitize_ocr_results(playerstats)  # Assuming sanitize_ocr_results is defined elsewhere

        if second_image_url:
            playerstats2, visited2 = await fetch_and_process_image(session, second_image_url, ocr_processing,
                                                                   language_file)
            playerstats2 = sanitize_ocr_results(playerstats2)
            # merge dictionaries
            for key, value in playerstats2.items():
                if key in visited2 and (key not in visited or visited2[key] > visited[key]):
                    playerstats[key] = value

        playerstats['playername'] = playername
        playerstats['guildid'] = guild_id
        playerstats['discordid'] = discord_id

    return playerstats


# asynchronously fetch the image and call the ocr and post processing function
async def fetch_and_process_image(session, url, ocr_processing_func, language_file):
    """
    Asynchronously fetches an image from a URL, performs OCR, and processes the text.

    :param session: The aiohttp ClientSession object for making HTTP requests.
    :param url: The URL of the image to fetch and process.
    :param ocr_processing_func: The OCR processing function to use.
    :param language_file: A dict containing OCR language and mappings for column names.
    :return: A tuple (playerstats, visited) where playerstats is a dict of extracted information,
             and visited tracks which fields have been processed.
    """
    async with session.get(url) as response:
        response.raise_for_status()
        image_data = await response.read()
        byte_stream = io.BytesIO(image_data)
        img = Image.open(byte_stream)
        loop = asyncio.get_running_loop()
        img_text = await loop.run_in_executor(executor, ocr_processing_func, img,
                                              language_file)  # run ocr in separate thread
        print(img_text)
        lines = img_text.strip().split("\n")
        playerstats, visited = await process_text_tess(lines,
                                                       language_file)  # post process
        return playerstats, visited


# function that preprocesses and performs ocr on the image
def ocr_processing(img, language_file):
    """
    Preprocesses an image and performs OCR to extract text.

    :param img: The PIL Image object to process.
    :param language_file: A dict containing OCR language and mappings for column names.
    :return: The extracted text as a string.
    """
    width, height = img.size
    smallest_dim = min(width, height)

    min_dim = 1080
    max_dim = 1440
    if smallest_dim < min_dim:
        scaling_factor = min_dim / smallest_dim
    elif smallest_dim > max_dim:
        scaling_factor = max_dim / smallest_dim
    else:
        scaling_factor = 1

    if scaling_factor != 1:
        img = img.resize((round(width * scaling_factor), round(height * scaling_factor)), Image.LANCZOS)

    img = img.convert('L')
    img = ImageOps.invert(img)
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(2)
    img.save("latest.png")  # for debugging
    img_text = image_to_string(img,
                               config=f'--psm 6 --oem 1 -c preserve_interword_spaces=1 -l {language_file.get("tesseractmodel")}')
    return img_text


# post process
async def process_text_tess(img_text, language_file):
    """
    Processes OCR text to extract and map information to database column names.

    :param img_text: List of text lines extracted from the image.
    :param language_file: A dict containing OCR language and mappings for column names.
    :return: A tuple (processed, visited) where processed is a dict of extracted information,
             and visited tracks which fields have been processed.
    """
    column_names = db_related.get_column_names()  # db column names
    localized_column_names = dict()
    processed = dict()
    visited = dict()

    for column in column_names:
        localized_column_names[''.join(language_file.get(column,
                                                         column).split()).lower()] = column  # dicitonary that maps localized column names to original column names

    for line in img_text:
        key_value = re.split(r"\s{2,}", line, maxsplit=1)

        # if the line correctly splits into two parts
        if len(key_value) == 2:
            key, value = key_value
            print(f"key: {key}, value: {value}")

            # fuzzy match the key with localized column names
            key_no_spaces = ''.join(key.split()).lower()
            match = find_best_match(key_no_spaces, localized_column_names.keys())
            if match:
                best_match, score = match
                column_key = localized_column_names[best_match]  # get the db column name from the localized column name

                if column_key in visited and visited[column_key] > score:  # go to next iteration if no better match
                    continue

                if column_key in ['kingdom', 'class']:
                    processed[column_key] = value
                else:
                    processed[column_key] = extract_numeric_value(column_key,value)
                visited[column_key] = score

    return processed, visited


# post process numeric values
def extract_numeric_value(key, value):
    """
       Processes numeric attributes to extract the correct values.

       :param key: The attribute
       :param value: The unprocessed attribute value
       :return: The processed attribute value
       """
    # handle the general extraction of digits and possible trailing single/double digit removal (caused by misrecognized question marks)
    if key != 'playtime' and key != 'datecreated':
        value = re.sub(r'\D+', ' ', value)  # replace one or more non digit chars with a single space
        value.strip()
        parts = value.split()
        if len(parts) > 1 and parts[-2].isdigit() and parts[-1].isdigit() and 1 <= len(parts[-1]) <= 2:
            # remove the last part if it's 1 or 2 digits following a single number (misrecognized question mark)
            value = ' '.join(parts[:-1])
        else:
            value = value

    value = ''.join(filter(str.isdigit, value))  # remove remaining non digits

    if key == 'playtime':
        try:
            return convert_to_hours(value)
        except ValueError:
            return 0

    if key == 'datecreated':
        return format_date_string(value)

    try:
        return int(value)
    except ValueError:
        return None if value == "" else value


def format_date_string(date_str):
    """
    Converts a date string from 'YYYYMMDD' format to 'YYYY-MM-DD' format.

    :param date_str: A string representing a date in 'YYYYMMDD' format.
    :return: A string representing the formatted date in 'YYYY-MM-DD' format,
             or None if the input string is not a valid date.
    """
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        formatted_date = date_obj.strftime('%Y-%m-%d')
        return formatted_date
    except ValueError:
        # return the original string if it's not a valid date
        return None


def convert_to_hours(playtime):
    """
    Converts a string representing the playtime in days and hours to the playtime in hours. 

    :param playtime: A string representing a time duration.
    :return: An integer representing the total number of hours.
    """
    if len(playtime) <= 2:  # if the string is 2 characters or less, it's only hours
        return int(playtime)
    else:
        days = int(playtime[:-2])
        hours = int(playtime[-2:])
        return days * 24 + hours


def sanitize_ocr_results(ocr_results):
    """
    Sanitizes OCR results by enforcing type constraints and value ranges
    for each recognized field, to ensure that they will not cause database issues

    :param ocr_results: A dictionary containing OCR-extracted information.
    :return: A dictionary with sanitized results, ensuring integer values
             fall within a specified range and converting other types appropriately.
    """
    min_val = 0
    max_val = 9999999999

    sanitized_results = {}
    for key, value in ocr_results.items():
        if isinstance(value, int):  # If the value is an integer, sanitize it
            if value < min_val:
                sanitized_value = min_val
            elif value > max_val:
                sanitized_value = max_val
            else:
                sanitized_value = value
            sanitized_results[key] = sanitized_value
        elif isinstance(value, str):  # If the value is a string, keep it as is
            sanitized_results[key] = value
        else:
            sanitized_results[key] = str(value)
    return sanitized_results


def similarity_score(score, best_match, key):
    """
    Calculates a weighted similarity score based on fuzzy matching score and
    length similarity between the key and its best match.

    :param score: The fuzzy matching score as an integer.
    :param best_match: The best matching string found.
    :param key: The original key string being matched.
    :return: A float representing the weighted similarity score.
    """
    key_no_spaces = ''.join(key.split())
    len_diff = abs(len(best_match) - len(key_no_spaces))
    max_len = max(len(best_match), len(key_no_spaces))

    normalized_len_diff = len_diff / max_len if max_len > 0 else 0
    len_similarity = 1 - normalized_len_diff
    weighted_score = 0.7 * score / 100 + 0.3 * len_similarity
    return weighted_score


def find_best_match(key, column_names):
    """
    Finds the best match for a given key within a list of column names using
    fuzzy matching and a custom similarity scoring function.

    :param key: The key string to find a match for.
    :param column_names: A list of column name strings to search within.
    :return: A tuple (best_match, highest_score) where best_match is the column
             name that best matches the key, and highest_score is the score of
             that match.
    """
    best_match = None
    highest_score = -1

    for column_name in column_names:
        fuzzy_score = fuzz.ratio(key, column_name)
        weighted_score = similarity_score(fuzzy_score, column_name, key)

        if weighted_score > highest_score:
            best_match = column_name
            highest_score = weighted_score
    return best_match, highest_score
