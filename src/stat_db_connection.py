"""
Copyright © 2024, ClosetPie107 <closetpie107@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""

import aiosqlite
from asyncio import Lock


class StatDBConnection:
    """
    Singleton class to manage a SQLite database connection for storing player statistics.

    This class ensures that only one instance of the database connection is created throughout the application. It uses aiosqlite to perform asynchronous database operations.

    Attributes:
        _instance (StatDBConnection): The singleton instance of the StatDBConnection class.
        _lock (Lock): An asyncio Lock object to ensure thread-safe initialization of the singleton instance.
        _connection (aiosqlite.Connection): The SQLite database connection.

    Methods:
        get_instance: Returns the singleton instance of the StatDBConnection class, creating it if it does not exist.
        get_connection: Returns the aiosqlite database connection.
    """

    _instance = None
    _lock = Lock()

    def __init__(self):
        if StatDBConnection._instance is not None:
            raise Exception("There can only be one stat DB connection instance!")
        else:
            self._connection = None

    @staticmethod
    async def get_instance():
        async with StatDBConnection._lock:
            if StatDBConnection._instance is None:
                StatDBConnection._instance = StatDBConnection()
                await StatDBConnection._instance._initialize()
            return StatDBConnection._instance

    async def _initialize(self):
        if self._connection is None:
            self._connection = await aiosqlite.connect('../playerstats.db')

    async def get_connection(self):
        return self._connection
