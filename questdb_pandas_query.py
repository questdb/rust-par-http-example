#!/usr/bin/env python3


"""
Run:

pip install aiohttp[speedups] pyarrow
"""

import asyncio
import aiohttp
import pandas as pd
from io import BytesIO
from enum import Enum
from concurrent.futures import ThreadPoolExecutor

from typing import Optional


class QueryError(Exception):
    def __init__(self, message: str, query: str, position: Optional[int] = None):
        super().__init__(message)
        self.query = query
        self.position = position

    @classmethod
    def from_json(cls, json: dict):
        return cls(
            message=json['error'],
            query=json['query'],
            position=json.get('position'))


class Endpoint:
    def __init__(self, host, port, https=True, username=None, password=None):
        self.host = host
        self.port = port
        self.https = https
        self.username = username
        self.password = password

    @property
    def url(self):
        protocol = 'https' if self.https else 'http'
        return f'{protocol}://{self.host}:{self.port}'

    def new_session(self):
        auth = None
        if self.username is not None:
            if self.password is not None:
                raise ValueError('Password specified without username')
            auth = aiohttp.BasicAuth(self.username, self.password)
        return aiohttp.ClientSession(auth=auth)


async def _pre_query(session: aiohttp.ClientSession, endpoint: Endpoint, query: str) -> tuple[list[tuple[str, (str, object)]], int]:
    url = f'{endpoint.url}/exec'
    params = [('query', query), ('count', 'true'), ('limit', '0')]
    dtypes_map = {
        'STRING': ('STRING', str),
        'SYMBOL': ('SYMBOL', 'category'),
        'DOUBLE': ('DOUBLE', 'float64'),
        'TIMESTAMP': ('TIMESTAMP', None)
    }
    async with session.get(url=url, params=params) as resp:
        result = await resp.json()
        if resp.status != 200:
            raise QueryError.from_json(result)
        columns = [
            (col['name'], dtypes_map[col['type'].upper()])
            for col in result['columns']]
        count = result['count']
        return columns, count


async def _query_pandas(
        session: aiohttp.ClientSession,
        executor: ThreadPoolExecutor,
        endpoint: Endpoint,
        query: str,
        result_schema: list[tuple[str, tuple[str, object]]],
        limit_range: tuple[int, int]) -> pd.DataFrame:
    url = f'{endpoint.url}/exp'
    params = [
        ('query', query),
        ('limit', f'{limit_range[0]},{limit_range[1]}')]
    async with session.get(url=url, params=params) as resp:
        if resp.status != 200:
            raise QueryError.from_json(await resp.json())
        buf = await resp.content.read()
        download_bytes = len(buf)
        buf_reader = BytesIO(buf)
        dtypes = {
            col[0]: col[1][1]
            for col in result_schema
            if col[1][1] is not None}

        def _read_csv():
            df = pd.read_csv(buf_reader, dtype=dtypes, engine='pyarrow')
            # Patch up the column types.
            for col_schema in result_schema:
                col_name = col_schema[0]
                col_type = col_schema[1][0]
                try:
                    if col_type == 'TIMESTAMP':
                        series = df[col_name]
                        series = pd.to_datetime(series)
                        df[col_name] = series
                except Exception as e:
                    raise ValueError(
                        f'Failed to convert column {col_name} to type {col_type}: {e}\n{series}')
            return df

        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(executor, _read_csv)
        return df, download_bytes


async def pandas_query(endpoint: Endpoint, query: str, chunks: int = 1, stats: bool = False) -> pd.DataFrame:
    """
    Query QuestDB via CSV to a Pandas DataFrame.
    """
    # if 'limit' in query.lower():
    #     raise ValueError('SQL LIMIT is not supported in queries to Pandas')
    with ThreadPoolExecutor(max_workers=chunks) as executor:
        async with endpoint.new_session() as session:
            result_schema, row_count = await _pre_query(session, endpoint, query)
            rows_per_spawn = row_count // chunks
            limit_ranges = [
                (
                    i * rows_per_spawn,
                    ((i + 1) * rows_per_spawn) if i < chunks - 1 else row_count
                )
                for i in range(chunks)]
            tasks = [
                asyncio.ensure_future(_query_pandas(
                    session, executor, endpoint, query, result_schema, limit_range))
                for limit_range in limit_ranges]
            results = await asyncio.gather(*tasks)
            sub_dataframes = [result[0] for result in results]
            df = pd.concat(sub_dataframes)
            if stats:
                total_downloaded = sum(result[1] for result in results)
                return df, total_downloaded
            else:
                return df


def _parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int)
    parser.add_argument('--https', action='store_true')
    parser.add_argument('--username', type=str)
    parser.add_argument('--password', type=str)
    parser.add_argument('--chunks', type=int, default=1)
    parser.add_argument('query', type=str)
    return parser.parse_args()


async def main(args):
    import time
    port = args.port or (443 if args.https else 9000)
    endpoint = Endpoint(
        host=args.host,
        port=port,
        https=args.https,
        username=args.username,
        password=args.password)
    start_time = time.perf_counter()
    df, total_downloaded = await pandas_query(endpoint, args.query, args.chunks, stats=True)
    elapsed = time.perf_counter() - start_time
    print(df)
    print(f'Elapsed: {elapsed}')
    if df is not None:
        row_throughput = len(df.index) / elapsed
        print(f'Row throughput: {row_throughput:.2f} rows/sec')
    bytes_throughput = total_downloaded / 1024.0 / 1024.0 / elapsed
    print(
        f'Data throughput: {bytes_throughput:.2f} MiB/sec (of downloaded CSV data)')


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(args))
