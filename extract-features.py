#!/usr/bin/env python3
import asyncio
import hashlib
import json
import os
import typing

import aiofile
import aiohttp
import aiologger
import click
import pandas as pd
import yarl

_TIKA_CTAKES_SCHEME = os.getenv("TIKA_CTAKES_SCHEME", "http")
_TIKA_CTAKES_HOST = os.getenv("TIKA_CTAKES_HOST", "localhost")
_TIKA_CTAKES_PORT = os.getenv("TIKA_CTAKES_PORT", "8888")
_TIKA_CTAKES_API = os.getenv(
    "TIKA_CTAKES_API",
    yarl.URL.build(
        scheme=_TIKA_CTAKES_SCHEME, host=_TIKA_CTAKES_HOST, port=_TIKA_CTAKES_PORT
    ),
)
_TIKA_GEO_SCHEME = os.getenv("TIKA_GEO_SCHEME", "http")
_TIKA_GEO_HOST = os.getenv("TIKA_GEO_HOST", "localhost")
_TIKA_GEO_PORT = os.getenv("TIKA_GEO_PORT", "8888")
_TIKA_GEO_API = os.getenv(
    "TIKA_GEO_API",
    yarl.URL.build(scheme=_TIKA_GEO_SCHEME, host=_TIKA_GEO_HOST, port=_TIKA_GEO_PORT),
)

_MIMETYPES = {_TIKA_GEO_API: "application/geotopic", _TIKA_CTAKES_API: "text/plain"}

logger = aiologger.Logger.with_default_handlers()


def _init_parser(
    http_session: aiohttp.ClientSession, api_uri: str,
) -> typing.Callable[[bytes], typing.Awaitable[typing.Tuple[int, str]]]:
    async def _fn(buff: bytes) -> typing.Tuple[int, str]:
        """
        Send request to Tika.
        """
        async with http_session.put(
            yarl.URL(api_uri).with_path("/rmeta"),
            data=buff,
            headers={"Content-Type": _MIMETYPES[api_uri]},
        ) as resp:
            resp_txt = await resp.text()
            return resp.status, resp_txt

    return _fn


async def _worker(
    queue: asyncio.Queue,
    tika_fn: typing.Callable[[bytes], typing.Awaitable[typing.Tuple[int, str]]],
    output_dir: str,
    write_filter: typing.Callable[[str], bool] = lambda x: True,
):
    while True:
        row: dict = await queue.get()
        try:
            resp_code, resp_txt = await tika_fn(row["abstract"].encode("utf8"))
            if resp_code == 200:
                if write_filter(resp_txt):
                    await write_json(output_dir, resp_txt, row)
            else:
                logger.warn(
                    f"non successful resp_code {resp_code} -> {resp_txt}, placing back in queue"
                )
                await queue.put(row)
        except Exception as ex:
            logger.exception(ex)
        finally:
            queue.task_done()


async def write_json(out_dir: str, tika_json: str, row: dict):
    """
    Persist tika output to disk.
    """
    doi = row["doi"]
    clean_doi = str(doi)
    if clean_doi == "nan":
        clean_doi = (
            clean_doi
            + "_"
            + str(hashlib.sha224(row["abstract"].encode("utf-8")).hexdigest())
        )
    else:
        clean_doi = str(doi).replace("/", "_")
    out_file = os.path.join(out_dir, f"{clean_doi}.json")
    async with aiofile.AIOFile(out_file, "wt") as jw:
        logger.info("Writing JSON to:  [" + out_file + "]")
        await jw.write(tika_json)


def _geo_write_filter(resp_txt: str) -> bool:
    resp = json.loads(resp_txt)
    return "Geographic_NAME" in resp[0]


async def _doit(input_file: str, output_dir: str, num_workers: int):
    ctakes_output_dir = os.path.join(output_dir, "ctakes-json")
    geo_output_dir = os.path.join(output_dir, "geo-json")

    # Initialize the shared worker dependencies
    http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600))
    ctakes_queue: asyncio.Queue = asyncio.Queue(maxsize=num_workers * 2)
    geo_queue: asyncio.Queue = asyncio.Queue(maxsize=num_workers * 2)
    os.makedirs(ctakes_output_dir)
    os.makedirs(geo_output_dir)

    worker_tasks: typing.List[asyncio.Task] = [
        asyncio.create_task(
            _worker(
                ctakes_queue,
                _init_parser(http_session, _TIKA_CTAKES_API),
                ctakes_output_dir,
            )
        )
        for _ in range(num_workers)
    ] + [
        asyncio.create_task(
            _worker(
                geo_queue,
                _init_parser(http_session, _TIKA_GEO_API),
                geo_output_dir,
                _geo_write_filter,
            )
        )
        for _ in range(num_workers)
    ]

    covid_df = pd.read_csv(input_file)

    for (i, row) in covid_df.iterrows():
        row_dict = row.to_dict()
        if isinstance(row_dict["abstract"], str) and row_dict["abstract"]:
            await geo_queue.put(row_dict)
            await ctakes_queue.put(row_dict)

    # clean up
    await ctakes_queue.join()
    await geo_queue.join()
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    await http_session.close()
    await logger.shutdown()


@click.command()
@click.option(
    "--input-file",
    required=True,
    prompt=False,
    type=click.STRING,
    help="Input metadata.csv file",
)
@click.option(
    "--output-dir",
    required=True,
    prompt=False,
    type=click.STRING,
    help="Output directory",
)
@click.option(
    "--num-workers",
    prompt=False,
    type=click.INT,
    help="Concurrent operations/requests",
    default=4,
)
def main(input_file: str, output_dir: str, num_workers: int):
    try:
        asyncio.get_event_loop().run_until_complete(
            _doit(input_file, output_dir, num_workers)
        )
    except:
        print("Received exit, exiting")


if __name__ == "__main__":
    main()
