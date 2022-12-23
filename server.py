import argparse
import asyncio
from dataclasses import dataclass
import logging
import os
from functools import partial
from pathlib import Path

from aiohttp import web
from aiohttp.web_request import StreamResponse
import aiofiles

logger = logging.getLogger(__name__)

DATA_CHUNK = 1024 * 100  # 100 kb


@dataclass
class Options:
    logging: bool
    delay: bool
    path: Path


async def archive(request: web.Request, options: Options) -> StreamResponse:
    response = web.StreamResponse()
    archive_hash = request.match_info.get('archive_hash', '')

    cwd = options.path / archive_hash
    if not os.path.exists(cwd):
        raise web.HTTPNotFound(text='Архив не существует или был удален')

    proc = await asyncio.create_subprocess_exec(
        'zip', '-r', '-qq', '-', '.', stdout=asyncio.subprocess.PIPE, cwd=cwd)

    response.headers['Content-Type'] = 'multipart/form-data'
    response.headers['Content-Disposition'] = 'attachment; filename="photos.zip"'

    await response.prepare(request)

    chunk_number = 1
    try:
        while not proc.stdout.at_eof():
            data = await proc.stdout.read(n=DATA_CHUNK)
            logger.debug(f'Sending archive chunk {chunk_number} ...')
            await response.write(data)
            if options.delay:
                await asyncio.sleep(3)
            chunk_number += 1
    except ConnectionError:
        logging.debug('Download was interrupted')
    finally:
        logging.debug('Killing...')
        try:
            proc.kill()
            await proc.communicate()
            logging.debug('Process was killed')
        except ProcessLookupError:
            logging.debug('Process has killed already')

    return response


async def handle_index_page(request: web.Request) -> web.Response:
    async with aiofiles.open('index.html', mode='r', encoding='UTF8') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(
        prog='Async download server',
        description='It is microservice for async downloading archives',
    )

    parser.add_argument('-l', '--logging', action='store_true', help='turn on logging')
    parser.add_argument('-d', '--delay', action='store_true', help='turn on delay')
    parser.add_argument('-p', '--path', type=Path, required=True, help='path to catalog with folders')

    args = parser.parse_args()

    options = Options(**args.__dict__)

    if not options.logging:
        logging.disable()

    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', partial(archive, options=options)),
    ])
    web.run_app(app)
