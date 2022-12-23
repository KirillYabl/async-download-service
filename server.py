import asyncio
import os

from aiohttp import web
from aiohttp.web_request import StreamResponse
import aiofiles

DATA_CHUNK = 1024 * 100


async def archive(request: web.Request) -> StreamResponse:
    response = web.StreamResponse()
    archive_hash = request.match_info.get('archive_hash', '')

    cwd = os.path.join('test_photos', archive_hash)
    if not os.path.exists(cwd):
        raise web.HTTPNotFound(text='Архив не существует или был удален')

    proc = await asyncio.create_subprocess_exec(
        'zip', '-r', '-qq', '-', '.', stdout=asyncio.subprocess.PIPE, cwd=cwd)

    response.headers['Content-Type'] = 'multipart/form-data'
    response.headers['Content-Disposition'] = 'attachment; filename="photos.zip"'

    await response.prepare(request)

    while not proc.stdout.at_eof():
        data = await proc.stdout.read(DATA_CHUNK)
        await response.write(data)

    return response


async def handle_index_page(request: web.Request) -> web.Response:
    async with aiofiles.open('index.html', mode='r', encoding='UTF8') as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type='text/html')


if __name__ == '__main__':
    app = web.Application()
    app.add_routes([
        web.get('/', handle_index_page),
        web.get('/archive/{archive_hash}/', archive),
    ])
    web.run_app(app)
