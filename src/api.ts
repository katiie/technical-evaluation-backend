import { HttpBindings } from '@hono/node-server';
import { Hono } from 'hono';
import { HTTPException } from 'hono/http-exception';
import { FileStorage } from './file-storage/file-storage';
import { FileStorageToken } from './ioc-tokens';
import { getAppContainer } from './app';

type Bindings = HttpBindings;
const honoApp = new Hono<{ Bindings: Bindings }>();

const container = getAppContainer();
const fileStorage = container.resolve<FileStorage>(FileStorageToken);

export const app = honoApp
    .post('/upload/:fileName', async (c) => {
        console.log('Handling request');
        const uploadStream = c.req.raw.body;

        if (!uploadStream) {
            throw new HTTPException(422, { message: 'Missing upload body' });
        }

        const fileName = c.req.param('fileName');
        console.log('Uploading file...', fileName);

        await fileStorage.uploadFile(uploadStream, fileName, 10_000_000, 4);

        return c.json({ ok: true });
    })
    .get('/download/:fileName', async (c) => {
        const fileName = c.req.param('fileName');

        const downloadContents = await fileStorage.downloadFile(fileName, 4);

        c.header('Content-Type', 'application/octet-stream');

        return c.body(downloadContents);
    });
