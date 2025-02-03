import { ReadStream } from 'node:fs';
import type { ReadableStream } from 'node:stream/web';
import { inject, injectable } from 'tsyringe';

import { StorageBackend } from './storage-backend/storage-backend';
import { StorageBackendToken } from '../ioc-tokens';
import { computeChecksum } from './helpers';

export interface FileStorage {
    /**
     * Upload file should handle a web standards ReadableStream and put the file into the storage backend.
     *
     * Chunk size is a parameter that should be used to determine the size of "chunks" of the file to store in
     * the storagebackend.
     *
     * Note: parallel is a "bonus" feature that should control the number of parallel requests made to the
     * storage backend
     */
    uploadFile(
        fileStream: ReadableStream<Uint8Array> | ReadStream,
        fileName: string,
        chunkSize: number,
        _parallel?: number
    ): Promise<void>;

    /**
     * Download file should return the full file that was uploaded by the given `fileName` as a Buffer.
     *
     * Note: parallel is a "bonus" feature that should control the number of parallel requests made to the
     * storage backend
     */
    downloadFile(fileName: string, _parallel?: number): Promise<Buffer>;

    /**
     * List uploaded files is primarily used in unit tests and would be a method for debugging. Therefore, it
     * does not need to be highly performant (for example might use `SCAN` or `KEYS` with a redis implementation).
     */
    listUploadedFiles(): Promise<string[]>;
}

@injectable()
export class AppFileStorage implements FileStorage {
    uploadedfileKey: string[] = [];
    genericStrFileClassifier: string = '%str%';
    constructor(@inject(StorageBackendToken) private backend: StorageBackend) {
        console.log('TODO: implement AppFileStorage', this.backend);
    }

    public async uploadFile(
        _fileStream: ReadableStream<Uint8Array> | ReadStream,
        _fileName: string,
        _chunkSize: number,
        _parallel: number
    ): Promise<void> {
        // Add file content to buffer
        // Todo: check for duplicate files and decide what to do
        try {
            if (_fileStream instanceof ReadStream) {
                await this.processReadStreamData(_fileStream, _chunkSize, _fileName, _parallel);
            } else {
                await this.processStreamChunks(_fileStream, _chunkSize, _fileName, _parallel);
            }
        } catch (error) {
            console.error('Error processing file stream:', error);
            throw error;
        }

        this.uploadedfileKey.push(_fileName);
    }

    public async downloadFile(fileName: string, _parallel: number): Promise<Buffer> {
        _parallel = _parallel < 1 ? 1 : _parallel;

        let fileKeys = await this.backend.keys(`${fileName}*`);
        let fileBuffer = Buffer.alloc(0);
        if (fileKeys.length > 0) {
            fileKeys.sort();
            let isStr = fileKeys[0].includes(`${this.genericStrFileClassifier}}`);
            console.log('not null');
            console.log('Get chunks of data from storage');

            for (let key of fileKeys) {
                let bufferValue = await this.appendBufferFromFileKey(key, isStr, fileName);
                if (bufferValue != null) {
                    fileBuffer = Buffer.concat([fileBuffer, bufferValue]);
                }
            }
            return fileBuffer;
        }
        throw new Error(`File ${fileName} not found`);
    }

    public async listUploadedFiles(): Promise<string[]> {
        return this.uploadedfileKey;
    }

    private async appendBufferFromFileKey(
        key: string,
        isStr: boolean,
        fileName: string
    ): Promise<Buffer | null> {
        if (isStr) {
            const value = await this.backend.get(key);
            if (value != null) {
                return Buffer.from(value);
            }
        }

        const value = await this.backend.getBuffer(key);
        if (value != null) {
            let checksum = await this.backend.verifyChecksum(key);
            if (key.includes(checksum)) {
                return value;
            } else {
                throw new Error(`File ${fileName} lost its integrity`);
            }
        }
        return null;
    }

    private async processReadStreamData(
        _fileStream: ReadStream,
        _chunkSize: number,
        _fileName: string,
        _parallel: number
    ) {
        let counter = 0;
        let state = { buffer: Buffer.alloc(0), size: 0, text: '' };
        let calls: Promise<any>[] = [];
        _parallel = _parallel < 1 ? 1 : _parallel;

        _fileStream.on('data', async (chunk) => {
            switch (typeof chunk) {
                case 'string':
                    const byteLength = Buffer.byteLength(chunk, 'utf8');
                    state.size += byteLength;
                    state.text += chunk;
                    if (state.size === _chunkSize) {
                        const promise = new Promise((resolve, reject) => {
                            this.backend
                                .set(`${_fileName}-${counter}-str`, state.text)
                                .then(() => resolve('done'))
                                .catch(reject);
                        });
                        calls.push(promise);
                    }
                    break;
                case 'object':
                    if (Buffer.isBuffer(chunk)) {
                        state.size += chunk.length;
                        state.buffer = Buffer.concat([state.buffer, chunk]);
                        if (state.size === _chunkSize) {
                            let checksum = computeChecksum(chunk);
                            const promise = new Promise((resolve, reject) => {
                                this.backend
                                    .set(`${_fileName}-${counter}-b${checksum}`, state.buffer)
                                    .then(() => resolve('done'))
                                    .catch(reject);
                            });
                            calls.push(promise);
                        }
                    }
                    break;
            }

            if (calls.length == _parallel) {
                let pendingCalls = [...calls];
                calls.length = 0;
                await Promise.all(pendingCalls);
            }

            if (state.size == _chunkSize) {
                state = { buffer: Buffer.alloc(0), size: 0, text: '' };
                counter += 1;
            }
        });
        _fileStream.on('end', async () => {
            if (calls.length > 0) {
                let pendingCalls = [...calls];
                calls.length = 0;
                await Promise.all(pendingCalls);
            }
            _fileStream.close();
        });
    }

    async processStreamChunks(
        _fileStream: ReadableStream<Uint8Array>,
        _chunkSize: number,
        _fileName: string,
        _parallel: number
    ) {
        const reader = _fileStream.getReader();
        const calls: Promise<any>[] = [];
        _parallel = Math.max(1, _parallel);
        const state = { buffer: Buffer.alloc(0), size: 0, count: 0 };
        while (true) {
            const { value: chunk, done } = await reader.read();
            if (done) break;

            console.log(`Calling stream at index ${state.count}`);
            const buffer = chunk as Buffer;
            state.buffer = Buffer.concat([state.buffer, buffer]);
            state.size += buffer.length;

            if (state.size >= _chunkSize) {
                let checksum = computeChecksum(chunk);
                const promise = new Promise((resolve, reject) => {
                    this.backend
                        .set(`${_fileName}-${state.count}-b${checksum}`, state.buffer)
                        .then(() => resolve('done'))
                        .catch(reject);
                });
                calls.push(promise);
                // Reset for next chunk
                state.buffer = Buffer.alloc(0);
                state.size = 0;
                state.count++;
            }

            if (calls.length == _parallel) {
                console.log(`Saving to storage`);
                let pendingCalls = [...calls];
                calls.length = 0;
                await Promise.all(pendingCalls);
            }
        }

        // Save any remaining buffer
        if (state.buffer.length > 0) {
            let checksum = computeChecksum(state.buffer);
            this.backend.set(`${_fileName}-${state.count}-b${checksum}`, state.buffer);
        }

        console.log(`Upload completed`);
    }
}
