import { ReadStream } from 'node:fs';
import { ReadableStream } from 'node:stream/web';
import { inject, injectable } from 'tsyringe';
import pLimit from 'p-limit';

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
    private uploadedFilesKey = 'UploadedFiles';

    constructor(@inject(StorageBackendToken) private backend: StorageBackend) {}

    public async uploadFile(
        _fileStream: ReadableStream<Uint8Array> | ReadStream,
        _fileName: string,
        _chunkSize: number,
        _parallel: number
    ): Promise<void> {
        try {
            const readableStream =
                _fileStream instanceof ReadStream
                    ? new ReadableStream({
                          start(controller) {
                              _fileStream.on('data', (chunk) =>
                                  controller.enqueue(new Uint8Array(Buffer.from(chunk)))
                              );
                              _fileStream.on('end', () => controller.close());
                              _fileStream.on('error', (error) => controller.error(error));
                          },
                      })
                    : _fileStream;

            await this.processStreamChunks(readableStream, _chunkSize, _fileName, _parallel);
        } catch (error) {
            console.error('Error processing file stream:', error);
            throw error;
        }
    }
    public async downloadFile(fileName: string, _parallel?: number): Promise<Buffer> {
        _parallel = Math.max(1, _parallel ?? 1);
        // Get file metadata
        const fileMetaData = await this.backend.get(`${fileName}`);
        if (!fileMetaData) {
            throw new Error(`File ${fileName} not found`);
        }

        const fileChunksKeys = new Map<string, string>(Object.entries(JSON.parse(fileMetaData)));
        try {
            let fileBuffer = Buffer.alloc(0);
            if (fileChunksKeys.size > 0) {
                const promises: Promise<Buffer>[] = [];

                for (const [key, value] of fileChunksKeys) {
                    const promise = new Promise<Buffer>((resolve, reject) => {
                        this.appendBufferFromFileKey(key, value, fileName)
                            .then((data) => (data ? resolve(data) : null))
                            .catch(reject);
                    });
                    promises.push(promise);
                }

                // Usage
                if (promises.length > 0) {
                    await this.batchApiCallsWithLimit(promises, 9).then((results) => {
                        fileBuffer = Buffer.concat([fileBuffer, ...results]);
                    });
                }
            }

            if (fileBuffer.length === 0) {
                throw new Error(`Downloaded file ${fileName} is empty`);
            }

            console.log(`Download completed`);
            return fileBuffer;
        } catch (error) {
            console.error('Error downloading file:', error);
            throw error;
        }
    }

    public async listUploadedFiles(): Promise<string[]> {
        return await this.backend.getListAll(this.uploadedFilesKey);
    }
    async batchApiCallsWithLimit<Buffer>(
        methodCall: Promise<Buffer>[],
        limitCount: number
    ): Promise<Buffer[]> {
        const limit = pLimit(limitCount);
        const promises = methodCall.map((call) => limit(() => call.then((res) => res)));
        return await Promise.all(promises);
    }

    private async appendBufferFromFileKey(
        key: string,
        savedChecksum: string,
        fileName: string
    ): Promise<Buffer | null> {
        const value = await this.backend.getBuffer(key);
        if (value != null) {
            const checksum = await this.backend.verifyChecksum(key);
            if (checksum === savedChecksum) {
                return value;
            } else {
                console.log(`File ${fileName}: ${key.split('.')[0]} lost its integrity`);
                return null;
            }
        }
        return null;
    }

    private async processStreamChunks(
        _fileStream: ReadableStream<Uint8Array>,
        _chunkSize: number,
        _fileName: string,
        _parallel: number
    ) {
        const reader = _fileStream.getReader();
        const calls: Promise<any>[] = [];
        _parallel = Math.max(1, _parallel);
        const state = { buffer: Buffer.alloc(0), size: 0, count: 0 };
        const filechunksDetail = new Map<string, string>();

        while (true) {
            const { value: chunk, done } = await reader.read();
            if (done) break;

            const buffer = chunk as Buffer;
            state.buffer = Buffer.concat([state.buffer, buffer]);
            state.size += buffer.length;

            if (state.size >= _chunkSize) {
                // Compute backend checksum
                const checksum = computeChecksum(state.buffer);
                const keyName = `${_fileName}-${state.count}`;

                // Verify checksum
                await this.confirmCheckSum(checksum, keyName);

                const promise = new Promise((resolve, reject) => {
                    this.backend
                        .set(keyName, state.buffer)
                        .then(() => resolve('done'))
                        .catch(reject);
                });
                calls.push(promise);
                filechunksDetail.set(keyName, checksum);

                // Reset for next chunk
                state.buffer = Buffer.alloc(0);
                state.size = 0;
                state.count++;
            }

            if (calls.length == _parallel) {
                const pendingCalls = [...calls];
                calls.length = 0;
                await Promise.all(pendingCalls);
            }
        }

        // Save any remaining buffer
        if (state.buffer.length > 0) {
            const keyName = `${_fileName}-${state.count}`;
            const checksum = computeChecksum(state.buffer);
            this.backend.set(keyName, state.buffer);
            filechunksDetail.set(keyName, checksum);

            // Verify checksum
            await this.confirmCheckSum(checksum, keyName);
        }

        // Save File metadata and Uploaded file names
        await Promise.all([
            this.backend.set(_fileName, JSON.stringify(Object.fromEntries(filechunksDetail))),
            this.backend.rPush(this.uploadedFilesKey, _fileName),
        ]);
        await new Promise((resolve) => setTimeout(resolve, 0));
        console.log(`Upload completed`);
    }

    public async confirmCheckSum(savedChecksum: string, keyName: string) {
        // Verify checksum
        if ((await this.backend.verifyChecksum(keyName)) !== savedChecksum) {
            // an exception can be throw
            console.log('Invalid checksum');
        }
    }
}
