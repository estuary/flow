import * as h2 from 'http2';

import { routes, Document, Lambda } from './routes';

// Server serves transform lambda invocation requests, streaming source
// collection documents, processing each via the designated transform, and
// streaming resulting derived documents in response.
export class Server {
    listenPath: string;

    constructor(listenPath: string) {
        this.listenPath = listenPath;
    }

    start(): void {
        const server = h2.createServer();
        server.on('stream', this._processStream.bind(this));
        server.on('error', console.error);
        server.listen({ path: this.listenPath });
    }

    _processStream(req: h2.ServerHttp2Stream, headers: h2.IncomingHttpHeaders): void {
        const malformed = (msg: string): void => {
            req.respond({
                ':status': 400,
                'content-type': 'text/plain',
            });
            req.end(msg + '\n'); // Send message & EOF.
        };

        const path = headers[':path'];
        if (path === undefined) {
            return malformed('expected :path header');
        }

        const lambda: Lambda | undefined = routes[path];
        if (lambda === undefined) {
            return malformed(`route ${path} is not defined`);
        }

        // Gather and join all data buffers.
        const chunks: string[] = [];

        req.on('data', (chunk: string) => {
            chunks.push(chunk);
        });

        req.on('end', () => {
            // Join input chunks and parse into an array of invocation rows.
            const [sources, registers] = JSON.parse(chunks.join('')) as [Document[], Document[][] | undefined];

            // Map each row into a future which will return Document[].
            const futures = sources.map(async (source, index) => {
                const previous = registers ? registers[index][0] : undefined;
                const register = registers ? registers[index][1] : undefined;

                return lambda(source, register || previous, previous);
            });

            // When all rows resolve, return the Document[][] to the caller.
            Promise.all(futures)
                .then((rows: Document[][]) => {
                    const body = Buffer.from(JSON.stringify(rows), 'utf8');

                    req.respond({
                        ':status': 200,
                        'content-type': 'application/json',
                        'content-length': body.length,
                    });
                    req.end(body);
                })
                .catch((err: Error) => {
                    // Send |err| to peer, and log to console.
                    req.respond({
                        ':status': 400,
                        'content-type': 'text/plain',
                    });
                    req.end(`${err.name}: (${err.message})\n`);
                    console.error(err);
                });
        });

        req.on('error', (err) => {
            console.error(err);
        });
    }
}
