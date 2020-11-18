import * as h2 from 'http2';
import {BootstrapLambda, BootstrapMap, Document, PublishLambda, TransformMap, UpdateLambda} from './types';

// Server serves transform lambda invocation requests, streaming source
// collection documents, processing each via the designated transform, and
// streaming resulting derived documents in response.
class Server {
  listenPath: string;
  bootstraps: BootstrapMap;
  transforms: TransformMap;

  constructor(
      listenPath: string, bootstraps: BootstrapMap, transforms: TransformMap) {
    this.listenPath = listenPath;
    this.bootstraps = bootstraps;
    this.transforms = transforms;
  }

  start(): void {
    const server = h2.createServer();
    server.on('stream', this._processStream.bind(this));
    server.on('error', console.error);
    server.listen({path: this.listenPath});
  }

  // Invokes the target bootstrap lambdas, and responds with 204 or a failure
  // message.
  _processBootstrap(
      req: h2.ServerHttp2Stream,
      _: h2.IncomingHttpHeaders,
      bootstraps: BootstrapLambda[],
      ): void {
    // Invoke bootstraps sequentially, in declaration order.
    let p = Promise.resolve();
    for (const bs of bootstraps) {
      p = p.then(async () => await bs());
    }

    p.then(
        () => {
          req.respond({':status': 204}, {endStream: true});
        },
        (err: Error) => {
          req.respond({
            ':status': 400,
            'content-type': 'text/plain',
          });
          req.end(`${err.name}: (${err.message})`);  // Send message & EOF.
        },
    );
  }

  // Invokes the designated transform lambda with the request's input document
  // stream, responding with transformed documents.
  _process(
      req: h2.ServerHttp2Stream,
      _: h2.IncomingHttpHeaders,
      update?: UpdateLambda,
      publish?: PublishLambda,
      ): void {
    // Gather and join all data buffers.
    const chunks: string[] = [];

    req.on('data', (chunk: string) => {
      chunks.push(chunk);
    });

    req.on('end', () => {
      // Join input chunks and parse into an array of invocation rows.
      const rows: Document[][] = JSON.parse(chunks.join(''));

      // Map each row into a future which will return Document[].
      const futures = rows.map(async (row) => {
        const source = row[0];

        if (update !== undefined) {
          return update(source);
        }

        const previous = row[1];
        const register = row[2];

        if (publish !== undefined) {
          return publish(source, previous, register || previous);
        }

        throw 'not reached';
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
          .catch((err) => {
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

  // Processes request streams:
  // - /bootstrap/(\d+) invokes bootstraps of the given derivationId.
  // - /transform/(\d+) transforms a stream of input JSON documents through
  //   the identified transform lambda, streaming back transformed documents.
  _processStream(req: h2.ServerHttp2Stream, hdrs: h2.IncomingHttpHeaders):
      void {
    const malformed = (msg: string): void => {
      req.respond({
        ':status': 400,
        'content-type': 'text/plain',
      });
      req.end(msg + '\n');  // Send message & EOF.
    };

    const path = hdrs[':path'];
    if (path === undefined) {
      return malformed('expected :path header');
    }

    const pathBootstrap = /^\/bootstrap\/(\d+)$/.exec(path);
    if (pathBootstrap) {
      const bootstraps = this.bootstraps[parseInt(pathBootstrap[1], 10)];
      if (bootstraps !== undefined) {
        this._processBootstrap(req, hdrs, bootstraps);
      } else {
        req.respond({':status': 204}, {endStream: true});
      }
      return;
    }

    const pathUpdate = /^\/update\/(\d+)$/.exec(path);
    if (pathUpdate) {
      const update = this.transforms[parseInt(pathUpdate[1], 10)]?.update;
      if (update === undefined) {
        return malformed(`update ${path} is not defined`);
      }
      this._process(req, hdrs, update, undefined);
      return;
    }

    const pathPublish = /^\/publish\/(\d+)$/.exec(path);
    if (pathPublish) {
      const publish = this.transforms[parseInt(pathPublish[1], 10)]?.publish;
      if (publish === undefined) {
        return malformed(`publish ${path} is not defined`);
      }
      this._process(req, hdrs, undefined, publish);
      return;
    }

    malformed(`unknown route ${path}`);
  }
}

export function main(bootstraps: BootstrapMap, transforms: TransformMap): void {
  if (!process.env.SOCKET_PATH) {
    throw new Error('SOCKET_PATH environment variable is required');
  }
  new Server(process.env.SOCKET_PATH, bootstraps, transforms).start();

  console.log('READY');
}
