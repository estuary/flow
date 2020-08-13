import {OutgoingHttpHeaders} from 'http';
import * as h2 from 'http2';
import * as JSONStreamValues from 'stream-json/streamers/StreamValues';

import {LambdaTransformer} from './lambda_transform';
import {BootstrapLambda, BootstrapMap, TransformMap} from './types';

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
      transform: (one: Document, two?: Document, three?: Document) =>
          Promise<Document[]>,
      ): void {
    req.respond(
        {
          ':status': 200,
          'content-type': 'application/json-seq',
        },
        {endStream: false, waitForTrailers: true},
    );

    // We'll send trailer headers at the end of the response.
    const trailers: OutgoingHttpHeaders = {};

    // Stand up a processing pipeline which:
    // 1) parses input byte-stream into JSON documents
    // 2) invokes |lambda| with each document, using |store|.
    // 3) marshals emitted documents as stringified sequential JSON.
    // 4) pipes back to the request's response stream.
    const parse = JSONStreamValues.withParser();
    const transformer = new LambdaTransformer(transform);
    req.pipe(parse).pipe(transformer).pipe(req);

    // 'wantTrailers' is invoked (only) on clean |req| write stream end.
    // pipe() doesn't end streams or forward if an error occurs.
    req.on('wantTrailers', () => {
      if (!trailers['error']) {
        trailers['success'] = 'true';
      }
      req.sendTrailers(trailers);
      parse.destroy();
      transformer.destroy();
    });

    // Errors in intermediate pipeline steps abort the |req| stream with an
    // error.
    const onErr = (err: Error): void => {
      console.error(err);
      trailers['error'] = `${err.name} (${err.message})`;
      req.end();  // Trigger sending of trailers.
    };
    parse.on('error', onErr);
    transformer.on('error', onErr);
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
      req.end(msg);  // Send message & EOF.
    };

    const path = hdrs[':path'];
    if (path === undefined) {
      return malformed('expected :path header');
    }

    const pathBootstrap = /^\/bootstrap\/(\d+)$/.exec(path);
    if (pathBootstrap) {
      const bootstraps = this.bootstraps[parseInt(pathBootstrap[1], 10)];
      if (bootstraps === undefined) {
        return malformed(`bootstrap ${path} is not defined`);
      }
      this._processBootstrap(req, hdrs, bootstraps);
      return;
    }

    const pathUpdate = /^\/update\/(\d+)$/.exec(path);
    if (pathUpdate) {
      const transform = this.transforms[parseInt(pathUpdate[1], 10)].update;
      if (transform === undefined) {
        return malformed(`update ${path} is not defined`);
      }
      this._process(req, hdrs, transform);
      return;
    }

    const pathPublish = /^\/update\/(\d+)$/.exec(path);
    if (pathPublish) {
      const transform = this.transforms[parseInt(pathPublish[1], 10)].update;
      if (transform === undefined) {
        return malformed(`publish ${path} is not defined`);
      }
      this._process(req, hdrs, transform);
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
