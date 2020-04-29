import * as h2 from 'http2';
import {OutgoingHttpHeaders} from 'http';
import * as JSONStreamValues from 'stream-json/streamers/StreamValues';
import {Store} from './store';
import {LambdaTransformer} from './lambda_transform';
import {BootstrapMap, TransformMap} from './types';

// Server serves transform lambda invocation requests, streaming source collection
// documents, processing each via the designated transform, and streaming resulting
// derived documents in response.
class Server {
  listenPath: string;
  bootstraps: BootstrapMap;
  transforms: TransformMap;

  constructor(
    listenPath: string,
    bootstraps: BootstrapMap,
    transforms: TransformMap
  ) {
    this.listenPath = listenPath;
    this.bootstraps = bootstraps;
    this.transforms = transforms;
  }

  start() {
    const server = h2.createServer();
    server.on('stream', this._processTransformStream);
    server.on('error', console.error);
    server.listen({path: this.listenPath});
  }

  // Consumes a stream of input JSON documents to transform under a named Lambda
  // and Store endpoint. Produces a stream of transformed output JSON documents
  // using 'application/json-seq' content encoding.
  _processTransformStream(
    req: h2.ServerHttp2Stream,
    hdrs: h2.IncomingHttpHeaders
  ): void {
    const malformed = (msg: string) => {
      req.respond({
        ':status': 400,
        'content-type': 'text/plain',
      });
      req.end(msg); // Send message & EOF.
    };

    const path = hdrs[':path'];
    if (!path) {
      return malformed('expected :path header');
    }

    const lambdaId = parseInt(path.slice(1), 10);
    const lambda = this.transforms[lambdaId];
    if (!lambda) {
      return malformed(`lambda id ${lambdaId} is not defined`);
    }

    const stateStore = hdrs['state-store'];
    if (!stateStore) {
      return malformed("expected 'state-store' header'");
    }
    const storeEndpoint = stateStore[0];

    let store: Store | null = null;
    try {
      store = new Store(storeEndpoint);
    } catch (err) {
      return malformed(err);
    }

    req.respond(
      {
        ':status': 200,
        'content-type': 'application/json-seq',
      },
      {endStream: false, waitForTrailers: true}
    );

    // We'll send trailer headers at the end of the response.
    const trailers: OutgoingHttpHeaders = {};

    // Stand up a processing pipeline which:
    // 1) parses input byte-stream into JSON documents
    // 2) invokes |lambda| with each document, using |store|.
    // 3) marshals emitted documents as stringified sequential JSON.
    // 4) pipes back to the request's response stream.
    const parse = JSONStreamValues.withParser();
    const transform = new LambdaTransformer(lambda, store);
    req.pipe(parse).pipe(transform).pipe(req);

    // 'wantTrailers' is invoked (only) on clean |req| write stream end.
    // pipe() doesn't end streams or forward if an error occurs.
    req.on('wantTrailers', () => {
      trailers['num-input'] = transform.numInput;
      trailers['num-output'] = transform.numOutput;

      if (!trailers['error']) {
        trailers['success'] = 'true';
      }
      req.sendTrailers(trailers);
      parse.destroy();
      transform.destroy();
    });

    // Errors in intermediate pipeline steps abort the |req| stream with an error.
    const onErr = (err: Error) => {
      console.error(err);
      trailers['error'] = `${err.name} (${err.message})`;
      req.end(); // Trigger sending of trailers.
    };
    parse.on('error', onErr);
    transform.on('error', onErr);
  }
}

export function main(bootstraps: BootstrapMap, transforms: TransformMap) {
  if (!process.env.SOCKET_PATH) {
    throw new Error('SOCKET_PATH environment variable is required');
  }
  new Server(process.env.SOCKET_PATH, bootstraps, transforms).start();

  console.log('READY');
}
