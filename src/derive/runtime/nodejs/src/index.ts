import * as h2 from 'http2';
import * as net from 'net';
import * as stream from 'stream';
import * as querystring from 'querystring';
import * as url from 'url';
import * as JSONStreamValues from 'stream-json/streamers/StreamValues';
import {OutgoingHttpHeaders} from 'http';

export type TransformLambda = (
  doc: any,
  ctx: StateStore
) => Promise<any[] | void>;

export type BootstrapLambda = (state: StateStore) => void | Promise<void>;

export interface TransformMap {
  [id: number]: TransformLambda;
}
export interface BootstrapMap {
  [id: number]: BootstrapLambda;
}

// StateStore is a store of JSON documents, indexed under an ordered document key.
export class StateStore {
  session: h2.ClientHttp2Session;

  constructor(endpoint: string) {
    const opts: h2.ClientSessionOptions = {};

    if (endpoint.startsWith('uds:')) {
      opts.createConnection = function (authority: url.URL, _): stream.Duplex {
        return net.createConnection(authority.pathname);
      };
      // opts.protocol = 'http:'; // H2C prior-knowledge.
    }
    this.session = h2.connect(endpoint, opts);
  }

  // Set the key to the provided JSON document.
  set(key: string, doc: any, expiry?: Date): Promise<void> {
    const data = JSON.stringify({
      key: key,
      doc: doc,
    });
    const req = this.session.request(
      {
        ':method': 'PUT',
        ':path': '/docs',
        'content-type': 'application/json',
        'content-length': data.length,
      },
      {endStream: false}
    );

    req.setEncoding('utf8'); // 'data' as strings, not Buffers.
    req.write(data);
    req.end(); // Send client EOF (waitForTrailers not set).

    return new Promise((resolve, reject) => {
      req.on('response', (hdrs: h2.IncomingHttpStatusHeader, _flags) => {
        if (hdrs[':status'] !== 200) {
          reject(`unexpected response ${hdrs}`);
        }
        console.log('got setDoc headers %j', hdrs);
      });
      req.on('data', () => {
        return;
      }); // We expect no response.
      req.on('end', resolve); // Read server EOF.
      req.on('error', reject);
    });
  }

  // Returns a single document having the given key, or null.
  get(key: string): Promise<any> {
    return this._get(key, false);
  }

  // Returns an array of documents which are prefixed by the given key.
  getPrefix(key: string): Promise<any[]> {
    return this._get(key, true);
  }

  _get(key: string, prefix: boolean): Promise<any> {
    const query = querystring.stringify({key: key, prefix: prefix});
    const req = this.session.request(
      {
        ':method': 'GET',
        ':path': '/docs?' + query,
      },
      {endStream: true}
    ); // Send client EOF.

    req.setEncoding('utf8'); // 'data' as strings, not Buffers.

    return new Promise((resolve, reject) => {
      req.on('response', (hdrs: h2.IncomingHttpStatusHeader, _flags) => {
        if (hdrs[':status'] !== 200) {
          reject(`unexpected response ${hdrs}`);
        }
        console.log('got _get headers %j', hdrs);
      });

      const chunks = new Array<string>();
      req.on('data', (chunk: string) => chunks.push(chunk));
      req.on('end', () => {
        // Read server EOF.
        try {
          const parsed = JSON.parse(chunks.join(''));
          if (prefix) {
            resolve(parsed[0] || null);
          } else {
            resolve(parsed);
          }
        } catch (err) {
          reject(err);
        }
      });
      req.on('error', reject);
    });
  }
}

// LambdaTransform is a stream.Transform which reads parsed JSON documents
// and applies them to the given Lambda, with the provided StateStore.
class LambdaTransform extends stream.Transform {
  lambda: TransformLambda; // Lambda function to invoke.
  store: StateStore; // State context.
  numInput: number; // Number of read input documents.
  numOutput: number; // Number of emitted output documents.

  // Builds a LambdaTransform which invokes the Lambda with the provided StateStore.
  constructor(lambda: TransformLambda, store: StateStore) {
    super({writableObjectMode: true});
    this.lambda = lambda;
    this.store = store;
    this.numInput = 0;
    this.numOutput = 0;
  }

  _transform(chunk: any, _: string, done: stream.TransformCallback) {
    this.numInput++;

    // |lambda| may or may not be async, and may or may not throw.
    // Wrap in an async invocation to ensure async throws, rejections,
    // and non-async throws all become Promise rejections.
    const invoke = async () => {
      this._emit(await this.lambda(chunk.value, this.store));
    };
    invoke()
      .then(() => done())
      .catch(done); // Propagate as terminal pipeline error.
  }

  // Stringify each of an array of output documents, and emit as
  // content-encoding "application/json-seq".
  _emit(docs: any[] | void) {
    if (!docs) {
      return;
    }
    const parts = new Array<string>(docs.length * 3);
    let i = 0;

    for (const doc of docs) {
      // Encode per RFC 7464.
      parts[i++] = '\x1E'; // Record separater.
      parts[i++] = JSON.stringify(doc);
      parts[i++] = '\x0A'; // Line feed.
      this.numOutput++;
    }
    // Concat and send through pipeline as one chunk.
    if (parts.length !== 0) {
      this.push(parts.join(''));
    }
  }
}

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
  // and StateStore endpoint. Produces a stream of transformed output JSON documents
  // using 'application/json-seq' content encoding.
  _processTransformStream(
    req: h2.ServerHttp2Stream,
    hdrs: h2.IncomingHttpHeaders,
    _: number
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

    let store: StateStore | null = null;
    try {
      store = new StateStore(storeEndpoint);
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
    const transform = new LambdaTransform(lambda, store);
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
