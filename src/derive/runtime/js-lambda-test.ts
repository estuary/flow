import * as h2 from "http2";
import * as net from 'net';
import * as JSONStreamValues from 'stream-json/streamers/StreamValues'
import * as stream from 'stream';
import * as querystring from 'querystring';
import * as url from 'url';

async function getFive(): Promise<number> { return 5; }
async function getTen(): Promise<number> { return 10; }

type Lambda = (doc: any, ctx: StateStore) => any[] | Promise<any[]>;
interface LambdaMap { [id: number]: Lambda; }

// TODO: generate this map from javascript blocks in catalog.
// Also allow for auxillary TS/JS sources which are "compiled" in.
let lambdas : LambdaMap = {
    "my_lambda": (doc, ctx) => [{...doc, val: 1}, {...doc, val: 2}],
    "my_lambda2": async (doc, ctx) => [
        {...doc, val: await getFive()},
        {...doc, val: await getTen()},
    ],
    "do_set": async (doc, store) => {
        await store.set(doc.key, doc);
        return [];
    },
    "do_get": async (doc, store) => [ await store.get(doc.key) ],
    "do_prefix": async (doc, store) => await store.getPrefix(doc.key),
};

// StateStore is a store of JSON documents, indexed under an ordered document key.
class StateStore {
    session: h2.ClientHttp2Session;

    constructor(endpoint: string) {
        let opts : h2.ClientSessionOptions = {};

        if (endpoint.startsWith("uds:")) {
            opts.createConnection = function (authority: url.URL, _): stream.Duplex {
                return net.createConnection(authority.pathname)
            }
            opts.protocol = "http:"; // H2C prior-knowledge.
        }
        this.session = h2.connect(endpoint, opts)
    }

    // Set the key to the provided JSON document.
    set(key: string, doc: any): Promise<void> {
        const data = JSON.stringify({
            key: key,
            doc: doc,
        });
        let req = this.session.request({
            ':method':  'PUT',
            ':path': '/docs',
            'content-type': 'application/json',
            'content-length': data.length,
        }, {endStream: false});

        req.setEncoding('utf8'); // 'data' as strings, not Buffers.
        req.write(data);
        req.end(); // Send client EOF (waitForTrailers not set).

        return new Promise((resolve, reject) => {
            req.on('response', (hdrs: h2.IncomingHttpStatusHeader, _flags) => {
                if (hdrs[':status'] != 200) {
                    reject(`unexpected response ${hdrs}`);
                }
                console.log('got setDoc headers %j', hdrs);
            });
            req.on('data', () => {}); // We expect no response.
            req.on('end', resolve);   // Read server EOF.
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
        const query = querystring.encode({key: key, prefix: prefix ? true : false});
        let req = this.session.request({
            ':method': 'GET',
            ':path': '/docs?' + query,
        }, {endStream: true}) // Send client EOF.

        req.setEncoding('utf8'); // 'data' as strings, not Buffers.

        return new Promise((resolve, reject) => {
            req.on('response', (hdrs: h2.IncomingHttpStatusHeader, _flags) => {
                if (hdrs[':status'] != 200) {
                    reject(`unexpected response ${hdrs}`);
                }
                console.log("got _get headers %j", hdrs);
            });

            let chunks = new Array<string>();
            req.on('data', (chunk: string) => chunks.push(chunk));
            req.on('end', () => { // Read server EOF.
                try {
                    let parsed = JSON.parse(chunks.join(''));
                    if (prefix) {
                        resolve(parsed[0] || null);
                    } else {
                        resolve(parsed)
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
    lambda: Lambda;     // Lambda function to invoke.
    store: StateStore;    // State context.
    num_input: number;  // Number of read input documents.
    num_output: number; // Number of emitted output documents.

    // Builds a LambdaTransform which invokes the Lambda with the provided StateStore.
    constructor(lambda: Lambda, store: StateStore) {
        super({ writableObjectMode: true });
        this.lambda = lambda;
        this.store = store;
        this.num_input = 0;
        this.num_output = 0;
    }

    _transform(chunk: any, _, done: stream.TransformCallback) {
        this.num_input++;

        // |lambda| may or may not be async, and may or may not throw.
        // Wrap in an async invocation to ensure async throws, rejections,
        // and non-async throws all become Promise rejections.
        let invoke = async () => {
            this._emit(await this.lambda(chunk.value, this.store));
        };
        invoke()
            .then(() => done())
            .catch(done); // Propagate as terminal pipeline error.
    }

    // Stringify each of an array of output documents, and emit as
    // content-encoding "application/json-seq".
    _emit(docs: any[]) {
        let parts = new Array<string>(docs.length * 3);
        let i = 0;

        for (const doc of docs) {
            // Encode per RFC 7464.
            parts[i++] = '\x1E'; // Record separater.
            parts[i++] = JSON.stringify(doc);
            parts[i++] = '\x0A'; // Line feed.
            this.num_output++;
        }
        // Concat and send through pipeline as one chunk.
        if (parts.length != 0) {
            this.push(parts.join(''));
        }
    }
};


// Consumes a stream of input JSON documents to transform under a named Lambda
// and StateStore endpoint. Produces a stream of transformed output JSON documents
// using 'application/json-seq' content encoding.
function _processTransformStream(req: h2.ServerHttp2Stream, hdrs: h2.IncomingHttpHeaders, _flags): void {
    let malformed = (msg: string) => {
        req.respond({
            ':status': 400,
            'content-type': 'text/plain',
        });
        req.end(msg);  // Send message & EOF.
    };

    const lambda_name = hdrs[":path"].slice(1);
    const lambda = lambdas[lambda_name];
    if (!lambda) {
        return malformed(`lambda ${lambda_name} is not defined`);
    }

    const store_endpoint = hdrs["state-store"][0];
    if (!store_endpoint) {
        return malformed(`expected 'state-store' header'`);
    }

    let store: StateStore = null;
    try {
        store = new StateStore(store_endpoint);
    } catch (err) {
        return malformed(err);
    }

    req.respond({
        ':status': 200,
        'content-type': 'application/json-seq'
    }, {endStream: false, waitForTrailers: true});

    // We'll send trailer headers at the end of the response.
    let trailers = {};

    // Stand up a processing pipeline which:
    // 1) parses input byte-stream into JSON documents
    // 2) invokes |lambda| with each document, using |store|.
    // 3) marshals emitted documents as stringified sequential JSON.
    // 4) pipes back to the request's response stream.
    let parse = JSONStreamValues.withParser();
    let transform = new LambdaTransform(lambda, store);
    req.pipe(parse).pipe(transform).pipe(req);

    // 'wantTrailers' is invoked (only) on clean |req| write stream end.
    // pipe() doesn't end streams or forward if an error occurs.
    req.on('wantTrailers', () => {
        trailers['num-input'] = transform.num_input;
        trailers['num-output'] = transform.num_output;

        if (!trailers['error']) {
            trailers['success'] = "true";
        }
        req.sendTrailers(trailers);
        parse.destroy();
        transform.destroy();
    });

    // Errors in intermediate pipeline steps abort the |req| stream with an error.
    let onErr = (err: Error) => {
        console.error(err);
        trailers['error'] = `${err.name} (${err.message})`;
        req.end(); // Trigger sending of trailers.
    };
    parse.on('error', onErr);
    transform.on('error', onErr);
}

const server = h2.createServer();
server.on('stream', _processTransformStream);
server.on('error', console.error);
server.listen({ path: process.env.SOCKET_PATH });
process.stdout.write("READY\n");