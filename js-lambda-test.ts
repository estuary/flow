import * as h2 from "http2";
import * as net from 'net';
import * as JSONStreamValues from 'stream-json/streamers/StreamValues'
import * as stream from 'stream';
import * as querystring from 'querystring';
import { URL } from 'url';

async function getFive(): Promise<number> {
    return 5;
}
async function getTen(): Promise<number> {
    return 10;
}

type Lambda = (doc: any, ctx: DocStore) => any[] | Promise<any[]>;
interface LambdaMap { [name: string]: Lambda; }

let lambdas : LambdaMap = {
    "my_lambda": (doc, ctx) => [{...doc, val: 1}, {...doc, val: 2}],
    "my_lambda2": async (doc, ctx) => [
        {...doc, val: await getFive()},
        {...doc, val: await getTen()},
    ],
    "do_set": async (doc, store) => {
        await store.setDoc(doc.key, doc);
        return [];
    },
    "do_get": async (doc, store) => [ await store.getDoc(doc.key) ],
    "do_prefix": async (doc, store) => await store.getPrefix(doc.key),
};

class DocStore {
    session: h2.ClientHttp2Session;

    constructor(endpoint: string) {
        let opts : h2.ClientSessionOptions = {};

        if (endpoint.startsWith("uds:")) {
            opts.createConnection = function (authority: URL, _): stream.Duplex {
                return net.createConnection(authority.pathname)
            }
            opts.protocol = "http:"; // H2C prior-knowledge.
        }
        this.session = h2.connect(endpoint, opts)
    }

    setDoc(key: string, doc: any): Promise<void> {
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

    getDoc(key: string): Promise<any> {
        return this._get(key, false);
    }

    getPrefix(key: string): Promise<any[]> {
        return this._get(key, true);
    }
}

class LambdaTransform extends stream.Transform {
    lambda: Lambda;  // Lambda function to invoke.
    store: DocStore; // State context.
    num_input: number;
    num_output: number;

    constructor(lambda: Lambda, store: DocStore) {
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
        let i = 0;
        let parts = new Array<string>(docs.length * 3);

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

const server = h2.createServer();

server.on('error', (err) => {
    console.error(`server error ${err}`)
    server.close();
});

server.listen({ path: "node-test-sock" }); 

server.on('stream', (req: h2.ServerHttp2Stream, hdrs: h2.IncomingHttpHeaders, _flags) => {
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

    const store = new DocStore("uds://localhost/home/ubuntu/test-doc-store");

    req.respond({
        ':status': 200,
        'content-type': 'application/json-seq'
    }, {endStream: false, waitForTrailers: true});

    let trailers = {};
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
});