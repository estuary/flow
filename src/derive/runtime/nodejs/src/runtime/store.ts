import * as h2 from 'http2';
import * as url from 'url';
import * as stream from 'stream';
import * as net from 'net';
import * as querystring from 'querystring';
import {Document} from './types';

// TODO(johnny): actually support expiry.
/*eslint @typescript-eslint/no-unused-vars: ["error", { "argsIgnorePattern": "^expiry$" }]*/

// Store of indexed, ordered JSON documents.
export class Store {
  session: h2.ClientHttp2Session;

  constructor(endpoint: string) {
    const opts: h2.ClientSessionOptions = {};

    if (endpoint.startsWith('uds:')) {
      opts.createConnection = function (authority: url.URL): stream.Duplex {
        return net.createConnection(authority.pathname);
      };
    }
    this.session = h2.connect(endpoint, opts);
  }

  // Set the key to the provided JSON document.
  set(key: string, doc: Document, expiry?: Date): Promise<void> {
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
      req.on('response', (hdrs: h2.IncomingHttpStatusHeader) => {
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
  get(key: string): Promise<Document> {
    return this._get(key, false);
  }

  // Returns an array of documents which are prefixed by the given key.
  getPrefix(key: string): Promise<Document[]> {
    return this._get(key, true);
  }

  _get(key: string, prefix: boolean): Promise<Document> {
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
      req.on('response', (hdrs: h2.IncomingHttpStatusHeader) => {
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
