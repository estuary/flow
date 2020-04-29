import * as stream from 'stream';
import {Store} from './store';
import {Document, TransformLambda} from './types';

// LambdaTransform is a stream.Transform which reads parsed JSON documents
// and applies them to the given Lambda, with the provided StateStore.
export class LambdaTransformer extends stream.Transform {
  lambda: TransformLambda; // Lambda function to invoke.
  store: Store; // State context.
  numInput: number; // Number of read input documents.
  numOutput: number; // Number of emitted output documents.

  // Builds a LambdaTransform which invokes the Lambda with the provided StateStore.
  constructor(lambda: TransformLambda, store: Store) {
    super({writableObjectMode: true});
    this.lambda = lambda;
    this.store = store;
    this.numInput = 0;
    this.numOutput = 0;
  }

  _transform(
    chunk: {value: Document},
    _: string,
    done: stream.TransformCallback
  ) {
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
  _emit(docs: Document[] | void) {
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
