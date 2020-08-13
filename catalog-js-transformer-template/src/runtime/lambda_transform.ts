import * as stream from 'stream';
import {Document, TransformLambda} from './types';

// LambdaTransform is a stream.Transform which reads parsed JSON documents
// and applies them to the given Lambda, with the provided StateStore.
export class LambdaTransformer extends stream.Transform {
  lambda: TransformLambda;  // Lambda function to invoke.

  // Builds a LambdaTransform which invokes the Lambda.
  constructor(lambda: TransformLambda) {
    super({writableObjectMode: true});
    this.lambda = lambda;
  }

  _transform(
      chunk: {value: Document[]}, _: string,
      done: stream.TransformCallback): void {
    // |lambda| may or may not be async, and may or may not throw.
    // Wrap in an async invocation to ensure async throws, rejections,
    // and non-async throws all become Promise rejections.
    const invoke = async(): Promise<void> => {
      this._emit(await this.lambda(
          chunk.value[0],
          ...chunk.value.slice(
              1,
              )));
    };
    invoke()
        .then(() => done())
        .catch(done);  // Propagate as terminal pipeline error.
  }

  // Stringify each of an array of output documents, and emit as
  // content-encoding "application/json-seq".
  _emit(docs: Document[]|void): void {
    this.push(JSON.stringify(docs));
    this.push('\x0A');  // Line feed.
  }
}
