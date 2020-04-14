"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var h2 = require("http2");
var net = require("net");
var StreamValues = require("stream-json/streamers/StreamValues");
var stream = require("stream");
var querystring = require("querystring");
function getFive() {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, 5];
        });
    });
}
function getTen() {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, 10];
        });
    });
}
var lambdas = {
    "my_lambda": function (doc, ctx) { return [__assign(__assign({}, doc), { val: 1 }), __assign(__assign({}, doc), { val: 2 })]; },
    "my_lambda2": function (doc, ctx) { return __awaiter(void 0, void 0, void 0, function () {
        var _a, _b, _c, _d, _e;
        return __generator(this, function (_f) {
            switch (_f.label) {
                case 0:
                    _a = [__assign({}, doc)];
                    _b = {};
                    return [4 /*yield*/, getFive()];
                case 1:
                    _c = [
                        __assign.apply(void 0, _a.concat([(_b.val = _f.sent(), _b)]))
                    ];
                    _d = [__assign({}, doc)];
                    _e = {};
                    return [4 /*yield*/, getTen()];
                case 2: return [2 /*return*/, _c.concat([
                        __assign.apply(void 0, _d.concat([(_e.val = _f.sent(), _e)]))
                    ])];
            }
        });
    }); },
    "do_set": function (doc, store) { return __awaiter(void 0, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, store.setDoc(doc.key, doc)];
                case 1:
                    _a.sent();
                    return [2 /*return*/, []];
            }
        });
    }); },
    "do_get": function (doc, store) { return __awaiter(void 0, void 0, void 0, function () { return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, store.getDoc(doc.key)];
            case 1: return [2 /*return*/, [_a.sent()]];
        }
    }); }); },
    "do_prefix": function (doc, store) { return __awaiter(void 0, void 0, void 0, function () { return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, store.getPrefix(doc.key)];
            case 1: return [2 /*return*/, _a.sent()];
        }
    }); }); }
};
var DocStore = /** @class */ (function () {
    function DocStore(endpoint) {
        var opts = {};
        if (endpoint.startsWith("uds:")) {
            opts.createConnection = function (authority, _) {
                return net.createConnection(authority.pathname);
            };
            opts.protocol = "http:"; // H2C prior-knowledge.
        }
        this.session = h2.connect(endpoint, opts);
    }
    DocStore.prototype.setDoc = function (key, doc) {
        var data = JSON.stringify({
            key: key,
            doc: doc
        });
        var req = this.session.request({
            ':method': 'PUT',
            ':path': '/docs',
            'content-type': 'application/json',
            'content-length': data.length
        }, { endStream: false });
        req.setEncoding('utf8'); // 'data' as strings, not Buffers.
        req.write(data);
        req.end(); // Send client EOF (waitForTrailers not set).
        return new Promise(function (resolve, reject) {
            req.on('response', function (hdrs, _flags) {
                if (hdrs[':status'] != 200) {
                    reject("unexpected response " + hdrs);
                }
                console.log('got setDoc headers %j', hdrs);
            });
            req.on('data', function () { }); // We expect no response.
            req.on('end', resolve); // Read server EOF.
            req.on('error', reject);
        });
    };
    DocStore.prototype._get = function (key, prefix) {
        var query = querystring.encode({ key: key, prefix: prefix ? true : false });
        var req = this.session.request({
            ':method': 'GET',
            ':path': '/docs?' + query
        }, { endStream: true }); // Send client EOF.
        req.setEncoding('utf8'); // 'data' as strings, not Buffers.
        return new Promise(function (resolve, reject) {
            req.on('response', function (hdrs, _flags) {
                if (hdrs[':status'] != 200) {
                    reject("unexpected response " + hdrs);
                }
                console.log("got _get headers %j", hdrs);
            });
            var chunks = new Array();
            req.on('data', function (chunk) { return chunks.push(chunk); });
            req.on('end', function () {
                try {
                    var parsed = JSON.parse(chunks.join(''));
                    if (prefix) {
                        resolve(parsed[0] || null);
                    }
                    else {
                        resolve(parsed);
                    }
                }
                catch (err) {
                    reject(err);
                }
            });
            req.on('error', reject);
        });
    };
    DocStore.prototype.getDoc = function (key) {
        return this._get(key, false);
    };
    DocStore.prototype.getPrefix = function (key) {
        return this._get(key, true);
    };
    return DocStore;
}());
var LambdaTransform = /** @class */ (function (_super) {
    __extends(LambdaTransform, _super);
    function LambdaTransform(lambda, store) {
        var _this = _super.call(this, { writableObjectMode: true }) || this;
        _this.lambda = lambda;
        _this.store = store;
        _this.num_input = 0;
        _this.num_output = 0;
        return _this;
    }
    LambdaTransform.prototype._transform = function (chunk, _, done) {
        var _this = this;
        this.num_input++;
        // |lambda| may or may not be async, and may or may not throw.
        // Wrap in an async invocation to ensure async throws, rejections,
        // and non-async throws all become Promise rejections.
        var invoke = function () { return __awaiter(_this, void 0, void 0, function () {
            var _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _a = this._emit;
                        return [4 /*yield*/, this.lambda(chunk.value, this.store)];
                    case 1:
                        _a.apply(this, [_b.sent()]);
                        return [2 /*return*/];
                }
            });
        }); };
        invoke()
            .then(function () { return done(); })["catch"](done); // Propagate as terminal pipeline error.
    };
    // Stringify each of an array of output documents, and emit as
    // content-encoding "application/json-seq".
    LambdaTransform.prototype._emit = function (docs) {
        var i = 0;
        var parts = new Array(docs.length * 3);
        for (var _i = 0, docs_1 = docs; _i < docs_1.length; _i++) {
            var doc = docs_1[_i];
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
    };
    return LambdaTransform;
}(stream.Transform));
;
var server = h2.createServer();
server.on('error', function (err) {
    console.error("server error " + err);
    server.close();
});
server.listen({ path: "node-test-sock" });
server.on('stream', function (req, hdrs, _flags) {
    var malformed = function (msg) {
        req.respond({
            ':status': 400,
            'content-type': 'text/plain'
        });
        req.end(msg); // Send message & EOF.
    };
    var lambda_name = hdrs[":path"].slice(1);
    var lambda = lambdas[lambda_name];
    if (!lambda) {
        return malformed("lambda " + lambda_name + " is not defined");
    }
    var store = new DocStore("uds://localhost/home/ubuntu/test-doc-store");
    req.respond({
        ':status': 200,
        'content-type': 'application/json-seq'
    }, { endStream: false, waitForTrailers: true });
    var trailers = {};
    var parse = StreamValues.withParser();
    var transform = new LambdaTransform(lambda, store);
    req.pipe(parse).pipe(transform).pipe(req);
    // 'wantTrailers' is invoked (only) on clean |req| write stream end.
    // pipe() doesn't end streams or forward if an error occurs.
    req.on('wantTrailers', function () {
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
    var onErr = function (err) {
        console.error(err);
        trailers['error'] = err.name + " (" + err.message + ")";
        req.end(); // Trigger sending of trailers.
    };
    parse.on('error', onErr);
    transform.on('error', onErr);
});
