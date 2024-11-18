exports.id = 570;
exports.ids = [570];
exports.modules = {

/***/ 3276:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var __publicField = (obj, key, value) => {
  __defNormalProp(obj, typeof key !== "symbol" ? key + "" : key, value);
  return value;
};
var Mixcloud_exports = {};
__export(Mixcloud_exports, {
  default: () => Mixcloud
});
module.exports = __toCommonJS(Mixcloud_exports);
var import_react = __toESM(__webpack_require__(96540));
var import_utils = __webpack_require__(75635);
var import_patterns = __webpack_require__(50327);
const SDK_URL = "https://widget.mixcloud.com/media/js/widgetApi.js";
const SDK_GLOBAL = "Mixcloud";
class Mixcloud extends import_react.Component {
  constructor() {
    super(...arguments);
    __publicField(this, "callPlayer", import_utils.callPlayer);
    __publicField(this, "duration", null);
    __publicField(this, "currentTime", null);
    __publicField(this, "secondsLoaded", null);
    __publicField(this, "mute", () => {
    });
    __publicField(this, "unmute", () => {
    });
    __publicField(this, "ref", (iframe) => {
      this.iframe = iframe;
    });
  }
  componentDidMount() {
    this.props.onMount && this.props.onMount(this);
  }
  load(url) {
    (0, import_utils.getSDK)(SDK_URL, SDK_GLOBAL).then((Mixcloud2) => {
      this.player = Mixcloud2.PlayerWidget(this.iframe);
      this.player.ready.then(() => {
        this.player.events.play.on(this.props.onPlay);
        this.player.events.pause.on(this.props.onPause);
        this.player.events.ended.on(this.props.onEnded);
        this.player.events.error.on(this.props.error);
        this.player.events.progress.on((seconds, duration) => {
          this.currentTime = seconds;
          this.duration = duration;
        });
        this.props.onReady();
      });
    }, this.props.onError);
  }
  play() {
    this.callPlayer("play");
  }
  pause() {
    this.callPlayer("pause");
  }
  stop() {
  }
  seekTo(seconds, keepPlaying = true) {
    this.callPlayer("seek", seconds);
    if (!keepPlaying) {
      this.pause();
    }
  }
  setVolume(fraction) {
  }
  getDuration() {
    return this.duration;
  }
  getCurrentTime() {
    return this.currentTime;
  }
  getSecondsLoaded() {
    return null;
  }
  render() {
    const { url, config } = this.props;
    const id = url.match(import_patterns.MATCH_URL_MIXCLOUD)[1];
    const style = {
      width: "100%",
      height: "100%"
    };
    const query = (0, import_utils.queryString)({
      ...config.options,
      feed: `/${id}/`
    });
    return /* @__PURE__ */ import_react.default.createElement(
      "iframe",
      {
        key: id,
        ref: this.ref,
        style,
        src: `https://www.mixcloud.com/widget/iframe/?${query}`,
        frameBorder: "0",
        allow: "autoplay"
      }
    );
  }
}
__publicField(Mixcloud, "displayName", "Mixcloud");
__publicField(Mixcloud, "canPlay", import_patterns.canPlay.mixcloud);
__publicField(Mixcloud, "loopOnEnded", true);


/***/ })

};
;