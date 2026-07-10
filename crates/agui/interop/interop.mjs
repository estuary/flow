// AG-UI interop harness.
//
// Drives the OFFICIAL `@ag-ui/client` (v0.0.57) HttpAgent against the Rust
// AG-UI server (the `agui` crate's `serve` example, backed by the deterministic
// MockProvider) and verifies the CLIENT's own post-run view of each scenario --
// `agent.messages`, `result.newMessages`, `result.result` -- rather than the raw
// SSE bytes. Passing here proves the Rust wire output is well-formed enough that
// the reference client reconstructs the intended messages/state.
//
// A thin in-process HTTP proxy sits between the client and the Rust server so we
// can (a) assert the server answered each POST with HTTP 200 (a serde rejection
// would surface as 4xx from axum, never reaching the client as a clean stream),
// and (b) inspect the exact request bodies the client transmits -- used to prove
// tool-call continuation input and multi-turn history reach the server verbatim.
//
// Run:  npm install && node interop.mjs   (or: npm test)
// The server binary is prebuilt at ../../../target/debug/examples/serve; override
// with AGUI_SERVE_BIN.

import { spawn } from "node:child_process";
import net from "node:net";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { HttpAgent } from "@ag-ui/client";

// undici (Node's fetch) can emit a *secondary* "terminated" promise rejection
// when a streaming socket is torn down (client abort, or upstream reset). It
// arrives after the run's own outcome is already settled, so it is noise; left
// unhandled it would crash the process mid-suite. This is a known behavior, not
// a protocol defect -- swallow and record it.
const swallowedRejections = [];
process.on("unhandledRejection", (reason) => {
  const message = reason?.message ?? String(reason);
  swallowedRejections.push(message);
  console.log(`  [guard] swallowed unhandledRejection: ${message}`);
});

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SERVE_BIN =
  process.env.AGUI_SERVE_BIN ??
  path.resolve(HERE, "../../../target/debug/examples/serve");

// ---------------------------------------------------------------------------
// Process / network plumbing.
// ---------------------------------------------------------------------------

// Reserve an ephemeral port by briefly binding it, then hand the number to the
// child. There is a small reuse window, but on loopback in a fresh container it
// is not a practical concern.
function reservePort() {
  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.on("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address();
      probe.close(() => resolve(port));
    });
  });
}

async function waitForPort(port, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const up = await new Promise((resolve) => {
      const socket = net.connect(port, "127.0.0.1");
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => {
        socket.destroy();
        resolve(false);
      });
    });
    if (up) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`server never accepted connections on :${port}`);
}

// Buffer-and-forward proxy. Records every request body and the upstream status,
// while streaming the SSE response back untouched. Records are appended in
// request order; scenarios run strictly sequentially so slicing by a saved
// length yields exactly that scenario's requests.
function startProxy(upstreamPort) {
  const requests = [];
  const server = http.createServer((downReq, downRes) => {
    const chunks = [];
    downReq.on("data", (c) => chunks.push(c));
    downReq.on("end", () => {
      const bodyBuf = Buffer.concat(chunks);
      let body = null;
      try {
        body = JSON.parse(bodyBuf.toString("utf8"));
      } catch {
        // Non-JSON bodies are recorded as null; not expected from the client.
      }
      const record = { body, status: null, bytes: bodyBuf.length };
      requests.push(record);

      const headers = { ...downReq.headers };
      delete headers.host;
      delete headers["content-length"]; // recomputed for the buffered body

      const upReq = http.request(
        {
          host: "127.0.0.1",
          port: upstreamPort,
          method: downReq.method,
          path: downReq.url,
          headers,
        },
        (upRes) => {
          record.status = upRes.statusCode;
          if (!downRes.headersSent) downRes.writeHead(upRes.statusCode, upRes.headers);
          upRes.pipe(downRes);
        },
      );
      upReq.on("error", () => {
        // Upstream torn down (e.g. client aborted -> we destroy upReq below).
        if (!downRes.writableEnded) downRes.end();
      });
      // Propagate a client abort to the upstream connection so the Rust server
      // observes the disconnect instead of writing into a dead socket.
      downRes.on("close", () => upReq.destroy());
      upReq.end(bodyBuf);
    });
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () =>
      resolve({ server, requests, port: server.address().port }),
    );
  });
}

// ---------------------------------------------------------------------------
// Test harness.
// ---------------------------------------------------------------------------

const outcomes = [];

function assert(cond, message) {
  if (!cond) throw new Error(message);
}

async function scenario(name, fn) {
  try {
    const evidence = await fn();
    outcomes.push({ name, pass: true, evidence });
    console.log(`PASS  ${name}\n      ${evidence}\n`);
  } catch (err) {
    outcomes.push({ name, pass: false, evidence: err?.message ?? String(err) });
    console.log(`FAIL  ${name}\n      ${err?.stack ?? err}\n`);
  }
}

// runAgent wrapped in a watchdog: a protocol bug that leaves the client waiting
// forever must fail loudly, not hang the suite. The mock streams instantly, so
// the timeout only fires on genuine breakage.
async function runAgent(agent, params = {}, subscriber, timeoutMs = 10000) {
  let timer;
  const watchdog = new Promise((_, reject) => {
    timer = setTimeout(() => {
      try {
        agent.abortRun();
      } catch {
        // best effort
      }
      reject(new Error(`run timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([agent.runAgent(params, subscriber), watchdog]);
  } finally {
    clearTimeout(timer);
  }
}

const mkAgent = (url) => new HttpAgent({ url });
const mockProps = (ops) => ({ forwardedProps: { _mock: ops } });
const allToolCalls = (messages) => messages.flatMap((m) => m.toolCalls ?? []);
const lastStatus = (proxy) => proxy.requests[proxy.requests.length - 1]?.status;

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

async function runScenarios(url, proxy) {
  // 1. Text-only run against the default (unscripted) mock.
  await scenario("1  text-only run (default mock)", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "hello" }];
    const { result, newMessages } = await runAgent(agent);
    assert(lastStatus(proxy) === 200, `server status ${lastStatus(proxy)} != 200`);
    const assistant = newMessages.filter((m) => m.role === "assistant");
    assert(assistant.length === 1, `expected 1 assistant message, got ${assistant.length}`);
    assert(
      assistant[0].content === "Hello from the mock provider.",
      `unexpected content ${JSON.stringify(assistant[0].content)}`,
    );
    assert(
      result && result.stopReason === "end_turn",
      `expected result.stopReason=end_turn, got ${JSON.stringify(result)}`,
    );
    return `HTTP 200; assistant="${assistant[0].content}"; result.stopReason=${result.stopReason}`;
  });

  // 2. Scripted text + tool call. The Rust server sets parentMessageId on the
  //    tool call to the preceding text message id, so the client must MERGE both
  //    into a single assistant message (content + toolCalls[0]).
  await scenario("2  scripted text + tool call (parentMessageId merge)", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "weather?" }];
    const { newMessages } = await runAgent(
      agent,
      mockProps([
        { text: "Checking the weather." },
        { toolCall: { name: "get_weather", args: '{"location":"Boston"}' } },
        { finish: { stopReason: "tool_use" } },
      ]),
    );
    assert(lastStatus(proxy) === 200, `server status ${lastStatus(proxy)} != 200`);
    const assistant = newMessages.filter((m) => m.role === "assistant");
    assert(assistant.length === 1, `expected ONE merged assistant message, got ${assistant.length}`);
    assert(
      assistant[0].content === "Checking the weather.",
      `unexpected content ${JSON.stringify(assistant[0].content)}`,
    );
    const calls = assistant[0].toolCalls ?? [];
    assert(calls.length === 1, `expected 1 toolCall on the assistant message, got ${calls.length}`);
    assert(calls[0].function.name === "get_weather", `tool name ${calls[0].function.name}`);
    assert(
      calls[0].function.arguments === '{"location":"Boston"}',
      `accumulated args mismatch: ${calls[0].function.arguments}`,
    );
    return `merged message: content + toolCall get_weather args=${calls[0].function.arguments}`;
  });

  // 3. Tool-only run, then a client-driven continuation with a tool result.
  //    Verifies the Rust server both PRODUCES a standalone tool-call message and
  //    ACCEPTS (HTTP 200) a follow-up input carrying assistant toolCalls + a tool
  //    message -- the full agentic loop against the real server.
  await scenario("3  tool-only run + tool-result continuation", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "weather?" }];
    const tools = [
      { name: "get_weather", description: "current weather", parameters: { type: "object", properties: {} } },
    ];

    const first = await runAgent(
      agent,
      { tools, ...mockProps([{ toolCall: { name: "get_weather", args: '{"location":"Boston"}' } }, { finish: { stopReason: "tool_use" } }]) },
    );
    assert(lastStatus(proxy) === 200, `run1 server status ${lastStatus(proxy)} != 200`);
    const firstAssistant = first.newMessages.filter((m) => m.role === "assistant");
    assert(firstAssistant.length === 1, `run1 expected 1 assistant message, got ${firstAssistant.length}`);
    const calls = firstAssistant[0].toolCalls ?? [];
    assert(calls.length === 1, `run1 expected 1 toolCall, got ${calls.length}`);
    assert(!firstAssistant[0].content, `run1 assistant should be tool-only, content=${JSON.stringify(firstAssistant[0].content)}`);
    const toolCallId = calls[0].id;

    // Feed the frontend-executed tool result back and continue the run.
    agent.messages.push({ id: "tr_1", role: "tool", content: "55F", toolCallId });
    const beforeSecond = proxy.requests.length;
    const second = await runAgent(
      agent,
      { tools, ...mockProps([{ text: "It is 55F in Boston." }, { finish: { stopReason: "end_turn" } }]) },
    );
    const secondReq = proxy.requests[beforeSecond];
    assert(secondReq.status === 200, `continuation server status ${secondReq.status} != 200 (server rejected the follow-up input)`);
    // The server must have RECEIVED assistant toolCalls + the tool message.
    const roles = secondReq.body.messages.map((m) => m.role);
    assert(
      roles.includes("assistant") && roles.includes("tool"),
      `continuation POST missing assistant/tool messages; roles=${JSON.stringify(roles)}`,
    );
    const sentTool = secondReq.body.messages.find((m) => m.role === "tool");
    assert(sentTool.toolCallId === toolCallId, `continuation tool msg toolCallId=${sentTool.toolCallId} != ${toolCallId}`);
    const secondAssistant = second.newMessages.filter((m) => m.role === "assistant");
    assert(
      secondAssistant.length === 1 && secondAssistant[0].content === "It is 55F in Boston.",
      `continuation did not resolve to expected text: ${JSON.stringify(secondAssistant)}`,
    );
    return `tool call id=${toolCallId}; continuation accepted (HTTP 200) with assistant+tool input; final="${secondAssistant[0].content}"`;
  });

  // 4. Two tool calls in one run. Both surface with correct names/args.
  await scenario("4  two tool calls in one run", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "do two things" }];
    const { newMessages } = await runAgent(
      agent,
      mockProps([
        { toolCall: { name: "alpha", args: "{}" } },
        { toolCall: { name: "beta", args: '{"x":1}' } },
        { finish: { stopReason: "tool_use" } },
      ]),
    );
    assert(lastStatus(proxy) === 200, `server status ${lastStatus(proxy)} != 200`);
    const calls = allToolCalls(newMessages);
    assert(calls.length === 2, `expected 2 tool calls, got ${calls.length}`);
    const byName = Object.fromEntries(calls.map((c) => [c.function.name, c.function.arguments]));
    assert(byName.alpha === "{}", `alpha args ${byName.alpha}`);
    assert(byName.beta === '{"x":1}', `beta args ${byName.beta}`);
    return `tool calls: alpha(${byName.alpha}), beta(${byName.beta})`;
  });

  // 5. Reasoning followed by text. The reasoning message must precede the
  //    assistant message.
  await scenario("5  reasoning + text (reasoning precedes assistant)", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "think then answer" }];
    const { newMessages } = await runAgent(
      agent,
      mockProps([
        { reasoning: "Let me think about this." },
        { text: "The answer is 42." },
        { finish: { stopReason: "end_turn" } },
      ]),
    );
    assert(lastStatus(proxy) === 200, `server status ${lastStatus(proxy)} != 200`);
    const reasoningIdx = newMessages.findIndex((m) => m.role === "reasoning");
    const assistantIdx = newMessages.findIndex((m) => m.role === "assistant");
    assert(reasoningIdx >= 0, `no reasoning message: ${JSON.stringify(newMessages.map((m) => m.role))}`);
    assert(assistantIdx >= 0, `no assistant message`);
    assert(reasoningIdx < assistantIdx, `reasoning (${reasoningIdx}) did not precede assistant (${assistantIdx})`);
    assert(
      newMessages[reasoningIdx].content === "Let me think about this." &&
        newMessages[assistantIdx].content === "The answer is 42.",
      `unexpected content: ${JSON.stringify(newMessages)}`,
    );
    return `reasoning@${reasoningIdx} "${newMessages[reasoningIdx].content}" before assistant@${assistantIdx}`;
  });

  // 6. Mid-stream provider error. runAgent RESOLVES (RUN_ERROR is non-throwing
  //    in this client); partial text is preserved; a subscriber captures the
  //    RUN_ERROR payload.
  await scenario("6  mid-stream provider error (resolves, error surfaced)", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "will fail" }];
    let capturedError = null;
    const subscriber = {
      onRunErrorEvent: (p) => {
        capturedError = p?.event?.message ?? p?.message ?? null;
      },
    };
    let threw = null;
    let newMessages = [];
    try {
      const res = await runAgent(
        agent,
        mockProps([{ text: "Partial answer before boom." }, { error: "mock provider exploded" }]),
        subscriber,
      );
      newMessages = res.newMessages;
    } catch (err) {
      threw = err;
    }
    assert(!threw, `runAgent threw instead of resolving: ${threw?.message}`);
    const assistant = newMessages.filter((m) => m.role === "assistant");
    assert(
      assistant.length === 1 && assistant[0].content === "Partial answer before boom.",
      `partial text not preserved: ${JSON.stringify(newMessages)}`,
    );
    assert(
      capturedError === "mock provider exploded",
      `onRunErrorEvent message mismatch: ${JSON.stringify(capturedError)}`,
    );
    return `resolved; partial="${assistant[0].content}"; onRunErrorEvent="${capturedError}"`;
  });

  // 7. abortRun(). NOTE: the mock emits its whole script eagerly with no
  //    inter-event delay (crates/agui/src/mock.rs uses futures::stream::iter),
  //    so a deterministic *mid-stream* interception is not possible. Per the
  //    task's fallback, we abort right after kicking off the run and assert that
  //    (a) abort never throws and (b) the agent stays usable for a later run.
  await scenario("7  abortRun() does not throw or corrupt the agent", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "stream" }];
    const big = Array.from({ length: 40 }, (_, i) => `chunk-${i}`).join(" ");
    let threw = null;
    const runPromise = agent.runAgent(mockProps([{ text: big }, { finish: { stopReason: "end_turn" } }]));
    agent.abortRun(); // synchronous, races the (instant) stream
    try {
      await runPromise;
    } catch (err) {
      threw = err;
    }
    assert(!threw, `aborted run threw: ${threw?.message}`);

    // The agent must remain usable afterwards.
    const { newMessages } = await runAgent(
      agent,
      mockProps([{ text: "Recovered fine." }, { finish: { stopReason: "end_turn" } }]),
    );
    const assistant = newMessages.filter((m) => m.role === "assistant");
    assert(
      assistant.length === 1 && assistant[0].content === "Recovered fine.",
      `agent not reusable after abort: ${JSON.stringify(newMessages)}`,
    );
    return `abort resolved cleanly (no throw); subsequent run produced "${assistant[0].content}" (mock has no inter-event delay, so abort races an instant stream)`;
  });

  // 8. Frontend tools transmission. The mock ignores tools, so this only proves
  //    the Rust types accept and round-trip a full tool definition (HTTP 200 =
  //    the axum Json extractor deserialized it; a bad shape would 4xx).
  await scenario("8  frontend tools transmission (request shape accepted)", async () => {
    const agent = mkAgent(url);
    agent.messages = [{ id: "u1", role: "user", content: "use a tool maybe" }];
    const tools = [
      {
        name: "lookup",
        description: "Look up a value",
        parameters: {
          type: "object",
          properties: { q: { type: "string" } },
          required: ["q"],
        },
      },
    ];
    const before = proxy.requests.length;
    const { newMessages } = await runAgent(agent, { tools, ...mockProps([{ text: "ok" }, { finish: { stopReason: "end_turn" } }]) });
    const req = proxy.requests[before];
    assert(req.status === 200, `server status ${req.status} != 200 (rejected the tool definition)`);
    assert(req.body.tools?.length === 1 && req.body.tools[0].name === "lookup", `tools not transmitted: ${JSON.stringify(req.body.tools)}`);
    assert(req.body.tools[0].parameters?.required?.[0] === "q", `tool parameters not round-tripped: ${JSON.stringify(req.body.tools[0].parameters)}`);
    const assistant = newMessages.filter((m) => m.role === "assistant");
    assert(assistant.length === 1 && assistant[0].content === "ok", `run did not complete: ${JSON.stringify(newMessages)}`);
    return `tools transmitted + accepted (HTTP 200); run produced "${assistant[0].content}"`;
  });

  // 9. Multi-turn conversation memory. One agent, two user turns; the second
  //    POST must carry the full prior history (user1, assistant1, user2), and
  //    agent.messages must grow to 4.
  await scenario("9  multi-turn conversation memory", async () => {
    const agent = mkAgent(url);
    const before = proxy.requests.length;

    agent.messages = [{ id: "u1", role: "user", content: "What is 2+2?" }];
    await runAgent(agent, mockProps([{ text: "It is 4." }, { finish: { stopReason: "end_turn" } }]));
    assert(agent.messages.length === 2, `after turn 1 expected 2 messages, got ${agent.messages.length}`);

    agent.messages.push({ id: "u2", role: "user", content: "And 3+3?" });
    await runAgent(agent, mockProps([{ text: "It is 6." }, { finish: { stopReason: "end_turn" } }]));
    assert(agent.messages.length === 4, `after turn 2 expected 4 messages, got ${agent.messages.length}`);

    const turn2 = proxy.requests[before + 1];
    assert(turn2.status === 200, `turn 2 server status ${turn2.status} != 200`);
    const sent = turn2.body.messages;
    assert(sent.length === 3, `turn 2 POST expected 3 prior messages, got ${sent.length}: ${JSON.stringify(sent.map((m) => m.role))}`);
    const contents = sent.map((m) => (typeof m.content === "string" ? m.content : ""));
    assert(
      contents.includes("What is 2+2?") && contents.includes("It is 4.") && contents.includes("And 3+3?"),
      `turn 2 POST missing history: ${JSON.stringify(contents)}`,
    );
    return `turn 2 POST carried full history [${sent.map((m) => m.role).join(", ")}]; agent.messages grew to ${agent.messages.length}`;
  });
}

// ---------------------------------------------------------------------------
// Orchestration.
// ---------------------------------------------------------------------------

async function main() {
  const serverPort = await reservePort();
  const serverLog = [];
  const server = spawn(SERVE_BIN, {
    env: { ...process.env, PORT: String(serverPort) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  server.stdout.on("data", (d) => serverLog.push(d.toString()));
  server.stderr.on("data", (d) => serverLog.push(d.toString()));
  server.on("error", (err) => {
    console.error(`failed to spawn server (${SERVE_BIN}): ${err.message}`);
  });

  let proxy;
  try {
    await waitForPort(serverPort);
    proxy = await startProxy(serverPort);
    const url = `http://127.0.0.1:${proxy.port}/agui`;
    console.log(`server pid=${server.pid} on :${serverPort}; proxy on :${proxy.port}; endpoint ${url}\n`);
    await runScenarios(url, proxy);
  } catch (err) {
    console.error("harness setup failed:", err.message);
    if (serverLog.length) console.error("server output:\n" + serverLog.join(""));
    outcomes.push({ name: "harness setup", pass: false, evidence: err.message });
  } finally {
    if (proxy) proxy.server.close();
    server.kill("SIGKILL");
  }

  // Summary.
  const passed = outcomes.filter((o) => o.pass).length;
  console.log("=".repeat(78));
  console.log(`SUMMARY: ${passed}/${outcomes.length} scenarios passed`);
  console.log("=".repeat(78));
  for (const o of outcomes) {
    console.log(`${o.pass ? "PASS" : "FAIL"}  ${o.name}`);
    console.log(`      ${o.evidence}`);
  }
  if (swallowedRejections.length) {
    console.log(`\n(${swallowedRejections.length} undici 'terminated' rejection(s) swallowed by the guard -- expected on socket drops)`);
  }

  const failed = outcomes.filter((o) => !o.pass).length;
  process.exit(failed === 0 ? 0 : 1);
}

main();
