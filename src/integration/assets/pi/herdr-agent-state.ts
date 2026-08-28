// installed by herdr
// managed by herdr; reinstalling or updating the integration overwrites this file.
// add custom hooks/plugins beside this file instead of editing it.
// HERDR_INTEGRATION_ID=pi
// HERDR_INTEGRATION_VERSION=10
// @ts-nocheck

import net from "node:net";

const HERDR_ENV = process.env.HERDR_ENV;
const socketPath = process.env.HERDR_SOCKET_PATH;
const socketEndpoint =
  process.platform === "win32" && socketPath ? `\\\\.\\pipe\\${socketPath}` : socketPath;
const paneId = process.env.HERDR_PANE_ID;
const source = "herdr:pi";

function enabled() {
  return HERDR_ENV === "1" && !!socketPath && !!paneId;
}

function sendRequestAttempt(request: unknown, timeoutMs: number): Promise<boolean> {
  if (!enabled()) {
    return Promise.resolve(true);
  }

  return new Promise((resolve) => {
    let done = false;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const finish = (delivered: boolean) => {
      if (done) return;
      done = true;
      if (timeout) {
        clearTimeout(timeout);
      }
      socket.destroy();
      resolve(delivered);
    };

    const socket = net.createConnection(socketEndpoint!);
    socket.on("error", () => finish(false));
    socket.on("connect", () => socket.write(`${JSON.stringify(request)}\n`));
    socket.on("data", () => finish(true));
    socket.on("end", () => finish(false));
    timeout = setTimeout(() => finish(false), timeoutMs);
    timeout.unref?.();
  });
}

async function sendRequest(request: unknown): Promise<void> {
  if (await sendRequestAttempt(request, 500)) {
    return;
  }
  await sendRequestAttempt(request, 1500);
}

type AgentState = "working" | "blocked" | "idle";

// pi-subagents persists an async launch as a tool result whose details carry
// status "started" and a run id; completion arrives later as a
// "subagent_result" custom message with the same id. Unmatched launches mean
// children are still running and will steer results back into this session.
const SUBAGENT_LAUNCH_TOOLS = new Set(["subagent", "subagent_resume"]);
const SUBAGENT_RESULT_CUSTOM_TYPE = "subagent_result";

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function collectStartedRunIds(details: unknown, into: Set<string>): void {
  if (!isRecord(details)) {
    return;
  }
  if (details.status === "started" && typeof details.id === "string") {
    into.add(details.id);
  }
  if (Array.isArray(details.children)) {
    for (const child of details.children) {
      collectStartedRunIds(child, into);
    }
  }
}

function pendingSubagentRunIds(entries: unknown): Set<string> {
  const started = new Set<string>();
  if (!Array.isArray(entries)) {
    return started;
  }
  for (const entry of entries) {
    if (!isRecord(entry)) {
      continue;
    }
    const message = isRecord(entry.message) ? entry.message : undefined;
    if (
      entry.type === "message" &&
      message?.role === "toolResult" &&
      typeof message.toolName === "string" &&
      SUBAGENT_LAUNCH_TOOLS.has(message.toolName)
    ) {
      collectStartedRunIds(message.details, started);
      continue;
    }
    const signal = entryCustomSignal(entry, message);
    if (signal?.customType !== SUBAGENT_RESULT_CUSTOM_TYPE) {
      continue;
    }
    if (isRecord(signal.details) && typeof signal.details.id === "string") {
      started.delete(signal.details.id);
    }
  }
  return started;
}

// pi-background-tasks returns a running task snapshot from its launch tools
// and sends a terminal "background-task-notification" custom message with the
// same task id when it finishes. Tasks launched with notifyOnCompletion:false
// never emit one, so they must not count — they would hold the pane forever.
const BG_RUN_TOOLS = new Set(["bg_run", "bg_run_pi_attested"]);
const BG_NOTIFICATION_CUSTOM_TYPE = "background-task-notification";

function entryCustomSignal(
  entry: Record<string, unknown>,
  message: Record<string, unknown> | undefined,
): { customType: unknown; details: unknown } | undefined {
  if (entry.type === "custom_message") {
    return { customType: entry.customType, details: entry.details };
  }
  if (message?.role === "custom") {
    return { customType: message.customType, details: message.details };
  }
  return undefined;
}

function pendingBackgroundTaskIds(entries: unknown): Set<string> {
  const running = new Set<string>();
  if (!Array.isArray(entries)) {
    return running;
  }
  for (const entry of entries) {
    if (!isRecord(entry)) {
      continue;
    }
    const message = isRecord(entry.message) ? entry.message : undefined;
    if (
      entry.type === "message" &&
      message?.role === "toolResult" &&
      typeof message.toolName === "string" &&
      BG_RUN_TOOLS.has(message.toolName)
    ) {
      const task = isRecord(message.details) ? message.details.task : undefined;
      if (
        isRecord(task) &&
        task.status === "running" &&
        typeof task.id === "string" &&
        task.notifyOnCompletion !== false
      ) {
        running.add(task.id);
      }
      continue;
    }
    const signal = entryCustomSignal(entry, message);
    if (signal?.customType !== BG_NOTIFICATION_CUSTOM_TYPE) {
      continue;
    }
    if (isRecord(signal.details) && typeof signal.details.id === "string") {
      running.delete(signal.details.id);
    }
  }
  return running;
}

function runningWorkMessage(subagents: number, backgroundTasks: number): string | undefined {
  const parts: string[] = [];
  if (subagents > 0) {
    parts.push(subagents === 1 ? "1 subagent" : `${subagents} subagents`);
  }
  if (backgroundTasks > 0) {
    parts.push(backgroundTasks === 1 ? "1 background task" : `${backgroundTasks} background tasks`);
  }
  return parts.length > 0 ? `${parts.join(", ")} running` : undefined;
}

type QueuedState = {
  state: AgentState;
  message?: string;
  seq: number;
};

let reportSeq = Date.now() * 1000;
let currentAgentSessionId: string | undefined;
let currentAgentSessionPath: string | undefined;

function nextReportSeq(): number {
  reportSeq += 1;
  return reportSeq;
}

function updateSessionRef(ctx: any): void {
  try {
    const file = ctx?.sessionManager?.getSessionFile?.();
    currentAgentSessionPath =
      typeof file === "string" && file.startsWith("/") ? file : undefined;
  } catch {
    currentAgentSessionPath = undefined;
  }

  try {
    const id = ctx?.sessionManager?.getSessionId?.();
    currentAgentSessionId = typeof id === "string" && id.length > 0 ? id : undefined;
  } catch {
    currentAgentSessionId = undefined;
  }
}

function withSessionRef(params: Record<string, unknown>): Record<string, unknown> {
  if (currentAgentSessionPath) {
    return { ...params, agent_session_path: currentAgentSessionPath };
  }
  if (currentAgentSessionId) {
    return { ...params, agent_session_id: currentAgentSessionId };
  }
  return params;
}

function currentSessionRef(): Record<string, unknown> | undefined {
  if (currentAgentSessionPath) {
    return { agent_session_path: currentAgentSessionPath };
  }
  if (currentAgentSessionId) {
    return { agent_session_id: currentAgentSessionId };
  }
  return undefined;
}

function reportSession(sessionStartSource?: string): Promise<void> {
  const sessionRef = currentSessionRef();
  if (!sessionRef) {
    return Promise.resolve();
  }

  return sendRequest({
    id: `${source}:session:${Date.now()}:${Math.random().toString(36).slice(2)}`,
    method: "pane.report_agent_session",
    params: {
      pane_id: paneId,
      source,
      agent: "pi",
      seq: nextReportSeq(),
      session_start_source: sessionStartSource,
      ...sessionRef,
    },
  });
}

function sendState(state: AgentState, message?: string, seq = nextReportSeq()): Promise<void> {
  return sendRequest({
    id: `${source}:${Date.now()}:${Math.random().toString(36).slice(2)}`,
    method: "pane.report_agent",
    params: withSessionRef({
      pane_id: paneId,
      source,
      agent: "pi",
      state,
      message,
      seq,
    }),
  });
}

let sendInFlight = false;
let queuedState: QueuedState | undefined;

function queueState(state: AgentState, message?: string): void {
  queuedState = { state, message, seq: nextReportSeq() };
  if (!sendInFlight) {
    void drainStateQueue();
  }
}

async function drainStateQueue(): Promise<void> {
  if (sendInFlight) {
    return;
  }

  sendInFlight = true;
  try {
    while (queuedState) {
      const next = queuedState;
      queuedState = undefined;
      await sendState(next.state, next.message, next.seq);
    }
  } finally {
    sendInFlight = false;
    if (queuedState) {
      void drainStateQueue();
    }
  }
}

export default function (pi) {
  if (!enabled()) {
    return;
  }

  let agentActive = false;
  let pendingSubagents = 0;
  let pendingBackgroundTasks = 0;
  // Runs and tasks already pending when this extension loads can never report
  // back (pi-subagents tracks runs in memory; pi-background-tasks kills its
  // tasks on reload), so they are ignored forever.
  let orphanedSubagentRunIds = new Set<string>();
  let orphanedBackgroundTaskIds = new Set<string>();
  let blockedCount = 0;
  let blockedMessage: string | undefined;
  let lastState: AgentState | undefined;
  let lastMessage: string | undefined;
  let rootSession = false;

  function countUnmatched(pending: Set<string>, orphaned: Set<string>): number {
    let count = 0;
    for (const id of pending) {
      if (!orphaned.has(id)) {
        count += 1;
      }
    }
    return count;
  }

  function refreshPendingWork(ctx: any): void {
    let entries: unknown;
    try {
      entries = ctx?.sessionManager?.getEntries?.();
    } catch {
      return;
    }
    pendingSubagents = countUnmatched(pendingSubagentRunIds(entries), orphanedSubagentRunIds);
    pendingBackgroundTasks = countUnmatched(
      pendingBackgroundTaskIds(entries),
      orphanedBackgroundTaskIds,
    );
  }

  function desiredState() {
    if (blockedCount > 0) {
      return { state: "blocked" as const, message: blockedMessage };
    }
    if (agentActive) {
      return { state: "working" as const, message: undefined };
    }
    const pendingWorkMessage = runningWorkMessage(pendingSubagents, pendingBackgroundTasks);
    if (pendingWorkMessage) {
      return { state: "working" as const, message: pendingWorkMessage };
    }
    return { state: "idle" as const, message: undefined };
  }

  function publishState(force = false) {
    const next = desiredState();
    if (!force && next.state === lastState && next.message === lastMessage) {
      return;
    }
    lastState = next.state;
    lastMessage = next.message;
    queueState(next.state, next.message);
  }

  pi.events.on("herdr:blocked", (data) => {
    if (!rootSession) {
      return;
    }
    if (!data?.active) {
      blockedCount = Math.max(0, blockedCount - 1);
      if (blockedCount === 0) {
        blockedMessage = undefined;
      }
      publishState();
      return;
    }

    blockedCount += 1;
    blockedMessage = data.label;
    publishState();
  });

  pi.on("session_start", async (event, ctx) => {
    // TUI only: RPC/JSON/print modes are headless (no PTY herdr can display),
    // and RPC still reports hasUI=true, so mode is the reliable gate.
    if (ctx?.mode !== "tui") {
      return;
    }
    rootSession = true;
    updateSessionRef(ctx);
    await reportSession(event?.reason);
    // A reload can replace this extension mid-run without emitting another agent_start.
    agentActive = ctx?.isIdle?.() === false;
    try {
      const entries = ctx?.sessionManager?.getEntries?.();
      orphanedSubagentRunIds = pendingSubagentRunIds(entries);
      orphanedBackgroundTaskIds = pendingBackgroundTaskIds(entries);
    } catch {
      orphanedSubagentRunIds = new Set();
      orphanedBackgroundTaskIds = new Set();
    }
    pendingSubagents = 0;
    pendingBackgroundTasks = 0;
    publishState(true);
  });

  pi.on("agent_start", (_event, ctx) => {
    if (!rootSession) {
      return;
    }
    updateSessionRef(ctx);
    void reportSession();
    agentActive = true;
    publishState();
  });

  pi.on("agent_settled", (_event, ctx) => {
    if (!rootSession || ctx?.isIdle?.() !== true) {
      return;
    }

    agentActive = false;
    refreshPendingWork(ctx);
    publishState();
  });
}
