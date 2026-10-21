/**
 * Replay-safe Temporal workflow trace bridge.
 *
 * The workflow isolate never performs network I/O. Its only side effect is a
 * Temporal sink, registered with callDuringReplay:false by each real worker.
 */
import {
  ContinueAsNew, defaultPayloadConverter, proxySinks, workflowInfo,
  type WorkflowInterceptors, type WorkflowInboundCallsInterceptor,
  type WorkflowOutboundCallsInterceptor, type Sinks
} from "@temporalio/workflow";
import type { Headers } from "@temporalio/common";
import { formatTraceparent, parseTraceparent } from "@space/contracts";

export const TRACE_HEADER = "space-otlp-traceparent";

export interface WorkflowTraceReport {
  traceId: string;
  spanId: string;
  parentSpanId: string;
  workflowType: string;
  workflowId: string;
  runId: string;
  startMs: number;
  endMs: number;
  error: boolean;
  continuedFromRunId?: string;
}

interface WorkflowTraceSinks extends Sinks {
  spaceOtlp: { finished(report: WorkflowTraceReport): void };
}

/** A stable nonzero span ID from Temporal's randomly assigned workflow run UUID. */
export function workflowRunSpanId(runId: string): string {
  const hex = runId.replace(/-/g, "").toLowerCase();
  if (!/^[0-9a-f]{32}$/.test(hex)) return "0000000000000001";
  const id = hex.slice(-16);
  return /^0{16}$/.test(id) ? "0000000000000001" : id;
}

function headerTrace(headers: Headers): string | null {
  const encoded = headers[TRACE_HEADER];
  if (!encoded) return null;
  try {
    const value = defaultPayloadConverter.fromPayload(encoded);
    return typeof value === "string" && parseTraceparent(value) ? value : null;
  } catch {
    return null;
  }
}

/** Never descend into prompt/content fields. Read only named, bounded context. */
function argTrace(args: readonly unknown[]): string | null {
  for (const value of args) {
    if (!value || typeof value !== "object" || Array.isArray(value)) continue;
    const o = value as Record<string, unknown>;
    if (typeof o.traceparent === "string" && parseTraceparent(o.traceparent)) return o.traceparent;
    for (const nested of ["turn", "item", "input"] as const) {
      const n = o[nested];
      if (!n || typeof n !== "object" || Array.isArray(n)) continue;
      const v = (n as Record<string, unknown>).traceparent;
      if (typeof v === "string" && parseTraceparent(v)) return v;
    }
  }
  return null;
}

export function interceptors(): WorkflowInterceptors {
  const { spaceOtlp } = proxySinks<WorkflowTraceSinks>();
  let outgoingTrace: string | null = null;
  const inbound: WorkflowInboundCallsInterceptor = {
    async execute(input, next) {
      const original = headerTrace(input.headers) ?? argTrace(input.args);
      const parent = parseTraceparent(original);
      if (!parent) return next(input);
      const info = workflowInfo();
      const spanId = workflowRunSpanId(info.runId);
      outgoingTrace = formatTraceparent(parent.traceId, spanId, parent.flags);
      const startMs = Date.now();
      let error = false;
      try {
        return await next(input);
      } catch (cause) {
        error = !(cause instanceof ContinueAsNew);
        throw cause;
      } finally {
        spaceOtlp.finished({
          traceId: parent.traceId, spanId, parentSpanId: parent.parentId,
          workflowType: info.workflowType, workflowId: info.workflowId,
          runId: info.runId, startMs, endMs: Date.now(), error,
          continuedFromRunId: info.continuedFromExecutionRunId
        });
      }
    }
  };
  const withTrace = (headers: Headers): Headers => {
    if (!outgoingTrace) return headers;
    const payload = defaultPayloadConverter.toPayload(outgoingTrace);
    return payload ? { ...headers, [TRACE_HEADER]: payload } : headers;
  };
  const outbound: WorkflowOutboundCallsInterceptor = {
    scheduleActivity: (input, next) => next({ ...input, headers: withTrace(input.headers) }),
    scheduleLocalActivity: (input, next) => next({ ...input, headers: withTrace(input.headers) }),
    startChildWorkflowExecution: (input, next) => next({ ...input, headers: withTrace(input.headers) }),
    continueAsNew: (input, next) => next({ ...input, headers: withTrace(input.headers) })
  };
  return { inbound: [inbound], outbound: [outbound] };
}
