import {
  RestrictedTextJobError,
  type RestrictedTextJobInput,
  type RestrictedTextJobResult,
  type ProviderSetupError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as References from "effect/References";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Sse from "effect/encoding/Sse";
import { HttpClient, HttpClientRequest } from "effect/http";
import type { CodexChatGptCredentials } from "../provider/CodexChatGptAuth.ts";
import { chatGptModels } from "../provider/CodexChatGptModels.ts";

const isRestrictedTextJobError = Schema.is(RestrictedTextJobError);
const Message = Schema.Struct({
  type: Schema.Literal("message"),
  role: Schema.Literal("assistant"),
  status: Schema.Literal("completed"),
  content: Schema.Array(
    Schema.Struct({ type: Schema.Literal("output_text"), text: Schema.String }),
  ),
});
// Project reasoning to its tag/status so opaque content never survives decoding.
const Reasoning = Schema.Struct({
  type: Schema.Literal("reasoning"),
  status: Schema.optional(Schema.Literal("completed")),
});
const Completed = Schema.Struct({
  status: Schema.Literal("completed"),
  model: Schema.String,
  output: Schema.Array(Schema.Union([Message, Reasoning])),
});
const Envelope = Schema.Struct({ type: Schema.String });
const OutputEvent = Schema.Struct({
  item: Schema.Struct({ type: Schema.Literals(["message", "reasoning"]) }),
});
const ContentEvent = Schema.Struct({
  part: Schema.Struct({
    type: Schema.Literals(["output_text", "reasoning_text"]),
  }),
});
const CompletionEvent = Schema.Struct({ response: Completed });
const UsageFailure = Schema.Struct({
  response: Schema.Struct({
    error: Schema.Struct({
      code: Schema.Literals([
        "subscription_sharing_usage_limit_exceeded",
        "subscription_sharing_usage_unavailable",
      ]),
    }),
  }),
});
const isUsageFailure = Schema.decodeUnknownEffect(Schema.fromJsonString(UsageFailure));
const parseEvent = Schema.decodeUnknownEffect(Schema.fromJsonString(Envelope));
const parseCompletion = Schema.decodeUnknownEffect(Schema.fromJsonString(CompletionEvent));
const parseOutput = Schema.decodeUnknownEffect(Schema.fromJsonString(OutputEvent));
const parseContent = Schema.decodeUnknownEffect(Schema.fromJsonString(ContentEvent));
const parseResult = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.JsonObject));
const allowedEvents = new Set([
  "response.created",
  "response.in_progress",
  "response.output_item.added",
  "response.output_item.done",
  "response.content_part.added",
  "response.content_part.done",
  "response.output_text.delta",
  "response.output_text.done",
  "response.completed",
  "response.reasoning_summary_part.added",
  "response.reasoning_summary_part.done",
  "response.reasoning_summary_text.delta",
  "response.reasoning_summary_text.done",
  "response.reasoning_text.delta",
  "response.reasoning_text.done",
]);
const protocolName = (value: string) =>
  /^[\w.:/+;= -]{1,400}$/.test(value) ? value : "unrecognised";
// Names only the failed check and upstream protocol identifiers, never request or response text.
const rejectOutput = (step: string, detail: Record<string, string> = {}) =>
  Effect.logWarning("Restricted text job rejected upstream output").pipe(
    Effect.annotateLogs({
      step,
      ...Object.fromEntries(
        Object.entries(detail).map(([key, value]) => [key, protocolName(value)]),
      ),
    }),
    Effect.andThen(Effect.fail(new RestrictedTextJobError({ reason: "invalid_output" }))),
  );

const eventChecks = new Map<string, (data: string) => Effect.Effect<unknown, Schema.SchemaError>>([
  ["response.output_item.added", parseOutput],
  ["response.output_item.done", parseOutput],
  ["response.content_part.added", parseContent],
  ["response.content_part.done", parseContent],
]);
const failedResponse = (data: string) =>
  isUsageFailure(data).pipe(
    Effect.match({ onSuccess: () => true, onFailure: () => false }),
    Effect.flatMap((limited) =>
      Effect.fail(new RestrictedTextJobError({ reason: limited ? "usage_limit" : "upstream" })),
    ),
  );
const completedText = (completed: typeof Completed.Type | undefined, model: string) => {
  if (!completed) return rejectOutput("missing-completion");
  if (completed.model !== model) return rejectOutput("model", { returnedModel: completed.model });
  return Effect.succeed(
    completed.output
      .flatMap((item) => (item.type === "message" ? item.content.map((part) => part.text) : []))
      .join(""),
  );
};

export const makeRestrictedChatGptJob = Effect.fn("makeRestrictedChatGptJob")(function* (
  access: Effect.Effect<CodexChatGptCredentials, ProviderSetupError>,
) {
  const rawHttp = yield* HttpClient.HttpClient;
  const http = HttpClient.withScope(rawHttp);
  // Neither request bodies nor successful results may enter span exits.
  return Effect.fnUntraced(
    function* (
      input: RestrictedTextJobInput,
    ): Effect.fn.Return<RestrictedTextJobResult, RestrictedTextJobError, Scope.Scope> {
      const credentials = yield* access.pipe(
        Effect.mapError(() => new RestrictedTextJobError({ reason: "sign_in_required" })),
      );
      if (!credentials.scopes.includes("chatgpt.tokens.use.direct"))
        return yield* new RestrictedTextJobError({
          reason: "sign_in_required",
        });
      const models = yield* chatGptModels(credentials.accessToken, []).pipe(
        Effect.provideService(HttpClient.HttpClient, rawHttp),
        Effect.mapError(() => new RestrictedTextJobError({ reason: "model_unavailable" })),
      );
      if (!models.some((model) => model.slug === input.model))
        return yield* new RestrictedTextJobError({
          reason: "model_unavailable",
        });
      const response = yield* http
        .execute(
          HttpClientRequest.post("https://api.openai.com/v1/responses").pipe(
            HttpClientRequest.bearerToken(credentials.accessToken),
            HttpClientRequest.bodyJsonUnsafe({
              model: input.model,
              instructions: input.instructions,
              input: [
                {
                  role: "user",
                  content: [{ type: "input_text", text: input.input }],
                },
              ],
              tools: [],
              tool_choice: "none",
              store: false,
              stream: true,
              service_tier: "default",
              text: { format: { type: "json_object" } },
            }),
          ),
        )
        .pipe(Effect.mapError(() => new RestrictedTextJobError({ reason: "upstream" })));
      if (response.status !== 200)
        return yield* new RestrictedTextJobError({
          reason: response.status === 429 ? "usage_limit" : "upstream",
        });
      if (!response.headers["content-type"]?.includes("text/event-stream"))
        return yield* rejectOutput("content-type", {
          contentType: response.headers["content-type"] ?? "missing",
          headerNames: Object.keys(response.headers).sort().join(" "),
          server: response.headers.server ?? "missing",
        });
      let totalBytes = 0;
      let completed: typeof Completed.Type | undefined;
      yield* response.stream.pipe(
        Stream.mapEffect((chunk) => {
          totalBytes += chunk.byteLength;
          return totalBytes > 8_388_608
            ? Effect.fail(new RestrictedTextJobError({ reason: "output_limit" }))
            : Effect.succeed(chunk);
        }),
        Stream.decodeText(),
        Stream.pipeThroughChannel(Sse.decode({ maxEventSize: 2_097_152 })),
        Stream.runForEach(
          Effect.fnUntraced(function* (event) {
            if (event.data === "[DONE]" && completed) return;
            if (completed) return yield* rejectOutput("event-after-completion");
            const { type } = yield* parseEvent(event.data).pipe(
              Effect.catch(() => rejectOutput("event-envelope")),
            );
            if (type === "response.failed") return yield* failedResponse(event.data);
            if (!allowedEvents.has(type))
              return yield* rejectOutput("event-type", { eventType: type });
            const check = eventChecks.get(type);
            if (check)
              yield* check(event.data).pipe(
                Effect.catch(() => rejectOutput("event-shape", { eventType: type })),
              );
            if (type === "response.completed")
              completed = (yield* parseCompletion(event.data).pipe(
                Effect.catch(() => rejectOutput("completion")),
              )).response;
          }),
        ),
        Effect.mapError((error) =>
          isRestrictedTextJobError(error)
            ? error
            : new RestrictedTextJobError({ reason: "upstream" }),
        ),
      );
      const output = yield* completedText(completed, input.model);
      const outputBytes = new TextEncoder().encode(output).length;
      if (outputBytes > 250_000)
        return yield* new RestrictedTextJobError({ reason: "output_limit" });
      const result = yield* parseResult(output).pipe(Effect.catch(() => rejectOutput("result")));
      return {
        result,
        evidence: {
          instanceId: input.instanceId,
          model: input.model,
          transport: "chatgpt-responses",
          serviceTier: "default",
          tools: "none",
          inputBytes: new TextEncoder().encode(input.input).length,
          outputBytes,
        },
      };
    },
    Effect.scoped,
    Effect.provideService(References.TracerEnabled, false),
    Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
  );
});
