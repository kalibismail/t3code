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
  part: Schema.Struct({ type: Schema.Literals(["output_text", "reasoning_text"]) }),
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
        return yield* new RestrictedTextJobError({ reason: "sign_in_required" });
      const models = yield* chatGptModels(credentials.accessToken, []).pipe(
        Effect.provideService(HttpClient.HttpClient, rawHttp),
        Effect.mapError(() => new RestrictedTextJobError({ reason: "model_unavailable" })),
      );
      if (!models.some((model) => model.slug === input.model))
        return yield* new RestrictedTextJobError({ reason: "model_unavailable" });
      const response = yield* http
        .execute(
          HttpClientRequest.post("https://api.openai.com/v1/responses").pipe(
            HttpClientRequest.bearerToken(credentials.accessToken),
            HttpClientRequest.bodyJsonUnsafe({
              model: input.model,
              instructions: input.instructions,
              input: [{ role: "user", content: [{ type: "input_text", text: input.input }] }],
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
        return yield* new RestrictedTextJobError({ reason: "invalid_output" });
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
            if (completed) return yield* new RestrictedTextJobError({ reason: "invalid_output" });
            const envelope = yield* parseEvent(event.data).pipe(
              Effect.mapError(() => new RestrictedTextJobError({ reason: "invalid_output" })),
            );
            if (envelope.type === "response.failed") {
              const limited = yield* isUsageFailure(event.data).pipe(
                Effect.match({ onSuccess: () => true, onFailure: () => false }),
              );
              return yield* new RestrictedTextJobError({
                reason: limited ? "usage_limit" : "upstream",
              });
            }
            if (!allowedEvents.has(envelope.type))
              return yield* new RestrictedTextJobError({ reason: "invalid_output" });
            if (
              envelope.type === "response.output_item.added" ||
              envelope.type === "response.output_item.done"
            ) {
              yield* parseOutput(event.data).pipe(
                Effect.mapError(() => new RestrictedTextJobError({ reason: "invalid_output" })),
              );
            }
            if (
              envelope.type === "response.content_part.added" ||
              envelope.type === "response.content_part.done"
            ) {
              yield* parseContent(event.data).pipe(
                Effect.mapError(() => new RestrictedTextJobError({ reason: "invalid_output" })),
              );
            }
            if (envelope.type === "response.completed") {
              completed = (yield* parseCompletion(event.data).pipe(
                Effect.mapError(() => new RestrictedTextJobError({ reason: "invalid_output" })),
              )).response;
            }
          }),
        ),
        Effect.mapError((error) =>
          isRestrictedTextJobError(error)
            ? error
            : new RestrictedTextJobError({ reason: "upstream" }),
        ),
      );
      if (!completed || completed.model !== input.model)
        return yield* new RestrictedTextJobError({ reason: "invalid_output" });
      const output = completed.output
        .flatMap((item) => (item.type === "message" ? item.content.map((part) => part.text) : []))
        .join("");
      const outputBytes = new TextEncoder().encode(output).length;
      if (outputBytes > 250_000)
        return yield* new RestrictedTextJobError({ reason: "output_limit" });
      const result = yield* parseResult(output).pipe(
        Effect.mapError(() => new RestrictedTextJobError({ reason: "invalid_output" })),
      );
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
