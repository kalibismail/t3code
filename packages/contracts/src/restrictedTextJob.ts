import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as SchemaIssue from "effect/SchemaIssue";
import { ProviderInstanceId } from "./providerInstance.ts";

export const RestrictedTextJobInput = Schema.Struct({
  instanceId: ProviderInstanceId,
  model: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  serviceTier: Schema.Literal("default"),
  instructions: Schema.NonEmptyString.check(Schema.isMaxLength(32_768)),
  input: Schema.NonEmptyString.check(Schema.isMaxLength(131_072)),
}).pipe(
  Schema.middlewareDecoding((effect) =>
    effect.pipe(
      Effect.mapError(
        () => new SchemaIssue.InvalidValue({ message: "Invalid restricted text job input." }),
      ),
    ),
  ),
);
export type RestrictedTextJobInput = typeof RestrictedTextJobInput.Type;

export const RestrictedTextJobResult = Schema.Struct({
  result: Schema.JsonObject,
  evidence: Schema.Struct({
    instanceId: ProviderInstanceId,
    model: Schema.String,
    transport: Schema.Literal("chatgpt-responses"),
    serviceTier: Schema.Literal("default"),
    tools: Schema.Literal("none"),
    inputBytes: Schema.Int,
    outputBytes: Schema.Int,
  }),
});
export type RestrictedTextJobResult = typeof RestrictedTextJobResult.Type;

export class RestrictedTextJobError extends Schema.TaggedError<RestrictedTextJobError>()(
  "RestrictedTextJobError",
  {
    reason: Schema.Literals([
      "invalid_input",
      "unsupported_provider",
      "sign_in_required",
      "model_unavailable",
      "busy",
      "timeout",
      "upstream",
      "usage_limit",
      "invalid_output",
      "output_limit",
    ]),
  },
) {
  override get message(): string {
    return `Restricted text job failed: ${this.reason}.`;
  }
}
