import {
  RestrictedTextJobError,
  RestrictedTextJobInput,
  type RestrictedTextJobResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { HttpClient } from "effect/http";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";

export class RestrictedTextJobs extends Context.Service<
  RestrictedTextJobs,
  {
    readonly run: (
      input: RestrictedTextJobInput,
    ) => Effect.Effect<RestrictedTextJobResult, RestrictedTextJobError>;
  }
>()("t3/textGeneration/RestrictedTextJobs") {}

const decodeInput = Schema.decodeEffect(RestrictedTextJobInput);

const make = Effect.gen(function* () {
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const slots = yield* Semaphore.make(2);
  // Span exits retain successful values, so jobs suppress tracing across the entire operation.
  const run = Effect.fnUntraced(
    function* (input: RestrictedTextJobInput) {
      const request = yield* decodeInput(input, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => new RestrictedTextJobError({ reason: "invalid_input" })));
      if (
        new TextEncoder().encode(request.instructions).length > 32_768 ||
        new TextEncoder().encode(request.input).length > 2_097_152
      ) {
        return yield* new RestrictedTextJobError({ reason: "invalid_input" });
      }
      const instance = yield* registry.getInstance(request.instanceId);
      if (!instance?.enabled || !instance.restrictedTextJob) {
        return yield* new RestrictedTextJobError({ reason: "unsupported_provider" });
      }
      return yield* instance.restrictedTextJob(request);
    },
    Effect.timeout("90 seconds"),
    Effect.catchTags({ TimeoutError: () => new RestrictedTextJobError({ reason: "timeout" }) }),
    slots.withPermitsIfAvailable(1),
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.fail(new RestrictedTextJobError({ reason: "busy" })),
        onSome: Effect.succeed,
      }),
    ),
    Effect.provideService(References.TracerEnabled, false),
    Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
  );
  return RestrictedTextJobs.of({ run });
});

export const layer = Layer.effect(RestrictedTextJobs, make);
