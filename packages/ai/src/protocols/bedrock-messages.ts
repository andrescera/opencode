import { Effect, Encoding, Schema, Struct } from "effect"
import { Headers } from "effect/unstable/http"
import { AIError } from "../schema/index.js"
import { Route } from "../route/client.js"
import { Endpoint } from "../route/endpoint.js"
import { Protocol } from "../route/protocol.js"
import { classifyProviderFailure } from "../provider-error.js"
import { AnthropicMessages } from "./anthropic-messages.js"
import { BedrockEventStream } from "./bedrock-event-stream.js"
import { BedrockAuth } from "./utils/bedrock-auth.js"
import { JsonObject, ProviderShared } from "./shared.js"

const ID = "bedrock-messages"
const VERSION = "bedrock-2023-05-31"
const Body = Schema.Struct({
  ...Struct.omit(AnthropicMessages.AnthropicMessagesBody.fields, ["model", "stream"]),
  anthropic_version: Schema.Literal(VERSION),
  anthropic_beta: Schema.optional(Schema.Array(Schema.String)),
}).check(
  Schema.makeFilter(
    (body) =>
      body.messages.flatMap((message) => message.content.map(mediaIssue)).find((issue) => issue !== undefined) ?? true,
  ),
)
const Event = Schema.Struct({
  chunk: Schema.optional(Schema.Struct({ bytes: Schema.String })),
  exception: Schema.optional(
    Schema.Struct({
      type: Schema.String,
      details: Schema.StructWithRest(
        Schema.Struct({ message: Schema.optional(Schema.String), originalMessage: Schema.optional(Schema.String) }),
        [JsonObject],
      ),
    }),
  ),
})

export const protocol = Protocol.make({
  id: ID,
  body: {
    schema: Body,
    from: Effect.fn("BedrockMessages.fromRequest")(function* (request) {
      const body = yield* AnthropicMessages.protocol.body.from(request)
      const headers = Headers.fromInput(request.http?.headers)
      const betas = new Set(
        (headers["anthropic-beta"] ?? "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
      )
      return {
        ...Struct.omit(body, ["model", "stream"]),
        anthropic_version: VERSION,
        anthropic_beta: betas.size ? [...betas] : undefined,
      } satisfies typeof Body.Type
    }),
  },
  stream: {
    event: Event,
    initial: AnthropicMessages.protocol.stream.initial,
    step: Effect.fn("BedrockMessages.step")(function* (state, event) {
      if (event.exception)
        return yield* new AIError({
          reason: classifyProviderFailure({
            message: event.exception.details.message ?? event.exception.details.originalMessage ?? event.exception.type,
            rawBody: ProviderShared.encodeJson(event),
          }),
        })
      if (!event.chunk) return yield* ProviderShared.eventError(ID, "Bedrock Messages event is missing its chunk")
      const text = yield* Effect.fromResult(Encoding.decodeBase64String(event.chunk.bytes)).pipe(
        Effect.mapError((cause) =>
          ProviderShared.eventError(ID, "Invalid Bedrock Messages chunk encoding", undefined, cause),
        ),
      )
      const decoded = yield* Schema.decodeUnknownEffect(AnthropicMessages.protocol.stream.event)(text).pipe(
        Effect.mapError((cause) => ProviderShared.eventError(ID, "Invalid Bedrock Messages event", undefined, cause)),
      )
      return yield* AnthropicMessages.protocol.stream.step(state, decoded)
    }),
  },
})

function mediaIssue(
  block: AnthropicMessages.AnthropicMessagesBody["messages"][number]["content"][number],
): string | undefined {
  if (block.type === "tool_result")
    return typeof block.content === "string"
      ? undefined
      : block.content.map(mediaIssue).find((issue) => issue !== undefined)
  if (block.type !== "image" && block.type !== "document") return undefined
  if (block.source.type === "url" || block.source.type === "file")
    return "Bedrock Messages does not support URL or file-ID media sources"
  if (
    block.type === "image" &&
    !["image/jpeg", "image/png", "image/webp", "image/gif"].includes(block.source.media_type)
  )
    return "Bedrock Messages requires a JPEG, PNG, WebP, or GIF image"
  if (block.source.type === "base64" && Encoding.decodeBase64(block.source.data)._tag === "Failure")
    return "Bedrock Messages media data must be valid base64"
  return undefined
}

export const route = Route.make({
  id: ID,
  provider: "amazon-bedrock",
  providerMetadataKey: "anthropic",
  protocol,
  endpoint: Endpoint.path(
    ({ request }) => `/model/${encodeURIComponent(request.model.id)}/invoke-with-response-stream`,
    { baseURL: "https://bedrock-runtime.us-east-1.amazonaws.com" },
  ),
  auth: BedrockAuth.auth,
  framing: BedrockEventStream.framing(ID),
})

export * as BedrockMessages from "./bedrock-messages.js"
