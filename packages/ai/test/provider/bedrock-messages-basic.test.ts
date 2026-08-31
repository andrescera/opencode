import { EventStreamCodec } from "@smithy/eventstream-codec"
import { fromUtf8, toUtf8 } from "@smithy/util-utf8"
import { expect } from "bun:test"
import { Effect } from "effect"
import { LLM, LLMRequest, Message } from "../../src/index.js"
import { LLMClient, compileRequest } from "../../src/route/client.js"
import { AmazonBedrock } from "../../src/providers/index.js"
import { it, testEffect } from "../lib/effect.js"
import { dynamicResponse, fixedResponse } from "../lib/http.js"

const codec = new EventStreamCodec(toUtf8, fromUtf8)
const response = Buffer.concat(
  [
    { type: "message_start", message: { usage: { input_tokens: 10 } } },
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
    { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Hello" } },
    { type: "content_block_stop", index: 0 },
    { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 2 } },
    { type: "message_stop" },
  ].map((event) =>
    codec.encode({
      headers: {
        ":message-type": { type: "string", value: "event" },
        ":event-type": { type: "string", value: "chunk" },
      },
      body: new TextEncoder().encode(JSON.stringify({ bytes: Buffer.from(JSON.stringify(event)).toString("base64") })),
    }),
  ),
)

for (const auth of [
  { apiKey: "test" },
  { credentials: { accessKeyId: "test", secretAccessKey: "test", region: "us-west-2" } },
]) {
  testEffect(
    dynamicResponse(({ request, text, respond }) =>
      Effect.sync(() => {
        expect(request.url).toBe(
          "https://bedrock-runtime.us-west-2.amazonaws.com/model/anthropic.claude-opus-4-6-v1%3A0/invoke-with-response-stream",
        )
        expect(request.headers.authorization).toStartWith(auth.apiKey ? "Bearer test" : "AWS4-HMAC-SHA256")
        const body = JSON.parse(text)
        expect(body.model).toBeUndefined()
        expect(body.stream).toBeUndefined()
        expect(body.anthropic_version).toBe("bedrock-2023-05-31")
        expect(body.anthropic_beta).toEqual(["existing-beta"])
        if (body.messages.length > 1) expect(body.messages[1].content).toEqual([{ type: "text", text: "Hello" }])
        return respond(response, { headers: { "content-type": "application/vnd.amazon.eventstream" } })
      }),
    ),
  ).effect(`Bedrock Messages text round trip with ${auth.apiKey ? "bearer" : "SigV4"} authentication`, () =>
    Effect.gen(function* () {
      const provider = AmazonBedrock.configure({ ...auth, region: "us-west-2" })
      expect(provider.model("fixture").route.id).toBe("bedrock-converse")
      const request = LLM.request({
        model: provider.messages("anthropic.claude-opus-4-6-v1:0"),
        prompt: "hello",
        http: { headers: { "anthropic-beta": "existing-beta, existing-beta" } },
      })
      const first = yield* LLMClient.generate(request)
      expect(first.text).toBe("Hello")
      expect(first.usage?.totalTokens).toBe(12)
      yield* LLMClient.generate(
        LLMRequest.update(request, {
          messages: [...request.messages, first.message, Message.user("continue")],
        }),
      )
    }),
  )
}

testEffect(
  fixedResponse(
    codec.encode({
      headers: {
        ":message-type": { type: "string", value: "exception" },
        ":exception-type": { type: "string", value: "throttlingException" },
      },
      body: new TextEncoder().encode(JSON.stringify({ message: "Too many requests", trace: "keep-original" })),
    }),
  ),
).effect("Bedrock Messages retains the original exception frame", () =>
  Effect.gen(function* () {
    const error = yield* LLMClient.generate(
      LLM.request({
        model: AmazonBedrock.configure({ apiKey: "test" }).messages("claude"),
        prompt: "hello",
      }),
    ).pipe(Effect.flip)
    expect(error.reason.body).toContain("keep-original")
    expect(error.reason.http?.status).toBe(200)
  }),
)

for (const mediaType of ["image/png", "application/pdf"]) {
  for (const data of ["https://example.com/media", "invalid base64!"]) {
    for (const role of ["user", "tool"] as const) {
      it.effect(`rejects ${role} ${mediaType} with ${data}`, () =>
        Effect.gen(function* () {
          const error = yield* compileRequest(
            LLM.request({
              model: AmazonBedrock.configure({ apiKey: "test" }).messages("claude"),
              messages:
                role === "user"
                  ? [Message.user({ type: "media", mediaType, data })]
                  : [
                      Message.tool({
                        id: "call_1",
                        name: "read",
                        result: { type: "content", value: [{ type: "file", mime: mediaType, uri: data }] },
                      }),
                    ],
            }),
          ).pipe(Effect.flip)
          expect(error.reason._tag).toBe("InvalidRequest")
          expect(error.message).toContain("Bedrock Messages")
        }),
      )
    }
  }
}

it.effect("accepts inline image and document sources", () =>
  Effect.gen(function* () {
    const prepared = yield* compileRequest(
      LLM.request({
        model: AmazonBedrock.configure({ apiKey: "test" }).messages("claude"),
        messages: [
          Message.user([
            { type: "media", mediaType: "image/png", data: "data:image/png;base64,AQID" },
            { type: "media", mediaType: "application/pdf", data: "data:application/pdf;base64,AQID" },
          ]),
        ],
      }),
    )
    expect(prepared.body.messages[0].content.map((block: { source: { type: string } }) => block.source.type)).toEqual([
      "base64",
      "base64",
    ])
  }),
)

it.effect("rejects Anthropic file IDs", () =>
  Effect.gen(function* () {
    const error = yield* compileRequest(
      LLM.request({
        model: AmazonBedrock.configure({ apiKey: "test" }).messages("claude"),
        messages: [Message.user({ type: "media", mediaType: "image/png", data: "", metadata: { file_id: "file_1" } })],
      }),
    ).pipe(Effect.flip)
    expect(error.message).toContain("file-ID")
  }),
)
