import { describe, expect } from "bun:test"
import { Deferred, Effect, Exit, Fiber, Schema, Scope, Stream } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Location } from "@opencode-ai/core/location"
import { Rpc } from "@opencode-ai/core/rpc"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { tempLocationLayer } from "./fixture/location"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Rpc.node]), [Location.node.replace(tempLocationLayer)]))

const definition = Rpc.define({
  id: "registration-test",
  methods: { value: { input: Schema.Void, output: Schema.Number } },
  events: { updated: { schema: Schema.Struct({ value: Schema.Number }) } },
})

// Check the retaining owner directly rather than depending on GC timing.
function finalizers(scope: Scope.Scope) {
  const state = scope.state
  if (state._tag !== "Open") return 0
  return state.finalizer !== undefined ? 1 : (state.finalizers?.size ?? 0)
}

describe("RPC registration lifetime", () => {
  it.effect("explicit disposal releases the owning scope finalizer", () =>
    Effect.gen(function* () {
      const rpc = yield* Rpc.Service
      const parent = yield* Scope.Scope
      const owner = yield* Scope.fork(parent)
      const baseline = finalizers(owner)
      const client = rpc.client(definition)

      yield* Effect.forEach(
        Array.from({ length: 32 }, (_, index) => index),
        (index) =>
          Effect.gen(function* () {
            const registration = yield* rpc
              .register(definition, { value: () => Effect.succeed(index) })
              .pipe(Scope.provide(owner))
            expect(finalizers(owner)).toBe(baseline + 1)
            expect(yield* client.value(undefined)).toBe(index)

            yield* registration.dispose
            yield* registration.dispose
            expect(yield* Effect.flip(client.value(undefined))).toMatchObject({ type: "rpc.unavailable" })
            expect(finalizers(owner)).toBe(baseline)
          }),
        { discard: true },
      )
      expect(owner.state._tag).toBe("Open")
    }),
  )

  it.effect("closing the owner removes registrations that were not explicitly disposed", () =>
    Effect.gen(function* () {
      const rpc = yield* Rpc.Service
      const parent = yield* Scope.Scope
      const owner = yield* Scope.fork(parent)
      const registration = yield* rpc
        .register(definition, { value: () => Effect.succeed(1) })
        .pipe(Scope.provide(owner))
      const client = rpc.client(definition)
      expect(yield* client.value(undefined)).toBe(1)

      yield* Scope.close(owner, Exit.void)
      expect(yield* Effect.flip(client.value(undefined))).toMatchObject({ type: "rpc.unavailable" })
      yield* registration.dispose
      expect(finalizers(owner)).toBe(0)
    }),
  )

  it.effect("disposal preserves overrides and cannot remove a newer registration", () =>
    Effect.gen(function* () {
      const rpc = yield* Rpc.Service
      const client = rpc.client(definition)
      const first = yield* rpc.register(definition, { value: () => Effect.succeed(1) })
      const second = yield* rpc.register(definition, { value: () => Effect.succeed(2) })
      expect(yield* client.value(undefined)).toBe(2)

      yield* second.dispose
      expect(yield* client.value(undefined)).toBe(1)
      const third = yield* rpc.register(definition, { value: () => Effect.succeed(3) })
      yield* second.dispose
      yield* first.dispose
      expect(yield* client.value(undefined)).toBe(3)
      yield* third.dispose
      expect(yield* Effect.flip(client.value(undefined))).toMatchObject({ type: "rpc.unavailable" })
    }),
  )

  it.effect("closing an override owner reveals the previous registration", () =>
    Effect.gen(function* () {
      const rpc = yield* Rpc.Service
      const parent = yield* Scope.Scope
      const owner = yield* Scope.fork(parent)
      yield* rpc.register(definition, { value: () => Effect.succeed(1) })
      yield* rpc.register(definition, { value: () => Effect.succeed(2) }).pipe(Scope.provide(owner))
      expect(yield* rpc.client(definition).value(undefined)).toBe(2)

      yield* Scope.close(owner, Exit.void)
      expect(yield* rpc.client(definition).value(undefined)).toBe(1)
    }),
  )

  it.effect("registration in an already closed owner cannot remain active", () =>
    Effect.gen(function* () {
      const rpc = yield* Rpc.Service
      const parent = yield* Scope.Scope
      const owner = yield* Scope.fork(parent)
      yield* Scope.close(owner, Exit.void)
      const registration = yield* rpc
        .register(definition, { value: () => Effect.succeed(1) })
        .pipe(Scope.provide(owner))

      expect(yield* Effect.flip(rpc.client(definition).value(undefined))).toMatchObject({ type: "rpc.unavailable" })
      yield* registration.dispose
      expect(finalizers(owner)).toBe(0)
    }),
  )

  it.effect("disposal does not interrupt a call already executing in its caller's scope", () =>
    Effect.gen(function* () {
      const rpc = yield* Rpc.Service
      const started = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const registration = yield* rpc.register(definition, {
        value: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)), Effect.as(1)),
      })
      const client = rpc.client(definition)
      const call = yield* client.value(undefined).pipe(Effect.forkScoped)
      yield* Deferred.await(started)
      yield* registration.dispose
      expect(yield* Effect.flip(client.value(undefined))).toMatchObject({ type: "rpc.unavailable" })

      yield* Deferred.succeed(release, undefined)
      expect(yield* Fiber.join(call)).toBe(1)
    }),
  )

  it.effect("registration disposal preserves existing event emitter behavior", () =>
    Effect.gen(function* () {
      const rpc = yield* Rpc.Service
      const registration = yield* rpc.register(definition, { value: () => Effect.succeed(1) })
      const received = yield* rpc
        .client(definition)
        .events.subscribe("updated")
        .pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped)
      yield* Effect.yieldNow
      yield* registration.events.emit("updated", { value: 1 })
      yield* registration.dispose
      yield* registration.events.emit("updated", { value: 2 })
      expect((yield* Fiber.join(received)).map((event) => event.data.value)).toEqual([1, 2])
    }),
  )
})
