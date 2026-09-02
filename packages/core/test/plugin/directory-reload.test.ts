import { describe, expect, setDefaultTimeout } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Duration, Effect, Layer, LayerMap, Schedule } from "effect"
import { Event } from "@opencode-ai/schema/config"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/util/effect/layer-node"
import { Global } from "@opencode-ai/util/global"
import { Bus } from "@opencode-ai/core/bus"
import { Command } from "@opencode-ai/core/command"
import { Database } from "@opencode-ai/core/database/database"
import { Watcher } from "@opencode-ai/core/filesystem/watcher"
import { Instance } from "@opencode-ai/core/instance"
import { LocationServiceMap } from "@opencode-ai/core/location-services"
import { Location } from "@opencode-ai/core/location"
import { Plugin } from "@opencode-ai/core/plugin"
import { SdkPlugins } from "@opencode-ai/core/plugin/sdk"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { tempGlobalLayer } from "../fixture/global"
import { tmpdirScoped } from "../fixture/tmpdir"
import { testEffect } from "../lib/effect"

// Real Location boot with plugin-directory discovery, so directory plugins are loaded and reloaded from disk.
setDefaultTimeout(15_000)

const watcher = Watcher.testLayer

const instances = Layer.effect(
  LocationServiceMap.Service,
  Effect.gen(function* () {
    const map = yield* LayerMap.make((ref: Location.Ref) => Instance.layer(ref, { replacements: bindings }), {
      idleTimeToLive: Duration.infinity,
    })
    const bindings: LayerNode.Replacements = [
      Global.node.replace(tempGlobalLayer),
      Watcher.node.replace(watcher),
      LocationServiceMap.node.replace(Layer.succeed(LocationServiceMap.Service, map)),
      Instance.node.replace(
        Layer.succeed(Instance.Service, {
          provide: (session) => Effect.provide(map.get(session.location)),
        }),
      ),
    ]
    return map
  }),
)

const it = testEffect(
  Layer.merge(
    AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SdkPlugins.node, LocationServiceMap.node]), [
      Global.node.replace(tempGlobalLayer),
      LocationServiceMap.node.replace(instances),
    ]),
    watcher,
  ),
)

// The entrypoint never changes; only the sibling module it imports does.
const index = `import { description } from "./greeting.ts"
export default {
  id: "acme",
  async setup(ctx) {
    await ctx.command.transform((editor) => editor.add({ name: "acme", description, execute: async () => {} }))
  },
}`

const greeting = (description: string) => `export const description = ${JSON.stringify(description)}\n`

// Local plugin revisions key on mtime, so give each write a distinct timestamp.
const write = (file: string, content: string, mtime: Date) =>
  Effect.promise(async () => {
    await Bun.write(file, content)
    await fs.utimes(file, mtime, mtime)
  })

// Reloads do filesystem work after the publish returns, so poll for the outcome.
const described = (commands: Command.Interface, description: string) =>
  commands.get("acme").pipe(
    Effect.flatMap((command) =>
      command?.description === description ? Effect.succeed(command) : Effect.fail("not reloaded"),
    ),
    Effect.retry({ times: 200, schedule: Schedule.spaced("25 millis") }),
  )

describe("directory plugin reload", () => {
  it.live("reloads a discovered directory plugin when only a sibling module changes", () =>
    Effect.gen(function* () {
      const directory = yield* tmpdirScoped()
      const plugin = path.join(directory.path, ".opencode/plugins/acme")
      const past = new Date(Date.now() - 60_000)
      yield* write(path.join(plugin, "index.ts"), index, past)
      yield* write(path.join(plugin, "greeting.ts"), greeting("Greets v1"), past)
      const bus = yield* Bus.Service
      const locations = yield* LocationServiceMap.Service
      yield* Effect.gen(function* () {
        const plugins = yield* Plugin.Service
        const commands = yield* Command.Service
        yield* plugins.awaitActivation
        expect(yield* commands.get("acme")).toMatchObject({ description: "Greets v1" })

        yield* write(path.join(plugin, "greeting.ts"), greeting("Greets v2"), new Date())
        yield* bus.publish(Event.Updated, {})

        expect(yield* described(commands, "Greets v2")).toMatchObject({ description: "Greets v2" })
      }).pipe(
        Effect.scoped,
        Effect.provide(locations.get(Location.Ref.make({ directory: AbsolutePath.make(directory.path) }))),
      )
    }),
  )

  it.live("watches a configured plugin directory outside config roots as one unit", () =>
    Effect.gen(function* () {
      const directory = yield* tmpdirScoped()
      const plugin = path.join(directory.path, "tools/acme")
      const past = new Date(Date.now() - 60_000)
      yield* write(path.join(plugin, "index.ts"), index, past)
      yield* write(path.join(plugin, "greeting.ts"), greeting("Greets v1"), past)
      // Relative plugin paths resolve against the config file's directory.
      yield* Effect.promise(() =>
        Bun.write(path.join(directory.path, "opencode.json"), JSON.stringify({ plugins: ["./tools/acme"] })),
      )
      const locations = yield* LocationServiceMap.Service
      const watches = yield* Watcher.Test
      yield* Effect.gen(function* () {
        const plugins = yield* Plugin.Service
        const commands = yield* Command.Service
        yield* plugins.awaitActivation
        expect(yield* commands.get("acme")).toMatchObject({ description: "Greets v1" })
        expect(yield* watches.subscriptions()).toContainEqual({
          path: plugin,
          type: "directory",
          ignore: [...Watcher.vendored].toSorted(),
        })

        yield* write(path.join(plugin, "greeting.ts"), greeting("Greets v2"), new Date())
        yield* watches.emit({ type: "update", path: path.join(plugin, "greeting.ts") })

        expect(yield* described(commands, "Greets v2")).toMatchObject({ description: "Greets v2" })
      }).pipe(
        Effect.scoped,
        Effect.provide(locations.get(Location.Ref.make({ directory: AbsolutePath.make(directory.path) }))),
      )
    }),
  )
})
