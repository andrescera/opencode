export * as PluginSourceDirectory from "./source-directory.js"

import { FSUtil } from "@opencode-ai/util/fs-util"
import { Effect, Option } from "effect"
import path from "path"

export const names = ["plugin", "plugins"] as const

export const discover = Effect.fn("PluginSourceDirectory.discover")(function* (
  fs: FSUtil.Interface,
  directory: string,
) {
  const children = (yield* Effect.forEach(names, (source) =>
    fs.readDirectoryEntries(path.join(directory, source)).pipe(
      Effect.orElseSucceed(() => []),
      Effect.map((entries) => entries.map((entry) => ({ ...entry, target: path.join(directory, source, entry.name) }))),
    ),
  ))
    .flat()
    .sort((a, b) => (a.target < b.target ? -1 : a.target > b.target ? 1 : 0))
  const targets = yield* Effect.forEach(children, (entry) =>
    Effect.gen(function* () {
      const source = entry.target.endsWith(".ts") || entry.target.endsWith(".js")
      if (entry.type === "file" && source) return Option.some(entry.target)
      if (entry.type === "directory") return yield* entrypoint(fs, entry.target)
      if (entry.type !== "symlink") return Option.none<string>()
      if (source && (yield* fs.isFile(entry.target))) return Option.some(entry.target)
      if (yield* fs.isDir(entry.target)) return yield* entrypoint(fs, entry.target)
      return Option.none<string>()
    }),
  )
  return targets.flatMap(Option.toArray)
})

/** The directory a plugin entrypoint belongs to, or undefined for a single-file plugin. */
export function root(entrypoint: string) {
  return path.basename(entrypoint).startsWith("index.") ? path.dirname(entrypoint) : undefined
}

/**
 * Revision timestamp for a local plugin: the newest mtime across the directory
 * for a directory plugin, so an edit to any sibling module reloads the whole
 * unit, or the entrypoint's own mtime for a single-file plugin. Vendored trees
 * cannot change the plugin and are skipped.
 */
export const mtime = Effect.fn("PluginSourceDirectory.mtime")(function* (fs: FSUtil.Interface, entrypoint: string) {
  const directory = root(entrypoint)
  const files = directory ? yield* walk(fs, directory) : [entrypoint]
  const times = yield* Effect.forEach(files, (file) =>
    fs.stat(file).pipe(
      Effect.map((info) => Option.getOrElse(info.mtime, () => new Date(0)).getTime()),
      Effect.orElseSucceed(() => 0),
    ),
  )
  return Math.max(0, ...times)
})

const vendored = new Set(["node_modules", ".git"])

// Symlinks are stamped but never followed, so a linked directory cannot loop the walk.
// An unreadable directory contributes nothing rather than failing the revision.
function walk(fs: FSUtil.Interface, directory: string): Effect.Effect<string[]> {
  return fs.readDirectoryEntries(directory).pipe(
    Effect.orElseSucceed(() => []),
    Effect.flatMap((entries) =>
      Effect.forEach(
        entries.filter((entry) => !vendored.has(entry.name)),
        (entry) =>
          entry.type === "directory"
            ? walk(fs, path.join(directory, entry.name))
            : Effect.succeed([path.join(directory, entry.name)]),
      ),
    ),
    Effect.map((nested) => nested.flat()),
  )
}

export function entrypoint(fs: FSUtil.Interface, directory: string) {
  return Effect.gen(function* () {
    const root = yield* fs.resolve(directory)
    return yield* Effect.findFirst(
      ["index.ts", "index.js"].map((entry) => path.join(directory, entry)),
      (entry) =>
        fs
          .isFile(entry)
          .pipe(
            Effect.flatMap((exists) =>
              exists
                ? fs.resolve(entry).pipe(Effect.map((resolved) => FSUtil.contains(root, resolved)))
                : Effect.succeed(false),
            ),
          ),
    )
  })
}
