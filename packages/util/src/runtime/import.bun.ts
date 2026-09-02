import path from "node:path"

// A query on a plain-path import makes Bun evaluate a fresh copy of that file,
// but the file's relative imports still resolve to their cached originals. Carry
// the importer's query down so a re-imported entrypoint re-evaluates its whole
// local module graph, not just its own file. Bare specifiers are untouched.
Bun.plugin({
  name: "opencode-import-query",
  setup(build) {
    build.onResolve({ filter: /^\.\.?\// }, (args) => {
      const index = args.importer.indexOf("?")
      if (index === -1) return undefined
      const resolved = resolveRelative(args.path, path.dirname(args.importer.slice(0, index)))
      return resolved ? { path: `${resolved}${args.importer.slice(index)}` } : undefined
    })
  },
})

// Fall through to Bun's own resolution (and its error message) when the import is unresolvable.
function resolveRelative(specifier: string, directory: string) {
  try {
    return Bun.resolveSync(specifier, directory)
  } catch {
    return undefined
  }
}

export function importModule(specifier: string) {
  return import(specifier) as Promise<unknown>
}

export function resolveModule(specifier: string, directory: string) {
  return import.meta.resolve(specifier, directory)
}
