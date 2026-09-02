import { Script, constants } from "node:vm"
import { createRequire, registerHooks } from "node:module"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { resolve, type Package } from "resolve.exports"

let conditions: readonly string[] = []
const conditionHooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    conditions = context.conditions
    return nextResolve(specifier, context)
  },
})
await new Script('import("node:module")', {
  importModuleDynamically: constants.USE_MAIN_CONTEXT_DEFAULT_LOADER,
}).runInThisContext()
conditionHooks.deregister()

// A query on a file: import makes Node evaluate a fresh copy of that file, but
// the file's relative imports still resolve to their cached originals. Carry the
// importer's query down so a re-imported entrypoint re-evaluates its whole local
// module graph, not just its own file. Bare specifiers are untouched.
registerHooks({
  resolve(specifier, context, nextResolve) {
    const result = nextResolve(specifier, context)
    if (!context.parentURL || !/^\.\.?\//.test(specifier) || !result.url.startsWith("file:")) return result
    const search = new URL(context.parentURL).search
    return search ? { ...result, url: `${result.url}${search}` } : result
  },
})

export async function importModule(specifier: string) {
  const imported = (await new Script(`import(${JSON.stringify(specifier)})`, {
    importModuleDynamically: constants.USE_MAIN_CONTEXT_DEFAULT_LOADER,
  }).runInThisContext()) as unknown
  if (typeof imported !== "object" || imported === null) return imported

  const module = imported as Record<string, unknown>
  const exports = module["module.exports"]
  if (exports !== module.default || (typeof exports !== "object" && typeof exports !== "function") || exports === null)
    return imported
  return Object.assign({}, module, exports)
}

export function resolveModule(specifier: string, directory: string) {
  const pkg = createRequire(import.meta.url)(path.join(directory, "package.json")) as Package
  const target = resolve(pkg, specifier, { conditions, unsafe: true })?.[0]
  if (target) return pathToFileURL(path.resolve(directory, target)).href
  const legacyTarget =
    specifier === pkg.name ? directory : path.resolve(directory, specifier.slice(pkg.name.length + 1))
  return pathToFileURL(createRequire(path.join(directory, "package.json")).resolve(legacyTarget)).href
}
