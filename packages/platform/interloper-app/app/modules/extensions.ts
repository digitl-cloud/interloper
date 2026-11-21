/**
 * The modules the app shares with extension UIs, named in the page's import map.
 *
 * Each shared module is a shim under `app/extensions/shared/` re-exporting
 * what the app itself runs. The client build emits every shim as a chunk of
 * its own that keeps all its exports, so the chunk hands an extension the very
 * instances the app uses: one Vue, one router, one Pinia, one icon store. In
 * dev the dev server serves the shims' sources. The map naming them reaches the
 * page through `server/plugins/import-map.ts`.
 *
 * In dev, `interloper dev` starts this server with `INTERLOPER_UI_DEV` (`<key>=<entry
 * URL>`, comma-separated) naming the Vite dev server of each extension UI it serves,
 * which the page then loads instead of its bundle. That server rewrites every shared
 * name to `<origin>/@id/<name>`, so the map names those URLs too. The variable is
 * that process's channel, not a setting: builds ignore it, so no deployment imports
 * code from elsewhere.
 */
import { realpathSync } from 'node:fs'
import { posix } from 'node:path'
import {
    addServerTemplate,
    addTemplate,
    addTypeTemplate,
    addVitePlugin,
    createResolver,
    defineNuxtModule,
    tryResolveModule,
} from 'nuxt/kit'
import { SHARED_MODULES } from '../app/extensions/shared/modules'

const IMPORT_MAP = '#interloper/shared-modules'
const DEV_ENTRIES = 'interloper/dev-entries.mjs'

export default defineNuxtModule({
    meta: { name: 'extensions' },
    async setup(_options, nuxt) {
        await assertOneIconStore(nuxt.options.rootDir)

        const resolver = createResolver(import.meta.url)
        const shims = Object.fromEntries(
            Object.entries(SHARED_MODULES).map(([name, file]) => [name, resolver.resolve(`../app/extensions/shared/${file}`)]),
        )
        const imports: Record<string, string> = {}
        const devEntries = nuxt.options.dev ? parseDevEntries(process.env.INTERLOPER_UI_DEV ?? '') : {}

        if (nuxt.options.dev) {
            const origins = Object.values(devEntries).map(entry => new URL(entry).origin)
            for (const [name, shim] of Object.entries(shims)) {
                const url = posix.join(nuxt.options.app.buildAssetsDir, '@fs', shim)
                imports[name] = url
                for (const origin of origins) imports[`${origin}/@id/${name}`] = url
            }
        }

        addVitePlugin({
            name: 'interloper:shared-modules',
            apply: 'build',
            buildStart() {
                for (const [name, shim] of Object.entries(shims))
                    this.emitFile({ type: 'chunk', id: shim, name: `shared-${name}`, preserveSignature: 'strict' })
            },
            generateBundle(_, bundle) {
                for (const chunk of Object.values(bundle)) {
                    if (chunk.type !== 'chunk' || !chunk.facadeModuleId) continue
                    const name = Object.keys(shims).find(key => shims[key] === chunk.facadeModuleId)
                    if (name) imports[name] = posix.join(nuxt.options.app.baseURL, chunk.fileName)
                }
            },
        }, { server: false })

        // Rendered when Nitro builds, after the client build has named the chunks.
        addServerTemplate({ filename: IMPORT_MAP, getContents: () => `export default ${JSON.stringify(imports)}` })
        addTypeTemplate({
            filename: 'types/interloper-shared-modules.d.ts',
            getContents: () => `declare module '${IMPORT_MAP}' {\n    const imports: Record<string, string>\n    export default imports\n}\n`,
        }, { nitro: true })

        addTemplate({ filename: DEV_ENTRIES, getContents: () => `export default ${JSON.stringify(devEntries)}` })
        addTypeTemplate({
            filename: 'types/interloper-dev-entries.d.ts',
            getContents: () => `declare module '#build/${DEV_ENTRIES}' {\n    const entries: Record<string, string>\n    export default entries\n}\n`,
        })
    },
})

/**
 * Read `INTERLOPER_UI_DEV`: each extension under development and the URL of its entry module.
 *
 * @param value The variable's value, `<key>=<entry URL>` pairs separated by commas.
 * @returns The entry URL of each extension key.
 */
function parseDevEntries(value: string): Record<string, string> {
    const entries: Record<string, string> = {}
    for (const pair of value.split(',').map(pair => pair.trim()).filter(Boolean)) {
        const [key, entry] = pair.split(/=(.*)/s)
        if (!key || !entry || !URL.canParse(entry))
            throw new Error(`INTERLOPER_UI_DEV: '${pair}' is not <key>=<entry URL>, such as acme=http://localhost:5173/src/index.ts`)
        entries[key] = entry
    }
    return entries
}

/**
 * Refuse to build when the app's `@iconify/vue` is not the one Nuxt Icon renders from: an
 * icon an extension registers through the shared module would land in a store no `UIcon` reads.
 *
 * @param rootDir The app's root, where its own dependencies resolve.
 */
async function assertOneIconStore(rootDir: string): Promise<void> {
    const nuxtUI = await tryResolveModule('@nuxt/ui', rootDir)
    const nuxtIcon = nuxtUI && await tryResolveModule('@nuxt/icon', nuxtUI)
    const iconsOfNuxtIcon = nuxtIcon && await tryResolveModule('@iconify/vue', nuxtIcon)
    const iconsOfApp = await tryResolveModule('@iconify/vue', rootDir)
    if (!iconsOfNuxtIcon || !iconsOfApp || realpathSync(iconsOfNuxtIcon) !== realpathSync(iconsOfApp)) {
        throw new Error(
            `The app's @iconify/vue (${iconsOfApp}) is not the one Nuxt Icon uses (${iconsOfNuxtIcon}): `
            + 'align the version in package.json with @nuxt/icon\'s.',
        )
    }
}
