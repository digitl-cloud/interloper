/**
 * The modules the app shares with extension UIs, named in the page's import map.
 *
 * Each shared module is a shim under `app/extensions/shared/` re-exporting
 * what the app itself runs. The client build emits every shim as a chunk of
 * its own that keeps all its exports, so the chunk hands an extension the very
 * instances the app uses: one Vue, one router, one Pinia, one icon store. In
 * dev the dev server serves the shims' sources. The map naming them reaches the
 * page through `server/plugins/import-map.ts`.
 */
import { realpathSync } from 'node:fs'
import { posix } from 'node:path'
import {
    addServerTemplate,
    addTypeTemplate,
    addVitePlugin,
    createResolver,
    defineNuxtModule,
    tryResolveModule,
} from 'nuxt/kit'

const TEMPLATE = '#interloper/shared-modules'

/** Each module name an extension imports, and the shim that serves it. */
const SHARED: Record<string, string> = {
    'vue': 'vue.ts',
    'vue-router': 'vue-router.ts',
    'pinia': 'pinia.ts',
    '@vueuse/core': 'vueuse-core.ts',
    '@iconify/vue': 'iconify-vue.ts',
    '@interloper/host': 'host.ts',
}

export default defineNuxtModule({
    meta: { name: 'extensions' },
    async setup(_options, nuxt) {
        await assertOneIconStore(nuxt.options.rootDir)

        const resolver = createResolver(import.meta.url)
        const shims = Object.fromEntries(
            Object.entries(SHARED).map(([name, file]) => [name, resolver.resolve(`../app/extensions/shared/${file}`)]),
        )
        const imports: Record<string, string> = {}

        if (nuxt.options.dev) {
            for (const [name, shim] of Object.entries(shims))
                imports[name] = posix.join(nuxt.options.app.buildAssetsDir, '@fs', shim)
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
        addServerTemplate({ filename: TEMPLATE, getContents: () => `export default ${JSON.stringify(imports)}` })
        addTypeTemplate({
            filename: 'types/interloper-shared-modules.d.ts',
            getContents: () => `declare module '${TEMPLATE}' {\n    const imports: Record<string, string>\n    export default imports\n}\n`,
        }, { nitro: true })
    },
})

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
