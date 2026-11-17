/**
 * The Vite configuration an extension's UI builds with: `export default interloper({ outDir })`.
 *
 * - The modules the app shares stay out of the bundle, in `vite build` and in the dev
 *   server alike, so the page's import map resolves them to the app's own instances.
 * - Templates use the components `@interloper/host` exports (`UButton`, `AppNavbar`, ...)
 *   without importing them, as the app's do.
 * - Tailwind compiles against the app's theme (`@import "@interloper/ui/tailwind.css"`).
 * - The bundle comes with a Vite manifest and relative chunk URLs, so the extension's
 *   Python package declares it with `UIBundle.from_vite(<outDir>)`.
 */
import { existsSync, readFileSync } from 'node:fs'
import tailwindcss from '@tailwindcss/vite'
import vue from '@vitejs/plugin-vue'
import components from 'unplugin-vue-components/vite'
import type { Plugin, UserConfig } from 'vite'

const HOST = new URL('../dist/host.json', import.meta.url)

export interface InterloperOptions {
    /** Where the bundle goes: a directory of the extension's Python package, emptied on each build. */
    outDir: string
    /** The entry module, whose default export is `defineExtension({ setup })`. */
    entry?: string
}

/**
 * The Vite configuration of an extension's UI.
 *
 * @param options Where the bundle goes, and the entry module when it is not `src/index.ts`.
 * @returns A configuration to export as is, or to extend with Vite's `mergeConfig`.
 */
export function interloper({ outDir, entry = 'src/index.ts' }: InterloperOptions): UserConfig {
    if (!existsSync(HOST))
        throw new Error('@interloper/ui is not built: run `pnpm --filter @interloper/ui build`')
    const host: { modules: string[], components: string[] } = JSON.parse(readFileSync(HOST, 'utf8'))
    const hostComponents = new Set(host.components)

    return {
        base: './',
        plugins: [
            sharedModules(host.modules),
            vue(),
            tailwindcss(),
            components({
                dts: false,
                resolvers: [name => hostComponents.has(name) ? { name, from: '@interloper/host' } : undefined],
            }),
        ],
        optimizeDeps: { exclude: host.modules },
        build: {
            outDir,
            emptyOutDir: true,
            manifest: true,
            rolldownOptions: { input: entry, preserveEntrySignatures: 'strict' },
        },
    }
}

/**
 * Leave the shared modules to the page's import map. The dev server still rewrites
 * each to `<origin>/@id/<name>`, a URL the app maps to the same module in dev.
 *
 * @param modules The module names the app shares.
 * @returns The plugin.
 */
function sharedModules(modules: string[]): Plugin {
    return {
        name: 'interloper:shared-modules',
        enforce: 'pre',
        resolveId: id => modules.includes(id) ? { id, external: true } : undefined,
    }
}
