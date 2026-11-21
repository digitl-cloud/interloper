import devEntries from '#build/interloper/dev-entries.mjs'
import type { Extension } from '~/composables/extensions'

/** An installed package as `GET /api/extensions` lists it; `ui` is set when it ships one that loaded. */
interface InstalledExtension {
    key: string | null
    name: string
    ui: { entry: string, css: string[] } | null
}

/** An extension's UI to load: its entry module and the stylesheets to add first. */
interface ExtensionUI {
    key: string
    name: string
    entry: string
    css: string[]
}

/**
 * Load the UI each installed extension ships, before the first navigation, so
 * a deep link into an extension's page resolves on a reload. The entries load
 * in parallel and are set up in the listing's order, so their sidebar entries
 * keep one order. An extension that fails to load is reported and skipped:
 * the app and the other extensions carry on. Signed out, the listing answers
 * 401 and no extension loads until the next page load after signing in.
 *
 * Under `interloper dev`, an extension whose UI it serves loads from that UI's
 * Vite dev server instead of its bundle, whether or not a bundle was built; that
 * server adds the extension's styles itself.
 */
export default defineNuxtPlugin({
    name: 'extensions',
    async setup() {
        const router = useRouter()
        const toast = useToast()

        let installed: InstalledExtension[]
        try {
            installed = await $fetch<InstalledExtension[]>('/api/extensions', { credentials: 'include' })
        }
        catch (error) {
            if ((error as { response?: { status?: number } }).response?.status !== 401)
                toast.add(errorToast(error, 'Couldn\'t load the extensions'))
            return
        }

        const uis = installed.flatMap(({ key, name, ui }): ExtensionUI[] => {
            if (!key) return []
            const devEntry = devEntries[key]
            if (devEntry) return [{ key, name, entry: devEntry, css: [] }]
            return ui ? [{ key, name, ...ui }] : []
        })
        const modules = await Promise.allSettled(uis.map((ui) => {
            for (const href of ui.css)
                document.head.append(Object.assign(document.createElement('link'), { rel: 'stylesheet', href }))
            return import(/* @vite-ignore */ ui.entry)
        }))

        for (const [index, ui] of uis.entries()) {
            const loaded = modules[index]!
            try {
                if (loaded.status === 'rejected') throw loaded.reason
                const definition = loaded.value.default as Extension | undefined
                if (typeof definition?.setup !== 'function')
                    throw new Error('its entry module has no defineExtension({ setup }) as default export')
                await definition.setup(createExtensionHost(ui.key, router))
            }
            catch (error) {
                console.error(`Extension '${ui.key}' failed to load`, error)
                toast.add(errorToast(error, `Couldn't load the ${ui.name || ui.key} extension`))
            }
        }
    },
})
