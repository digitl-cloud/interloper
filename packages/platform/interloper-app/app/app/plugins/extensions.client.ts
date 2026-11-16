import type { Extension } from '~/composables/extensions'

/** An installed package as `GET /api/extensions` lists it; `ui` is set when it ships one that loaded. */
interface InstalledExtension {
    key: string | null
    name: string
    ui: { entry: string, css: string[] } | null
}

/**
 * Load the UI each installed extension ships, before the first navigation, so
 * a deep link into an extension's page resolves on a reload. The entries load
 * in parallel and are set up in the listing's order, so their sidebar entries
 * keep one order. An extension that fails to load is reported and skipped:
 * the app and the other extensions carry on. Signed out, the listing answers
 * 401 and no extension loads until the next page load after signing in.
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

        const withUI = installed.filter(extension => extension.key && extension.ui)
        const modules = await Promise.allSettled(withUI.map((extension) => {
            for (const href of extension.ui!.css)
                document.head.append(Object.assign(document.createElement('link'), { rel: 'stylesheet', href }))
            return import(/* @vite-ignore */ extension.ui!.entry)
        }))

        for (const [index, extension] of withUI.entries()) {
            const loaded = modules[index]!
            try {
                if (loaded.status === 'rejected') throw loaded.reason
                const definition = loaded.value.default as Extension | undefined
                if (typeof definition?.setup !== 'function')
                    throw new Error('its entry module has no defineExtension({ setup }) as default export')
                await definition.setup(createExtensionHost(extension.key!, router))
            }
            catch (error) {
                console.error(`Extension '${extension.key}' failed to load`, error)
                toast.add(errorToast(error, `Couldn't load the ${extension.name || extension.key} extension`))
            }
        }
    },
})
