/**
 * Each module name the app shares with extension UIs, and the shim beside this file that serves it.
 * The app's import map names them; the extension SDK keeps them out of every bundle.
 */
export const SHARED_MODULES: Record<string, string> = {
    'vue': 'vue.ts',
    'vue-router': 'vue-router.ts',
    'pinia': 'pinia.ts',
    '@vueuse/core': 'vueuse-core.ts',
    '@iconify/vue': 'iconify-vue.ts',
    '@interloper/host': 'host.ts',
}
