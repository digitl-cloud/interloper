import { readFileSync } from 'node:fs'

// The enclosing python package carries the single PSR-bumped workspace
// version; absent (standalone SPA builds), the version label simply hides.
let appVersion = ''
try {
    appVersion = readFileSync(new URL('../pyproject.toml', import.meta.url), 'utf8')
        .match(/^version = "([^"]+)"/m)?.[1] ?? ''
}
catch { /* noop */ }

export default defineNuxtConfig({
    compatibilityDate: '2025-07-15',
    components: [
        // Generic, domain-free primitives keep their short names (DataTable,
        // EmptyState, …) instead of gaining a `Ui` path prefix.
        { path: '~/components/ui', pathPrefix: false },
        '~/components',
    ],
    css: [
        '~/assets/css/main.css',
        '@vue-flow/core/dist/style.css',
        '@vue-flow/core/dist/theme-default.css',
        '@vue-flow/controls/dist/style.css',
        '@vue-flow/minimap/dist/style.css',
    ],
    devtools: {
        enabled: true
    },
    googleFonts: {
        families: {
            Inter: {
                wght: [100, 200, 300, 400, 500, 600, 700, 800, 900],
                ital: [100, 200, 300, 400, 500, 600, 700, 800, 900],
            },
            'JetBrains Mono': {
                wght: [400, 500, 600],
            },
        },
        display: 'swap',
        prefetch: true,
        preconnect: true,
    },
    icon: {
        serverBundle: {
            collections: ['uil', 'mdi', 'logos'],
        },
        customCollections: [
            {
                prefix: 'icon',
                dir: './assets/icons',
            },
        ],
    },
    imports: {
        dirs: [
            'stores/**',
            'types/**',
        ],
    },
    modules: [
        '@nuxt/eslint',
        '@nuxt/ui',
        '@nuxtjs/google-fonts',
        '@nuxtjs/mdc',
        '@pinia/nuxt',
        '@vueuse/nuxt',
    ],
    nitro: {
        preset: process.env.NUXT_PRESET || 'node-server',
    },
    runtimeConfig: {
        public: {
            devApiPort: process.env.INTERLOPER_API_PORT || '',
            version: appVersion,
        },
    },
    ssr: false,
    typescript: {
        nodeTsConfig: {
            compilerOptions: {
                types: ['node'],
            },
        },
    },
    devServer: {
        port: Number(process.env.INTERLOPER_SERVER_PORT) || 3000,
    },
    vite: {
        server: {
            proxy: {
                '/api': {
                    target: `http://localhost:${process.env.INTERLOPER_API_PORT || 3000}`,
                    changeOrigin: true,
                    autoRewrite: true,
                },
            },
        },
    },
})