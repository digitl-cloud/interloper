export default defineNuxtConfig({
    compatibilityDate: '2025-07-15',
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
            }
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
            oauth: {
                amazonClientId: process.env.AMAZON_CLIENT_ID,
                amazonRedirectUri: process.env.AMAZON_REDIRECT_URI,
                criteoClientId: process.env.CRITEO_CLIENT_ID,
                criteoRedirectUri: process.env.CRITEO_REDIRECT_URI,
                facebookClientId: process.env.FACEBOOK_CLIENT_ID,
                facebookRedirectUri: process.env.FACEBOOK_REDIRECT_URI,
                googleClientId: process.env.GOOGLE_CLIENT_ID,
                googleRedirectUri: process.env.GOOGLE_REDIRECT_URI,
                linkedinClientId: process.env.LINKEDIN_CLIENT_ID,
                linkedinRedirectUri: process.env.LINKEDIN_REDIRECT_URI,
                microsoftClientId: process.env.MICROSOFT_CLIENT_ID,
                microsoftRedirectUri: process.env.MICROSOFT_REDIRECT_URI,
                pinterestClientId: process.env.PINTEREST_CLIENT_ID,
                pinterestRedirectUri: process.env.PINTEREST_REDIRECT_URI,
                snapchatClientId: process.env.SNAPCHAT_CLIENT_ID,
                snapchatRedirectUri: process.env.SNAPCHAT_REDIRECT_URI,
                tiktokClientId: process.env.TIKTOK_CLIENT_ID,
                tiktokRedirectUri: process.env.TIKTOK_REDIRECT_URI,
            },
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