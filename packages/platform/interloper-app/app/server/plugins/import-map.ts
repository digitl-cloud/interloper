/**
 * Name the modules the app shares with extension UIs in the page's import map.
 *
 * They are merged into the map Nuxt writes for its entry rather than added as
 * a second map: a browser that supports only one import map ignores the rest.
 */
import shared from '#interloper/shared-modules'

const IMPORT_MAP = /<script type="importmap">(.*?)<\/script>/s

export default defineNitroPlugin((nitroApp) => {
    nitroApp.hooks.hook('render:html', (html) => {
        const index = html.head.findIndex(chunk => IMPORT_MAP.test(chunk))
        if (index === -1) {
            html.head.unshift(`<script type="importmap">${JSON.stringify({ imports: shared })}</script>`)
            return
        }
        html.head[index] = html.head[index]!.replace(IMPORT_MAP, (_, json: string) => {
            const map = JSON.parse(json)
            return `<script type="importmap">${JSON.stringify({ ...map, imports: { ...shared, ...map.imports } })}</script>`
        })
    })
})
