import type { Component } from 'vue'
import { LazyAssetsRowsTable, LazyAssetsTableEditor, LazyAssetsUIModal } from '#components'
import { extensionAssetUI } from '~/composables/extensions'
import { qualifiedKey } from '~/types/catalog'
import type { ComponentRecord } from '~/types/component'

/** How an asset shows in the asset UI modal: its tab, the action that opens it, and what the tab renders. */
export interface AssetUITab {
    asset: ComponentRecord
    /** The tab's label: the extension UI's own, else the asset's name. */
    label: string
    /** What the asset pane's button reads: the extension UI's label, "Edit" for an editable asset, "Rows" otherwise. */
    action: string
    icon: string
    /** Rendered with one prop, `context: AssetUIContext`. */
    component: Component
}

/**
 * The UIs an asset opens in, in one modal per source: the UI an extension
 * contributed, else the grid editor of an editable asset, else its row preview.
 */
export function useAssetUI() {
    const catalogStore = useCatalogStore()
    const appConfig = useAppConfig()
    const overlay = useOverlay()

    /** The UI an asset opens in; undefined when it has neither an extension's nor an editor. */
    function uiOf(source: ComponentRecord, asset: ComponentRecord): AssetUITab | undefined {
        const key = qualifiedKey(source.key, asset.key)
        const contributed = extensionAssetUI(key)
        if (contributed) {
            const { label, icon, component } = contributed
            return { asset, label, action: label, icon: icon ?? appConfig.icons.extension, component }
        }
        const definition = catalogStore.getAssetDefinition(key)
        if (definition?.editable)
            return { asset, label: definition.name, action: 'Edit', icon: appConfig.icons.edit, component: LazyAssetsTableEditor }
        return undefined
    }

    /** What an asset opens in: its UI, else its row preview. */
    function tabOf(source: ComponentRecord, asset: ComponentRecord): AssetUITab {
        const name = catalogStore.getAssetDefinition(qualifiedKey(source.key, asset.key))?.name ?? asset.key
        return uiOf(source, asset) ?? { asset, label: name, action: 'Rows', icon: appConfig.icons.rows, component: LazyAssetsRowsTable }
    }

    /**
     * The modal's tabs when it opens on `opened`: every asset of the source
     * that has a UI, in the source definition's order, and `opened` itself,
     * as its row preview when it has none.
     */
    function tabsOf(source: ComponentRecord, opened: ComponentRecord): AssetUITab[] {
        const order = catalogStore.getSourceDefinition(source.key)?.assets.map(definition => definition.key) ?? []
        const rank = (asset: ComponentRecord) => {
            const index = order.indexOf(asset.key)
            return index < 0 ? order.length : index
        }
        return [...source.children]
            .sort((a, b) => rank(a) - rank(b))
            .flatMap((asset) => {
                const tab = asset.id === opened.id ? tabOf(source, asset) : uiOf(source, asset)
                return tab ? [tab] : []
            })
    }

    /** Open the asset UI modal on one asset of a source. */
    function open(source: ComponentRecord, asset: ComponentRecord) {
        overlay.create(LazyAssetsUIModal, { props: { source, asset }, destroyOnClose: true }).open()
    }

    return { uiOf, tabOf, tabsOf, open }
}
