<script setup lang="ts">
import type { AssetUIContext } from '~/composables/extensions'
import type { ComponentRecord } from '~/types/component'

/**
 * A source's asset UIs, near full screen, opened on one asset: a tab for every
 * asset of the source that has a UI, and the opened asset's row preview when
 * it has none. Each tab renders its component with the `AssetUIContext` of its
 * asset, and keeps its state while another tab shows.
 */
const props = defineProps<{
    source: ComponentRecord
    asset: ComponentRecord
}>()

const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const canRun = useCanRun()
const { tabsOf } = useAssetUI()

/** The source as the store holds it now, so a change while the modal is open shows. */
const source = computed(() => componentsStore.byId(props.source.id) ?? props.source)
const title = computed(() => source.value.name ?? catalogStore.getSourceDefinition(source.value.key)?.name ?? source.value.key)
const tabs = computed(() => tabsOf(source.value, props.asset))
const items = computed(() => tabs.value.map(tab => ({ label: tab.label, value: tab.asset.id })))

const selectedId = ref(props.asset.id)
const selected = computed(() => tabs.value.find(tab => tab.asset.id === selectedId.value) ?? tabs.value[0])

const context = computed<AssetUIContext | undefined>(() => selected.value && {
    asset: selected.value.asset,
    source: source.value,
    assets: Object.fromEntries(source.value.children.map(asset => [asset.key, asset])),
    canEdit: canRun.value,
})
</script>

<template>
    <UModal :title="title"
            :description="tabs.length === 1 ? tabs[0]!.label : undefined"
            :ui="{ content: 'sm:max-w-7xl h-full', body: 'flex flex-col gap-4' }">
        <template #body>
            <UTabs v-if="tabs.length > 1"
                   v-model="selectedId"
                   :items="items"
                   :content="false"
                   variant="pill"
                   size="sm"
                   class="self-start" />
            <KeepAlive>
                <component :is="selected.component"
                           v-if="selected && context"
                           :key="selected.asset.id"
                           :context="context" />
            </KeepAlive>
        </template>
    </UModal>
</template>
