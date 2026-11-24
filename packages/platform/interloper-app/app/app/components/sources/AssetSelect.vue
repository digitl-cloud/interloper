<script setup lang="ts">
/**
 * Checkbox multi-select for choosing which assets to enable on a source.
 *
 * Upstreams are not chosen here: an asset reading another source's assets
 * follows every matching asset of the organisation, so there is nothing to wire.
 */
import type { SourceDefinition } from '~/types/catalog'

const selectedKeys = defineModel<string[]>('selectedKeys', { default: () => [] })

const props = defineProps<{
    /** The source definition whose assets to display. */
    sourceDefn: SourceDefinition
}>()

const appConfig = useAppConfig()

// ── Filters ─────────────────────────────────────────────────────────

const tagFilter = ref('all')
const search = ref('')

const tagTabs = computed(() => [
    { label: 'All', value: 'all' },
    ...[...new Set(props.sourceDefn.assets.flatMap(a => a.tags))].map(tag => ({ label: tag, value: tag })),
])

/** Assets left by the tag tabs and the display-name/key search. */
const filteredAssets = computed(() => {
    const query = search.value.trim().toLowerCase()
    return props.sourceDefn.assets.filter(asset =>
        (tagFilter.value === 'all' || asset.tags.includes(tagFilter.value))
        && (!query
            || asset.key.toLowerCase().includes(query)
            || asset.name.toLowerCase().includes(query)),
    )
})

// ── Selection ───────────────────────────────────────────────────────

function toggle(key: string) {
    const idx = selectedKeys.value.indexOf(key)
    if (idx >= 0) selectedKeys.value.splice(idx, 1)
    else selectedKeys.value.push(key)
}

/** Select/deselect operate on the visible (filtered) assets only. */
function selectAll() {
    selectedKeys.value = [...new Set([...selectedKeys.value, ...filteredAssets.value.map(a => a.key)])]
}

function deselectAll() {
    const visible = new Set(filteredAssets.value.map(a => a.key))
    selectedKeys.value = selectedKeys.value.filter(k => !visible.has(k))
}
</script>

<template>
    <div class="flex flex-col gap-6">
        <div class="flex flex-col gap-3">
            <FilterBar :active="Number(tagFilter !== 'all')"
                       @reset="tagFilter = 'all'">
                <template #search>
                    <UInput v-model="search"
                            placeholder="Search assets…"
                            :icon="appConfig.ui.icons.search"
                            class="w-full sm:w-64" />
                </template>
                <template v-if="tagTabs.length > 2"
                          #default>
                    <FilterBarField label="Tag">
                        <UTabs v-model="tagFilter"
                               :items="tagTabs" />
                    </FilterBarField>
                </template>
            </FilterBar>

            <div class="flex items-center justify-between">
                <span class="text-sm text-muted tabular-nums">
                    {{ selectedKeys.length }} of {{ sourceDefn.assets.length }} assets selected
                </span>
                <div class="flex gap-2">
                    <UButton size="sm"
                             color="neutral"
                             variant="ghost"
                             label="Select all"
                             @click="selectAll()" />
                    <UButton size="sm"
                             color="neutral"
                             variant="ghost"
                             label="Deselect all"
                             @click="deselectAll()" />
                </div>
            </div>
        </div>

        <UEmpty v-if="filteredAssets.length === 0"
                variant="naked"
                size="sm"
                :icon="appConfig.ui.icons.search"
                title="No assets match these filters" />

        <div v-else
             class="flex flex-col gap-2">
            <SelectionCard v-for="asset in filteredAssets"
                           :key="asset.key"
                           as="div"
                           :selected="selectedKeys.includes(asset.key)"
                           :label="asset.name || asset.key"
                           :description="asset.description"
                           @select="toggle(asset.key)">
                <template #leading>
                    <UCheckbox :model-value="selectedKeys.includes(asset.key)"
                               @click.stop
                               @update:model-value="toggle(asset.key)" />
                </template>
                <template v-if="asset.tags.length"
                          #details>
                    <span class="mt-0.5 text-xs text-dimmed">{{ asset.tags.join(', ') }}</span>
                </template>
                <template v-if="asset.partitioning"
                          #trailing>
                    <UTooltip text="This asset is partitioned"
                              :delay-duration="0">
                        <UIcon name="i-lucide-layers"
                               class="size-4 shrink-0 text-muted" />
                    </UTooltip>
                </template>
            </SelectionCard>
        </div>
    </div>
</template>
