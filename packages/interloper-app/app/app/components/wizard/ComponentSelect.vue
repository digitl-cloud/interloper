<script setup lang="ts">
/**
 * Checkbox multi-select over components of mixed kinds (sources, assets,
 * jobs) — the relation pickers of the hook and job wizards.
 *
 * Long candidate lists narrow by kind and by type (the catalog display name
 * behind a component's key); the bulk buttons act on what the filters leave
 * visible, so selections made under another filter survive.
 */
import { capitalize } from 'vue'
import type { ComponentRecord } from '~/types/component'

const selectedIds = defineModel<string[]>({ default: () => [] })

const props = defineProps<{
    components: ComponentRecord[]
    /** Noun for the counter line, e.g. "watched" / "targeted". */
    noun?: string
}>()

const catalogStore = useCatalogStore()

/** Sentinel for "no filter" — an empty value would clear the select. */
const ALL = '__all__'

const kindFilter = ref(ALL)
const keyFilter = ref(ALL)

/** Display name of a component's type (source-owned assets live under their source). */
function typeName(component: ComponentRecord): string {
    const defn = component.kind === 'asset'
        ? catalogStore.getAssetDefinition(component.key)
        : catalogStore.catalog[component.key]
    return defn?.name ?? component.key
}

const kindItems = computed(() => [
    { label: 'All kinds', value: ALL },
    ...[...new Set(props.components.map(c => c.kind))].sort()
        .map(kind => ({ label: capitalize(kind), value: kind })),
])

/** Type choices for the kind in view, so the two filters never contradict. */
const keyItems = computed(() => {
    const names = new Map<string, string>()
    for (const component of props.components) {
        if (kindFilter.value !== ALL && component.kind !== kindFilter.value) continue
        names.set(component.key, typeName(component))
    }
    return [
        { label: 'All types', value: ALL },
        ...[...names.entries()]
            .sort(([, a], [, b]) => a.localeCompare(b))
            .map(([key, label]) => ({ label, value: key })),
    ]
})

watch(kindFilter, () => { keyFilter.value = ALL })

const filtered = computed(() => props.components.filter(component =>
    (kindFilter.value === ALL || component.kind === kindFilter.value)
    && (keyFilter.value === ALL || component.key === keyFilter.value),
))

/** One choice plus the sentinel is no choice at all. */
const showFilters = computed(() => kindItems.value.length > 2 || keyItems.value.length > 2)

function toggle(id: string) {
    const idx = selectedIds.value.indexOf(id)
    if (idx >= 0) selectedIds.value.splice(idx, 1)
    else selectedIds.value.push(id)
}

function selectVisible() {
    selectedIds.value = [...new Set([...selectedIds.value, ...filtered.value.map(c => c.id)])]
}

function deselectVisible() {
    const visible = new Set(filtered.value.map(c => c.id))
    selectedIds.value = selectedIds.value.filter(id => !visible.has(id))
}
</script>

<template>
    <div class="flex flex-col gap-3">
        <div v-if="showFilters && components.length"
             class="flex items-center gap-2">
            <USelect v-if="kindItems.length > 2"
                     v-model="kindFilter"
                     :items="kindItems"
                     value-key="value"
                     size="sm"
                     icon="i-lucide-shapes"
                     class="w-40" />
            <USelectMenu v-if="keyItems.length > 2"
                         v-model="keyFilter"
                         :items="keyItems"
                         value-key="value"
                         size="sm"
                         icon="i-lucide-tag"
                         :search-input="{ placeholder: 'Search types...' }"
                         class="w-56" />
        </div>

        <div class="flex items-center justify-between">
            <span class="text-sm text-muted">
                {{ selectedIds.length }} of {{ components.length }} components {{ noun ?? 'selected' }}
            </span>
            <div class="flex gap-2">
                <UButton size="xs"
                         variant="ghost"
                         label="Select all"
                         @click="selectVisible()" />
                <UButton size="xs"
                         variant="ghost"
                         label="Deselect all"
                         @click="deselectVisible()" />
            </div>
        </div>

        <InlineEmptyState v-if="components.length === 0"
                          icon="i-lucide-plug"
                          message="No sources, assets or jobs configured yet."
                          action-label="Go to Sources"
                          @action="navigateTo('/sources')" />

        <div v-else-if="filtered.length === 0"
             class="flex items-center justify-center rounded-md p-6 text-sm text-muted">
            No components match these filters.
        </div>

        <div v-else
             class="flex flex-col gap-2.5">
            <SelectionCard v-for="component in filtered"
                           :key="component.id"
                           :selected="selectedIds.includes(component.id)"
                           class="flex items-center gap-3 px-4 py-3"
                           @select="toggle(component.id)">
                <UCheckbox :model-value="selectedIds.includes(component.id)"
                           @click.stop
                           @update:model-value="toggle(component.id)" />
                <div class="size-10 shrink-0 rounded-lg border border-default bg-default flex items-center justify-center">
                    <UIcon :name="componentIcon(component.key)"
                           class="size-6" />
                </div>
                <div class="flex flex-col min-w-0">
                    <span class="text-[14.5px] font-semibold text-highlighted truncate">{{ component.name }}</span>
                    <span class="text-xs text-dimmed truncate">{{ component.key }}</span>
                </div>
                <UBadge color="neutral"
                        size="xs"
                        class="ml-auto capitalize">
                    {{ component.kind }}
                </UBadge>
            </SelectionCard>
        </div>
    </div>
</template>
