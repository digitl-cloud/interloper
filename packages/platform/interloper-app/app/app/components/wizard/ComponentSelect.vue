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

const organisationStore = useOrganisationStore()
const appConfig = useAppConfig()

const selectedIds = defineModel<string[]>({ default: () => [] })

const props = defineProps<{
    components: ComponentRecord[]
    /** Noun for the counter line, e.g. "watched" / "targeted". */
    noun?: string
}>()

const kindFilter = ref<string | null>(null)
const keyFilter = ref<string | null>(null)

/** Type choices follow the kind in view, so the two filters never contradict. */
const byKind = computed(() => kindFilter.value
    ? props.components.filter(component => component.kind === kindFilter.value)
    : props.components)

watch(kindFilter, () => { keyFilter.value = null })

const filtered = computed(() => byKind.value.filter(component =>
    keyFilter.value === null || component.key === keyFilter.value,
))

const kinds = computed(() => new Set(props.components.map(c => c.kind)).size)
const keys = computed(() => new Set(props.components.map(c => c.key)).size)

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
        <!-- A single kind or type is no choice at all. -->
        <div v-if="kinds > 1 || keys > 1"
             class="flex items-center gap-2">
            <KindFilter v-if="kinds > 1"
                        v-model="kindFilter"
                        :components="components" />
            <TypeFilter v-if="keys > 1"
                        v-model="keyFilter"
                        :components="byKind" />
        </div>

        <div class="flex items-center justify-between">
            <span class="text-sm text-muted tabular-nums">
                {{ selectedIds.length }} of {{ components.length }} components {{ noun ?? 'selected' }}
            </span>
            <div class="flex gap-2">
                <UButton size="sm"
                         color="neutral"
                         variant="ghost"
                         label="Select all"
                         @click="selectVisible()" />
                <UButton size="sm"
                         color="neutral"
                         variant="ghost"
                         label="Deselect all"
                         @click="deselectVisible()" />
            </div>
        </div>

        <UEmpty v-if="components.length === 0"
                variant="subtle"
                size="sm"
                :icon="appConfig.icons.source"
                title="Nothing to pick yet"
                description="No sources, assets or jobs are set up in this organisation."
                :actions="[{ label: 'Go to sources', onClick: () => navigateTo(organisationStore.path(kindPath('source'))) }]" />

        <UEmpty v-else-if="filtered.length === 0"
                variant="naked"
                size="sm"
                :icon="appConfig.ui.icons.search"
                title="No components match these filters" />

        <div v-else
             class="flex flex-col gap-2">
            <SelectionCard v-for="component in filtered"
                           :key="component.id"
                           :selected="selectedIds.includes(component.id)"
                           :label="component.name ?? component.key"
                           :description="component.key"
                           :icon="componentIcon(component.key)"
                           @select="toggle(component.id)">
                <template #leading>
                    <UCheckbox :model-value="selectedIds.includes(component.id)"
                               @click.stop
                               @update:model-value="toggle(component.id)" />
                </template>
                <template #trailing>
                    <UBadge color="neutral"
                            size="sm"
                            :label="capitalize(component.kind)" />
                </template>
            </SelectionCard>
        </div>
    </div>
</template>
