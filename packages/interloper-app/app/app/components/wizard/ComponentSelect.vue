<script setup lang="ts">
/**
 * Checkbox multi-select over components of mixed kinds (sources, assets,
 * jobs) — the relation pickers of the hook and job wizards.
 *
 * Long candidate lists narrow by kind and by type (the catalog display name
 * behind a component's key); the bulk buttons act on what the filters leave
 * visible, so selections made under another filter survive.
 */
import type { ComponentRecord } from '~/types/component'

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
                          @action="navigateTo(kindPath('source'))" />

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
