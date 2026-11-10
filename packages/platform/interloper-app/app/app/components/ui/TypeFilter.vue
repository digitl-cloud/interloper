<script setup lang="ts">
/**
 * Narrow a component list by type: the catalog display name behind each key
 * present in `components`. The model is the chosen key, null for all.
 */
import type { ComponentRecord } from '~/types/component'

/** Sentinel for "no filter" — an empty value would clear the select. */
const ALL = '__all__'

const key = defineModel<string | null>({ required: true })

const props = defineProps<{
    components: Pick<ComponentRecord, 'kind' | 'key'>[]
}>()

const items = computed(() => {
    const names = new Map<string, string>()
    for (const component of props.components) names.set(component.key, componentTypeName(component.kind, component.key))
    return [
        { label: 'All types', value: ALL },
        ...[...names.entries()]
            .sort(([, a], [, b]) => a.localeCompare(b))
            .map(([value, label]) => ({ label, value })),
    ]
})
</script>

<template>
    <USelectMenu :model-value="key ?? ALL"
                 :items="items"
                 value-key="value"
                 icon="i-lucide-tag"
                 :search-input="{ placeholder: 'Search types...' }"
                 class="w-56"
                 @update:model-value="key = $event === ALL ? null : $event" />
</template>
