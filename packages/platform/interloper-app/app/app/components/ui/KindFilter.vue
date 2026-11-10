<script setup lang="ts">
/** Narrow a component list by kind. The model is the chosen kind, null for all. */
import { capitalize } from 'vue'
import type { ComponentRecord } from '~/types/component'

/** Sentinel for "no filter" — an empty value would clear the select. */
const ALL = '__all__'

const kind = defineModel<string | null>({ required: true })

const props = defineProps<{
    components: Pick<ComponentRecord, 'kind'>[]
}>()

const items = computed(() => [
    { label: 'All kinds', value: ALL },
    ...[...new Set(props.components.map(c => c.kind))].sort().map(value => ({ label: capitalize(value), value })),
])
</script>

<template>
    <USelect :model-value="kind ?? ALL"
             :items="items"
             value-key="value"
             icon="i-lucide-shapes"
             class="w-40"
             @update:model-value="kind = $event === ALL ? null : $event" />
</template>
