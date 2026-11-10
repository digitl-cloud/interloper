<script setup lang="ts">
/** Narrow a runs or backfills list by status. The model is the chosen status, null for all. */

/** Sentinel for "no filter": an empty value would clear the select. */
const ALL = '__all__'

const status = defineModel<string | null>({ required: true })

const props = defineProps<{
    statuses: string[]
}>()

const items = computed(() => [
    { label: 'All statuses', value: ALL },
    ...props.statuses.map(value => ({ label: statusLabel(value), value })),
])
</script>

<template>
    <USelect :model-value="status ?? ALL"
             :items="items"
             value-key="value"
             icon="i-lucide-activity"
             class="w-40"
             @update:model-value="status = $event === ALL ? null : $event" />
</template>
