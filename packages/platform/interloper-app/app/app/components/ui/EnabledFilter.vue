<script setup lang="ts">
/** Narrow a list by its enabled flag. The model is the wanted state, null for all. */

/** Sentinel for "no filter" — an empty value would clear the select. */
const ALL = '__all__'

const enabled = defineModel<boolean | null>({ required: true })

const items = [
    { label: 'All states', value: ALL },
    { label: 'Enabled', value: 'enabled' },
    { label: 'Disabled', value: 'disabled' },
]
</script>

<template>
    <USelect :model-value="enabled === null ? ALL : enabled ? 'enabled' : 'disabled'"
             :items="items"
             value-key="value"
             icon="i-lucide-power"
             class="w-40"
             @update:model-value="enabled = $event === ALL ? null : $event === 'enabled'" />
</template>
