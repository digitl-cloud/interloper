<script setup lang="ts">
/** A run or backfill status as a badge; one still executing spins. */
const props = defineProps<{
    status: string
}>()

const executing = computed(() => props.status === 'running' || props.status === 'dispatched')
</script>

<template>
    <UBadge :color="statusColor(status)"
            :icon="executing ? 'i-lucide-loader-circle' : undefined"
            :ui="{ leadingIcon: 'animate-spin' }">
        {{ statusLabel(status) }}
    </UBadge>
</template>
