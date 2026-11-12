<script setup lang="ts">
/**
 * Reference chip for an entity (connection, destination, job, asset, …).
 * Entities use the `outline` variant so they read as object references,
 * distinct from the `soft` badges (the theme default) of statuses and tags.
 */
defineProps<{
    icon?: string
    label: string
    /** Number of additional entities, rendered as a "+N" suffix. */
    extra?: number
}>()
</script>

<template>
    <UBadge color="neutral"
            variant="outline"
            :icon="icon">
        {{ label }}{{ extra ? ` +${extra}` : '' }}
    </UBadge>
</template>
