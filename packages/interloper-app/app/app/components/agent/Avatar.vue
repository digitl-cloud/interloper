<script setup lang="ts">
/**
 * The assistant's mark beside a message: the dot matrix at work while the
 * turn is still running, the sparkles once it has answered.
 */
defineProps<{ live?: boolean }>()
</script>

<template>
    <span class="flex items-center justify-center size-4 shrink-0 text-primary">
        <AgentIndicator v-if="live" />
        <UIcon v-else
               name="i-lucide-sparkles"
               class="size-4" />
    </span>
</template>
