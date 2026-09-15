<script setup lang="ts">
/**
 * Dead-air cue for the stretches where the agent is working with nothing yet
 * to show: waiting on a model turn, or between a tool's result and the next
 * move. Steps that are themselves in flight carry their own spinner.
 */
</script>

<template>
    <div class="flex items-center gap-2 text-muted">
        <UIcon name="i-lucide-loader-circle"
               class="size-3.5 shrink-0 animate-spin" />
        <UChatShimmer text="Thinking…"
                      class="text-[13px]" />
    </div>
</template>
