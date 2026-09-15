<script setup lang="ts">
/**
 * Dead-air cue for the stretches where the agent is working with no activity
 * list to say so: the wait before the first tool call, and a turn that only
 * ever answers. Same pairing the Nuxt UI chat template uses for its own.
 */
</script>

<template>
    <div class="flex items-center gap-1.5 text-muted">
        <AgentIndicator />
        <UChatShimmer text="Thinking..."
                      class="text-sm" />
    </div>
</template>
