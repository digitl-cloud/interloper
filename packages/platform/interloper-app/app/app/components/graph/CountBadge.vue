<script setup lang="ts">
/** Round asset-count pill on the right of a group or source card. */
defineProps<{
    count: number
    /** Filled pill for top-level cards; sources nested in a group use the muted one. */
    emphasis?: boolean
}>()
</script>

<template>
    <span class="flex shrink-0 items-center justify-center rounded-full font-bold tabular-nums"
          :class="emphasis
              ? 'h-[22px] min-w-[22px] bg-[var(--ui-text-dimmed)] px-[7px] text-[11px] text-white'
              : 'h-5 min-w-5 bg-accented px-1.5 text-[10.5px] text-muted'">
        {{ count }}
    </span>
</template>
