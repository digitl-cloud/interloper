<script setup lang="ts">
/**
 * A status as a dot, where a badge does not fit. Give it a `status` to read
 * the status table, or a `color` (and `pulse`) for a state mapped elsewhere,
 * such as a graph node's.
 */
const props = withDefaults(defineProps<{
    status?: string
    color?: StatusColor
    pulse?: boolean
}>(), { status: undefined, color: undefined, pulse: false })

const meta = computed(() => statusMeta(props.status))
const color = computed(() => props.color ?? meta.value.color)
const pulsing = computed(() => props.pulse || (!props.color && meta.value.executing))
</script>

<template>
    <span class="inline-block size-2 shrink-0 rounded-full"
          :class="[STATUS_BG[color], pulsing && 'animate-pulse']" />
</template>
