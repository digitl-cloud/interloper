<script setup lang="ts">
/** Design status pill: tinted rounded-full chip with a currentColor dot. */
const props = withDefaults(defineProps<{
    label: string
    color?: 'success' | 'warning' | 'error' | 'neutral' | 'primary'
    /** Hide the leading dot (e.g. for counts). */
    dot?: boolean
}>(), { color: 'neutral', dot: true })

const COLOR_CLASSES: Record<NonNullable<typeof props.color>, string> = {
    success: 'bg-green-500/10 text-green-700 dark:bg-green-400/10 dark:text-green-400',
    warning: 'bg-amber-500/15 text-amber-700 dark:bg-amber-400/10 dark:text-amber-400',
    error: 'bg-red-500/10 text-red-700 dark:bg-red-400/10 dark:text-red-400',
    neutral: 'bg-elevated text-muted',
    primary: 'bg-primary/10 text-primary',
}
</script>

<template>
    <span class="inline-flex items-center gap-1.5 px-2.5 py-[3px] rounded-full text-[11px] font-semibold"
          :class="COLOR_CLASSES[color]">
        <span v-if="dot"
              class="size-1.5 rounded-full bg-current" />
        {{ label }}
    </span>
</template>
