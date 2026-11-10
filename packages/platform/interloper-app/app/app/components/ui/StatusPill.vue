<script setup lang="ts">
/** Design status pill: tinted rounded-full chip with a currentColor dot. */
const props = withDefaults(defineProps<{
    label: string
    color?: 'success' | 'warning' | 'error' | 'neutral' | 'primary'
    /** Hide the leading dot (e.g. for counts). */
    dot?: boolean
    /** Replace the dot with a spinner (the status is still executing). */
    spinner?: boolean
}>(), { color: 'neutral', dot: true, spinner: false })

const COLOR_CLASSES: Record<NonNullable<typeof props.color>, string> = {
    success: 'bg-emerald-500/10 text-emerald-700 dark:bg-emerald-400/10 dark:text-emerald-400',
    warning: 'bg-amber-500/15 text-amber-700 dark:bg-amber-400/10 dark:text-amber-400',
    error: 'bg-red-500/10 text-red-700 dark:bg-red-400/10 dark:text-red-400',
    neutral: 'bg-elevated text-muted',
    primary: 'bg-primary/10 text-primary',
}
</script>

<template>
    <span class="inline-flex items-center gap-1.5 px-2 py-[3px] rounded-md text-xs font-medium"
          :class="COLOR_CLASSES[color]">
        <UIcon v-if="spinner"
               name="i-lucide-loader-circle"
               class="size-3 animate-spin" />
        <span v-else-if="dot"
              class="size-1.5 rounded-full bg-current" />
        {{ label }}
    </span>
</template>
