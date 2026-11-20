<script setup lang="ts">
/**
 * A status as a dot and a word. `status` takes a run, backfill or execution
 * status from the status table; `color` and `label` state anything else (a
 * renewal, a sync check). The normal outcomes read muted and the others in
 * their colour, so the eye lands on what needs it; one still executing pulses.
 */
const props = defineProps<{
    status?: string
    color?: StatusColor
    label?: string
}>()

const meta = computed(() => props.status
    ? statusMeta(props.status)
    : { label: props.label ?? '', color: props.color ?? 'neutral', executing: false })

const TEXT: Record<StatusColor, string> = {
    success: 'text-muted',
    neutral: 'text-muted',
    error: 'text-error',
    warning: 'text-warning',
    info: 'text-info',
}
</script>

<template>
    <span class="inline-flex items-center gap-1.5 text-sm"
          :class="TEXT[meta.color]">
        <StatusDot :color="meta.color"
                   :pulse="meta.executing" />
        <slot>{{ props.label ?? meta.label }}</slot>
    </span>
</template>
