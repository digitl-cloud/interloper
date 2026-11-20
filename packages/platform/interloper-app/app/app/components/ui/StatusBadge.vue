<script setup lang="ts">
/**
 * A status as a badge in its colour. `status` takes a run, backfill or
 * execution status from the status table; `color` and `label` state anything
 * else (a renewal, a sync check, a hook's enabled flag). One still executing
 * spins.
 */
const props = defineProps<{
    status?: string
    color?: StatusColor
    label?: string
}>()

const meta = computed(() => props.status
    ? statusMeta(props.status)
    : { label: props.label ?? '', color: props.color ?? 'neutral', executing: false })
const appConfig = useAppConfig()
</script>

<template>
    <UBadge :color="meta.color"
            :icon="meta.executing ? appConfig.ui.icons.loading : undefined"
            :ui="{ leadingIcon: 'animate-spin' }">
        <slot>{{ props.label ?? meta.label }}</slot>
    </UBadge>
</template>
