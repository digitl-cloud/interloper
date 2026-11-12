<script setup lang="ts">
/** A run, backfill or execution status as a badge, from the status table; one still executing spins. */
const props = defineProps<{
    status: string
}>()

const meta = computed(() => statusMeta(props.status))
const appConfig = useAppConfig()
</script>

<template>
    <UBadge :color="meta.color"
            :label="meta.label"
            :icon="meta.executing ? appConfig.ui.icons.loading : undefined"
            :ui="{ leadingIcon: 'animate-spin' }" />
</template>
