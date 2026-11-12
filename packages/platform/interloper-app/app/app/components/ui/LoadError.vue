<script setup lang="ts">
/**
 * A load that failed: what couldn't load, the server's reason, and a Retry
 * when the page can try again. The one way a page reports a failed fetch.
 */
const props = defineProps<{
    /** What failed, as the alert's title: "Couldn't load members". */
    title: string
    error: unknown
    onRetry?: () => void
}>()

const appConfig = useAppConfig()

const actions = computed(() => props.onRetry
    ? [{ label: 'Retry', icon: appConfig.ui.icons.reload, color: 'neutral' as const, variant: 'outline' as const, onClick: props.onRetry }]
    : undefined)
</script>

<template>
    <UAlert color="error"
            :icon="appConfig.ui.icons.error"
            :title="title"
            :description="errorDetail(error) ?? GENERIC_ERROR"
            :actions="actions" />
</template>
