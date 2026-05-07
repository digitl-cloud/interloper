<script setup lang="ts">
import type { RunEvent } from '~/stores/events'

const props = defineProps<{
    event: RunEvent
}>()

const emit = defineEmits<{
    close: []
}>()

const title = computed(() => {
    const key = props.event.asset_key ?? 'Unknown'
    return `Error: ${key}`
})

const { html: tracebackHtml, highlight } = useHighlighter()

watch(() => props.event.traceback, (tb) => {
    if (tb) highlight(tb, 'python')
}, { immediate: true })
</script>

<template>
    <UModal :title="title"
            :close="{ onClick: () => emit('close') }"
            :ui="{ content: 'sm:max-w-4xl' }">
        <template #body>
            <div class="space-y-4">
                <div>
                    <h4 class="text-xs font-medium text-muted uppercase mb-1">
                        Error
                    </h4>
                    <pre class="whitespace-pre overflow-x-auto text-sm text-error bg-error/5 rounded-md p-3 font-mono max-h-40">{{ event.error }}</pre>
                </div>
                <div v-if="event.traceback">
                    <h4 class="text-xs font-medium text-muted uppercase mb-1">
                        Traceback
                    </h4>
                    <div class="rounded-md overflow-auto max-h-96 text-xs [&_pre]:!p-3 [&_pre]:!m-0 [&_pre]:!min-w-fit [&_code]:!text-xs"
                         v-html="tracebackHtml" />
                </div>
            </div>
        </template>
    </UModal>
</template>
