<script setup lang="ts">
import type { RunEvent } from '~/stores/events'

const props = defineProps<{
    event: RunEvent
}>()

const emit = defineEmits<{
    close: []
}>()

const title = computed(() => `Error in ${props.event.component_key ?? 'an unknown component'}`)

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
                <div v-if="event.error">
                    <h4 class="caption mb-1 text-muted">
                        Error
                    </h4>
                    <pre class="max-h-40 overflow-x-auto whitespace-pre rounded-lg bg-error/10 p-3 font-mono text-sm text-error">{{ event.error }}</pre>
                </div>
                <div v-if="event.traceback">
                    <h4 class="caption mb-1 text-muted">
                        Traceback
                    </h4>
                    <div class="max-h-96 overflow-auto rounded-lg text-xs [&_pre]:!p-3 [&_pre]:!m-0 [&_pre]:!min-w-fit [&_code]:!text-xs"
                         v-html="tracebackHtml" />
                </div>
            </div>
        </template>
    </UModal>
</template>
