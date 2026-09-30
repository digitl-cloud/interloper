<script setup lang="ts">
/**
 * One tool call of a turn, as a `UChatTool`: what it is doing while it runs,
 * what it did once done, Approve/Deny while the agent waits for the user, and
 * the payloads it exchanged behind the chevron.
 */
import type { DynamicToolUIPart, ToolUIPart } from 'ai'
import { getToolName } from 'ai'
import { isToolApprovalPending, isToolStreaming } from '@nuxt/ui/utils/ai'

const props = defineProps<{ part: ToolUIPart | DynamicToolUIPart }>()

const emit = defineEmits<{
    approve: [id: string, approved: boolean]
}>()

/** Cap the payload preview: a catalog listing runs to hundreds of kilobytes. */
const PREVIEW_LIMIT = 4000

const name = computed(() => getToolName(props.part))
const running = computed(() => isToolStreaming(props.part))
const pending = computed(() => isToolApprovalPending(props.part))
const failed = computed(() => props.part.state === 'output-error' || _failed(props.part.output))
const denied = computed(() => props.part.state === 'output-denied')

const text = computed(() => {
    if (pending.value) return `Approve: ${toolLabel(name.value, true).toLowerCase()}?`
    if (denied.value) return `${toolLabel(name.value, false)} — denied`
    return toolLabel(name.value, running.value)
})

const icon = computed(() => failed.value ? 'i-lucide-triangle-alert' : pending.value ? 'i-lucide-shield-question' : 'i-lucide-wrench')

const actions = computed(() => {
    if (!pending.value || props.part.state !== 'approval-requested') return undefined
    const id = props.part.approval.id
    return [
        { label: 'Approve', onClick: () => emit('approve', id, true) },
        { label: 'Deny', color: 'neutral' as const, variant: 'ghost' as const, onClick: () => emit('approve', id, false) },
    ]
})

function preview(payload: unknown) {
    const json = JSON.stringify(payload, null, 2)
    return json.length > PREVIEW_LIMIT ? `${json.slice(0, PREVIEW_LIMIT)}\n…` : json
}

function _failed(output: unknown) {
    return typeof output === 'object' && output !== null && (output as { status?: string }).status === 'error'
}
</script>

<template>
    <UChatTool :text="text"
               :icon="icon"
               :streaming="running"
               :actions="actions"
               :variant="pending ? 'card' : 'inline'"
               chevron="leading"
               class="w-full"
               :ui="failed ? { leadingIcon: 'text-error' } : undefined">
        <div class="flex flex-col gap-2">
            <div v-if="part.input !== undefined">
                <div class="eyebrow text-dimmed mb-1">Input</div>
                <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(part.input) }}</pre>
            </div>
            <div v-if="part.output !== undefined">
                <div class="eyebrow text-dimmed mb-1">Output</div>
                <pre class="overflow-x-auto rounded-md bg-elevated/50 p-2 text-[11px]/4 font-mono text-toned">{{ preview(part.output) }}</pre>
            </div>
            <p v-if="part.state === 'output-error'"
               class="text-error">
                {{ part.errorText }}
            </p>
        </div>
    </UChatTool>
</template>
