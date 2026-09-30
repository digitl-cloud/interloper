<script setup lang="ts">
/**
 * A stretch of a turn's work, folded behind one row: the reasoning and tool
 * calls between two things the user is shown (an answer, a card, an
 * approval). Closed by default; the row says which step is in progress
 * while the turn runs and how much was done once it is over.
 */
import type { UIMessage } from 'ai'
import { getToolName, isReasoningUIPart, isToolUIPart } from 'ai'
import { isPartStreaming, isToolStreaming } from '@nuxt/ui/utils/ai'

type Part = UIMessage['parts'][number]

const props = defineProps<{ parts: Part[], live?: boolean }>()

const tools = computed(() => props.parts.filter(isToolUIPart))
const failed = computed(() => tools.value.filter(part => part.state === 'output-error' || _failed(part.output)).length)

const text = computed(() => {
    if (props.live) {
        const last = props.parts[props.parts.length - 1]
        if (last && isToolUIPart(last)) return toolLabel(getToolName(last), isToolStreaming(last))
        return 'Thinking...'
    }
    const count = tools.value.length
    return count ? `${count} step${count === 1 ? '' : 's'}` : 'Thought'
})

const suffix = computed(() => !props.live && failed.value ? `${failed.value} failed` : undefined)
const icon = computed(() => failed.value && !props.live ? 'i-lucide-triangle-alert' : 'i-lucide-list-checks')

function _failed(output: unknown) {
    return typeof output === 'object' && output !== null && (output as { status?: string }).status === 'error'
}
</script>

<template>
    <UChatTool :text="text"
               :suffix="suffix"
               :icon="icon"
               :loading="live"
               :streaming="live"
               chevron="leading"
               class="w-full"
               :ui="failed && !live ? { leadingIcon: 'text-error' } : undefined">
        <div class="flex flex-col items-start gap-1 w-full">
            <template v-for="(part, index) in parts"
                      :key="index">
                <UChatReasoning v-if="isReasoningUIPart(part)"
                                :text="part.text"
                                :streaming="isPartStreaming(part)"
                                chevron="leading"
                                class="w-full">
                    <Markdown :value="part.text"
                              :streaming="isPartStreaming(part)"
                              class="*:first:mt-0 *:last:mb-0" />
                </UChatReasoning>
                <AgentTool v-else-if="isToolUIPart(part)"
                           :part="part" />
            </template>
        </div>
    </UChatTool>
</template>
