<script setup lang="ts">
/**
 * A stretch of a turn's work, folded behind one row: the reasoning and tool
 * calls between two things the user is shown (an answer, a card, an
 * approval). Closed by default; the row says which step is in progress
 * while the turn runs (the message's avatar animates beside it) and how much
 * was done once it is over.
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

function _failed(output: unknown) {
    return typeof output === 'object' && output !== null && (output as { status?: string }).status === 'error'
}
</script>

<template>
    <UCollapsible class="w-full">
        <button type="button"
                class="group flex items-center gap-1.5 w-full text-left text-sm text-muted hover:text-highlighted transition-colors cursor-pointer">
            <UIcon :name="failed ? 'i-lucide-triangle-alert' : 'i-lucide-list-checks'"
                   class="size-4 shrink-0"
                   :class="failed ? 'text-error' : ''" />
            <UChatShimmer v-if="live"
                          :text="text" />
            <span v-else>{{ text }}</span>
            <span v-if="suffix"
                  class="text-dimmed">{{ suffix }}</span>
            <UIcon name="i-lucide-chevron-down"
                   class="size-4 shrink-0 transition-transform group-data-[state=open]:rotate-180" />
        </button>

        <template #content>
            <div class="flex flex-col items-start gap-1 w-full pt-2 pl-1">
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
        </template>
    </UCollapsible>
</template>
