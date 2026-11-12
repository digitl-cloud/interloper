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
const failed = computed(() => tools.value.filter(isFailedToolCall).length)

const text = computed(() => {
    if (props.live) {
        const last = props.parts[props.parts.length - 1]
        if (last && isToolUIPart(last)) return toolLabel(getToolName(last), isToolStreaming(last))
        return 'Thinking…'
    }
    const count = tools.value.length
    return count ? `${count} step${count === 1 ? '' : 's'}` : 'Thought'
})

const suffix = computed(() => !props.live && failed.value ? `${failed.value} failed` : undefined)

const appConfig = useAppConfig()
</script>

<template>
    <AgentFold :icon="failed ? appConfig.ui.icons.warning : 'i-lucide-list-checks'"
               :text="text"
               :suffix="suffix"
               :live="live"
               :error="failed > 0">
        <template #leading>
            <slot name="leading" />
        </template>

        <div class="flex w-full flex-col items-start gap-1 pt-2 pl-1">
            <template v-for="(part, index) in parts"
                      :key="index">
                <UChatReasoning v-if="isReasoningUIPart(part)"
                                :text="part.text"
                                :streaming="isPartStreaming(part)"
                                chevron="leading"
                                class="w-full">
                    <Markdown :value="part.text"
                              :streaming="isPartStreaming(part)"
                              class="chat-prose *:first:mt-0 *:last:mb-0" />
                </UChatReasoning>
                <AgentTool v-else-if="isToolUIPart(part)"
                           :part="part" />
            </template>
        </div>
    </AgentFold>
</template>
