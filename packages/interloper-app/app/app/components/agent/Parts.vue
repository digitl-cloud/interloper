<script setup lang="ts">
/**
 * One message's parts, in the order the model produced them: what it
 * thought, the tools it reached for, the cards it asked the user through,
 * and what it said.
 *
 * The two card tools render their own components off the tool part and
 * answer through `output`; every other tool is a `AgentTool` row, which
 * carries the Approve/Deny actions when the agent is waiting. While the
 * turn is still running (`live`) and nothing is visibly streaming, the
 * thinking indicator holds the tail: a tool settles in milliseconds and the
 * model then thinks for seconds, which otherwise reads as a stall.
 */
import type { UIMessage } from 'ai'
import { getToolName, isReasoningUIPart, isTextUIPart, isToolUIPart } from 'ai'
import { isPartStreaming } from '@nuxt/ui/utils/ai'
import type { ConnectionSetupRequest, ConnectionSetupResult, SelectionRequest } from '~/types/agent'

const props = defineProps<{ message: UIMessage, live?: boolean }>()

const emit = defineEmits<{
    approve: [id: string, approved: boolean]
    output: [tool: string, toolCallId: string, output: unknown]
}>()

const CARDS = { request_user_selection: 'select', request_connection_setup: 'connect' } as const

type Part = UIMessage['parts'][number]

const thinking = computed(() => {
    if (!props.live) return false
    const last = props.message.parts[props.message.parts.length - 1]
    return !last || !((isTextUIPart(last) || isReasoningUIPart(last)) && isPartStreaming(last))
})

function card(part: Part) {
    return isToolUIPart(part) ? CARDS[getToolName(part) as keyof typeof CARDS] : undefined
}

/** The tool call's output, when it has one: the answer a card was given. */
function answer<T>(part: Part): T | undefined {
    return isToolUIPart(part) && part.state === 'output-available' ? part.output as T : undefined
}

function onSelected(part: Part, selected: string[]) {
    if (isToolUIPart(part)) emit('output', getToolName(part), part.toolCallId, { selected })
}

function onCreated(part: Part, result: ConnectionSetupResult) {
    if (isToolUIPart(part)) emit('output', getToolName(part), part.toolCallId, result)
}
</script>

<template>
    <template v-for="(part, index) in message.parts"
              :key="`${message.id}-${index}`">
        <UChatReasoning v-if="isReasoningUIPart(part)"
                        :text="part.text"
                        :streaming="isPartStreaming(part)"
                        chevron="leading"
                        class="w-full">
            <Markdown :value="part.text"
                      :streaming="isPartStreaming(part)"
                      class="*:first:mt-0 *:last:mb-0" />
        </UChatReasoning>

        <AgentSelectCard v-else-if="isToolUIPart(part) && card(part) === 'select'"
                         :request="part.input as SelectionRequest"
                         :answer="answer<{ selected: string[] }>(part)?.selected"
                         @selected="onSelected(part, $event)" />

        <AgentConnectCard v-else-if="isToolUIPart(part) && card(part) === 'connect'"
                          :request="part.input as ConnectionSetupRequest"
                          :answer="answer<ConnectionSetupResult>(part)"
                          @created="onCreated(part, $event)" />

        <AgentTool v-else-if="isToolUIPart(part)"
                   :part="part"
                   @approve="(id, approved) => emit('approve', id, approved)" />

        <template v-else-if="isTextUIPart(part)">
            <Markdown v-if="message.role === 'assistant'"
                      :value="part.text"
                      :streaming="isPartStreaming(part)"
                      class="*:first:mt-0 *:last:mb-0" />
            <p v-else
               class="whitespace-pre-wrap">
                {{ part.text }}
            </p>
        </template>
    </template>

    <AgentThinking v-if="thinking"
                   class="mt-2" />
</template>
