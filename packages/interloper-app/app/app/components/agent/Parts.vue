<script setup lang="ts">
/**
 * One message's parts, in the order the model produced them.
 *
 * What the user acts on or reads stands on its own: the answer, the
 * selection and connect cards on their tool parts (answered through
 * `output`), and a tool row waiting for Approve/Deny. Everything between,
 * the reasoning and the tool calls, folds into an `AgentSteps` trail, which
 * names the step in progress while the turn is still running (`live`).
 * A `leading` slot (the message's avatar) opens the first row: the trail's
 * own row when the message starts with one, a row of its own otherwise.
 */
import type { UIMessage } from 'ai'
import { getToolName, isReasoningUIPart, isTextUIPart, isToolUIPart } from 'ai'
import { isPartStreaming, isToolApprovalPending } from '@nuxt/ui/utils/ai'
import type { ConnectionSetupRequest, ConnectionSetupResult, SelectionRequest } from '~/types/agent'

const props = defineProps<{ message: UIMessage, live?: boolean }>()

const emit = defineEmits<{
    approve: [id: string, approved: boolean]
    output: [tool: string, toolCallId: string, output: unknown]
}>()

type Part = UIMessage['parts'][number]
type Segment = { kind: 'steps', parts: Part[] } | { kind: 'part', part: Part }

const CARDS = { request_user_selection: 'select', request_connection_setup: 'connect' } as const

/** A card is the app's side of a call that waits for the user; a call that failed instead is a step. */
function card(part: Part) {
    return isToolUIPart(part) && !isFailedToolCall(part) ? CARDS[getToolName(part) as keyof typeof CARDS] : undefined
}

/** A step is work the user only watches: reasoning, or a tool call that is neither a card nor awaiting approval. */
function isStep(part: Part) {
    if (isReasoningUIPart(part)) return true
    return isToolUIPart(part) && !card(part) && !isToolApprovalPending(part)
}

const segments = computed<Segment[]>(() => {
    const result: Segment[] = []
    for (const part of props.message.parts) {
        const last = result[result.length - 1]
        // A card renders from its call's input, which is undefined until the stream has delivered it.
        if (card(part) && isToolUIPart(part) && part.state === 'input-streaming') continue
        if (isStep(part)) {
            if (last?.kind === 'steps') last.parts.push(part)
            else result.push({ kind: 'steps', parts: [part] })
        }
        else if (isTextUIPart(part) || card(part) || isToolUIPart(part)) {
            result.push({ kind: 'part', part })
        }
    }
    return result
})

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
    <slot v-if="segments[0]?.kind !== 'steps'"
          name="leading" />
    <template v-for="(segment, index) in segments"
              :key="`${message.id}-${index}`">
        <AgentSteps v-if="segment.kind === 'steps'"
                    :parts="segment.parts"
                    :live="live && index === segments.length - 1">
            <template v-if="index === 0"
                      #leading>
                <slot name="leading" />
            </template>
        </AgentSteps>

        <AgentSelectCard v-else-if="isToolUIPart(segment.part) && card(segment.part) === 'select'"
                         :request="segment.part.input as SelectionRequest"
                         :answer="answer<{ selected: string[] }>(segment.part)?.selected"
                         @selected="onSelected(segment.part, $event)" />

        <AgentConnectCard v-else-if="isToolUIPart(segment.part) && card(segment.part) === 'connect'"
                          :request="segment.part.input as ConnectionSetupRequest"
                          :answer="answer<ConnectionSetupResult>(segment.part)"
                          @created="onCreated(segment.part, $event)" />

        <AgentTool v-else-if="isToolUIPart(segment.part)"
                   :part="segment.part"
                   @approve="(id, approved) => emit('approve', id, approved)" />

        <template v-else-if="isTextUIPart(segment.part)">
            <Markdown v-if="message.role === 'assistant'"
                      :value="segment.part.text"
                      :streaming="isPartStreaming(segment.part)"
                      class="chat-prose *:first:mt-0 *:last:mb-0" />
            <p v-else
               class="whitespace-pre-wrap">
                {{ segment.part.text }}
            </p>
        </template>
    </template>
</template>
