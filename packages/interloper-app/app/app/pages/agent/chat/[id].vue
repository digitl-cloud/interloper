<script setup lang="ts">
import type { UIMessage } from 'ai'

definePageMeta({ layout: 'agent' })

const route = useRoute()
const agentStore = useAgentStore()
const conversationId = route.params.id as string

const { data: conversation, error: loadError } = await useAsyncData(
    `conversation-${conversationId}`,
    () => agentStore.getConversation(conversationId),
)

const chat = conversation.value
    ? useAgentChat(conversationId, conversation.value.messages as UIMessage[])
    : undefined

const input = ref('')

function onSubmit(e: Event) {
    e.preventDefault()
    chat?.send(input.value)
    input.value = ''
}

onMounted(() => {
    const initialQuery = route.query.q as string | undefined
    if (initialQuery && chat && !chat.messages.value.length) chat.send(initialQuery)
})
</script>

<template>
    <UDashboardPanel id="chat"
                     class="relative min-h-0"
                     :ui="{ body: 'p-0 sm:p-0 overscroll-none' }">
        <template #body>
            <UContainer class="flex-1 flex flex-col gap-4 sm:gap-6 pt-6"
                        :ui="{ base: 'max-w-3xl' }">
                <div v-if="loadError || !chat"
                     class="flex flex-col items-center justify-center h-full text-muted py-12">
                    <UIcon name="i-lucide-message-square-off"
                           class="size-8 mb-2" />
                    <p class="text-sm">
                        This conversation is not available.
                    </p>
                </div>

                <UChatMessages v-else
                               :messages="chat.messages.value"
                               :status="chat.status.value"
                               should-auto-scroll
                               :spacing-offset="120"
                               :assistant="{ ui: { body: 'flex-1' } }"
                               class="pb-4 sm:pb-6">
                    <template #indicator>
                        <AgentThinking />
                    </template>

                    <template #leading="{ message }">
                        <AgentAvatar v-if="message.role === 'assistant'"
                                     :live="message.id === chat.liveMessageId.value" />
                    </template>

                    <template #content="{ message }">
                        <AgentParts :message="message"
                                    :live="message.id === chat.liveMessageId.value"
                                    @approve="(id, approved) => chat?.addToolApprovalResponse({ id, approved })"
                                    @output="(tool, toolCallId, output) => chat?.addToolOutput({ tool, toolCallId, output })" />
                    </template>
                </UChatMessages>

                <UChatPrompt v-model="input"
                             variant="subtle"
                             placeholder="Ask anything..."
                             :disabled="!chat"
                             :error="chat?.error.value"
                             class="sticky bottom-0 [view-transition-name:chat-prompt] rounded-b-none z-10"
                             :ui="{ base: 'px-1.5' }"
                             @submit="onSubmit">
                    <UChatPromptSubmit :status="chat?.status.value ?? 'ready'"
                                       color="neutral"
                                       size="sm"
                                       @stop="chat?.stop()" />
                </UChatPrompt>
            </UContainer>
        </template>
    </UDashboardPanel>
</template>
