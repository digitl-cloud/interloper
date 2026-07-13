<script setup lang="ts">
definePageMeta({ layout: 'agent' })

const route = useRoute()
const sessionId = computed(() => route.params.id as string)

const { messages, streaming, error, send, loadHistory } = useAgentChat(sessionId)

const input = ref('')

const status = computed(() => {
    if (streaming.value) return 'streaming' as const
    return 'ready' as const
})

function onSubmit(e: Event) {
    e.preventDefault()
    if (!input.value.trim() || streaming.value) return
    send(input.value)
    input.value = ''
}

/** Report a completed connection setup back into the chat so the agent continues. */
function onConnectionCreated(name: string) {
    send(`I completed the setup — connection "${name}" is created.`)
}

onMounted(async () => {
    await loadHistory()
    const initialQuery = route.query.q as string | undefined
    if (initialQuery && !messages.value.length) {
        send(initialQuery)
    }
})
</script>

<template>
    <UDashboardPanel id="chat"
                     class="relative min-h-0"
                     :ui="{ body: 'p-0 sm:p-0 overscroll-none' }">
        <template #body>
            <UContainer class="flex-1 flex flex-col gap-4 sm:gap-6 pt-6"
                        :ui="{ base: 'max-w-3xl' }">
                <UChatMessages should-auto-scroll
                               :status="status"
                               :spacing-offset="120"
                               class="pb-4 sm:pb-6">
                    <template v-if="!messages.length && !streaming">
                        <div class="flex flex-col items-center justify-center h-full text-muted py-12">
                            <UIcon name="i-lucide-sparkles"
                                   class="size-8 mb-2" />
                            <p class="text-sm">
                                Start a conversation
                            </p>
                        </div>
                    </template>

                    <UChatMessage v-for="message in messages"
                                  :id="message.id"
                                  :key="message.id"
                                  :role="message.role === 'user' ? 'user' : 'assistant'"
                                  :parts="[{ type: 'text', text: message.text }]"
                                  :variant="message.role === 'user' ? 'soft' : 'naked'"
                                  :side="message.role === 'user' ? 'right' : 'left'"
                                  :icon="message.role === 'assistant' ? 'i-lucide-sparkles' : undefined"
                                  :ui="message.role === 'assistant' ? { body: 'flex-1' } : undefined">
                        <template #content>
                            <!-- Render the connect card, markdown for assistant, plain text for user -->
                            <AgentConnectCard v-if="message.connectionSetup"
                                              :request="message.connectionSetup"
                                              @created="onConnectionCreated" />
                            <MDC v-else-if="message.role === 'assistant'"
                                 :value="message.text"
                                 :cache-key="message.id"
                                 class="*:first:mt-0 *:last:mb-0" />
                            <p v-else
                               class="whitespace-pre-wrap">
                                {{ message.text }}
                            </p>
                        </template>
                    </UChatMessage>
                </UChatMessages>

                <UChatPrompt v-model="input"
                             variant="subtle"
                             placeholder="Ask anything..."
                             :disabled="streaming"
                             class="sticky bottom-0 [view-transition-name:chat-prompt] rounded-b-none z-10"
                             :ui="{ base: 'px-1.5' }"
                             @submit="onSubmit">
                    <UChatPromptSubmit :status="status"
                                       color="neutral"
                                       size="sm" />
                </UChatPrompt>
            </UContainer>
        </template>
    </UDashboardPanel>
</template>
