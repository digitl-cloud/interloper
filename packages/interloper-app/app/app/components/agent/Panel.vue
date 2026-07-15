<script setup lang="ts">
/**
 * Docked agent chat panel (design: 400px right panel that pushes the app
 * layout, non-modal). Drives the real agent: a session is created lazily
 * on first open, then messages stream over SSE via useAgentChat.
 */
const { open, width, dragging, startResize, resetWidth } = useAgentPanel()
const agentStore = useAgentStore()

const sessionId = ref('')
const sessionError = ref(false)
const { messages, streaming, error, send } = useAgentChat(sessionId)

/**
 * Messages in the UIMessage shape UChatMessages expects. The empty assistant
 * placeholder (awaiting the first token) is dropped — that gap is what the
 * `submitted` status' thinking indicator covers.
 */
const displayMessages = computed(() => messages.value
    .filter(m => m.text || m.role === 'user' || m.connectionSetup || m.selection || m.confirmation)
    .map(m => ({
        id: m.id,
        role: m.role,
        text: m.text,
        parts: [{ type: 'text' as const, text: m.text }],
        connectionSetup: m.connectionSetup,
        selection: m.selection,
        confirmation: m.confirmation,
    })))

/** Report a completed connection setup back into the chat so the agent continues. */
function onConnectionCreated(name: string, verified: boolean) {
    send(`I completed the setup — connection "${name}" is created${verified ? ' and the connection check passed' : ''}.`)
}

/** Report a confirmed selection back into the chat so the agent continues. */
function onSelection(labels: string[], values: string[]) {
    send(`I selected: ${labels.map((label, i) => `${label} (${values[i]})`).join(', ')}`)
}

/** Report a confirmation decision back into the chat so the agent proceeds or stops. */
function onDecision(confirmed: boolean) {
    send(confirmed ? 'Confirmed — go ahead.' : 'Cancel that — do not proceed.')
}

const status = computed(() => {
    if (!streaming.value) return 'ready' as const
    const last = messages.value[messages.value.length - 1]
    return last?.role === 'assistant' && !last.text ? 'submitted' as const : 'streaming' as const
})

/** Create the backing session the first time the panel opens. */
watch(open, async (v) => {
    if (!v || sessionId.value || sessionError.value) return
    try {
        const session = await agentStore.createSession()
        sessionId.value = session.id
    }
    catch {
        sessionError.value = true
    }
}, { immediate: true })

const input = ref('')

function onSubmit(e: Event) {
    e.preventDefault()
    submit(input.value)
}

function submit(text: string) {
    if (!text.trim() || streaming.value || !sessionId.value) return
    send(text)
    input.value = ''
}

const SUGGESTIONS = [
    'What failed in the last 24 hours?',
    'Summarize my pipeline health',
    'Which sources are stale?',
]
</script>

<template>
    <aside class="fixed top-0 right-0 h-full z-40 bg-default border-l border-default flex flex-col min-h-0 transition-transform duration-300"
           :class="open ? 'translate-x-0' : 'translate-x-full'"
           :style="{ width: `${width}px` }">
        <UDashboardResizeHandle class="absolute left-0 inset-y-0 z-10 w-1 -ml-px transition-colors hover:bg-primary/30"
                                :class="dragging && 'bg-primary/30'"
                                @mousedown.prevent="startResize"
                                @touchstart.prevent="startResize"
                                @dblclick="resetWidth" />

        <!-- Header -->
        <div class="flex items-center gap-3 px-[18px] h-(--ui-header-height) border-b border-default shrink-0">
            <div class="size-8 rounded-[9px] bg-primary text-inverted flex items-center justify-center">
                <UIcon name="i-lucide-sparkles"
                       class="size-4" />
            </div>
            <div class="flex-1 min-w-0">
                <div class="text-[15px] font-bold tracking-[-0.01em] text-highlighted">Agent</div>
                <div class="text-[11.5px] text-dimmed">Reads your sources, jobs & runs</div>
            </div>
            <UButton icon="i-lucide-x"
                     color="neutral"
                     variant="soft"
                     size="sm"
                     class="rounded-[9px] text-muted"
                     aria-label="Close agent panel"
                     @click="open = false" />
        </div>

        <!-- Messages -->
        <div class="flex-1 min-h-0 overflow-y-auto p-[18px]">
            <div v-if="sessionError"
                 class="flex flex-col items-center gap-2 py-10 text-center">
                <UIcon name="i-lucide-plug-zap"
                       class="size-6 text-dimmed" />
                <p class="text-sm text-muted">The agent isn't available in this workspace.</p>
            </div>

            <template v-else>
                <UChatMessages :messages="displayMessages"
                               :status="status"
                               compact
                               should-auto-scroll
                               :assistant="{ icon: 'i-lucide-sparkles', ui: { body: 'flex-1', content: 'text-[13.5px]', leadingIcon: 'text-primary' } }"
                               :user="{ ui: { content: 'text-[13.5px]' } }"
                               :auto-scroll="{ size: 'xs', color: 'neutral', variant: 'outline' }"
                               class="pb-2">
                    <template #content="{ message }">
                        <AgentConnectCard v-if="(message as any).connectionSetup"
                                          :request="(message as any).connectionSetup"
                                          @created="onConnectionCreated" />
                        <AgentSelectCard v-else-if="(message as any).selection"
                                         :request="(message as any).selection"
                                         @selected="onSelection" />
                        <AgentConfirmCard v-else-if="(message as any).confirmation"
                                          :request="(message as any).confirmation"
                                          @decided="onDecision" />
                        <MDC v-else-if="message.role === 'assistant'"
                             :value="message.text"
                             :cache-key="message.id"
                             class="*:first:mt-0 *:last:mb-0 [&_code]:text-[12px]" />
                        <p v-else
                           class="whitespace-pre-wrap">
                            {{ message.text }}
                        </p>
                    </template>
                </UChatMessages>

                <p v-if="error && !streaming"
                   class="text-[12.5px] text-error mt-1">
                    Something went wrong — try again.
                </p>

                <!-- Suggested prompts on a fresh conversation -->
                <div v-if="!messages.length && !streaming"
                     class="flex flex-col gap-2">
                    <p class="text-[13.5px] text-toned leading-relaxed mb-2">
                        Hi — I'm your Interloper agent. Ask about pipeline health, why something
                        failed, or have me draft a job or connector.
                    </p>
                    <div class="eyebrow text-dimmed pl-0.5">Suggested</div>
                    <button v-for="suggestion in SUGGESTIONS"
                            :key="suggestion"
                            type="button"
                            class="flex items-center gap-2 text-left px-3 py-2.5 border border-default rounded-[10px] bg-(--ui-bg-band) text-[13px] text-toned cursor-pointer transition hover:border-primary/40"
                            @click="submit(suggestion)">
                        <UIcon name="i-lucide-arrow-up-right"
                               class="size-3.5 text-dimmed shrink-0" />
                        {{ suggestion }}
                    </button>
                </div>
            </template>
        </div>

        <!-- Composer -->
        <div class="border-t border-default p-3.5 shrink-0">
            <UChatPrompt v-model="input"
                         variant="outline"
                         placeholder="Ask about your workspace…"
                         :maxrows="6"
                         :disabled="streaming || sessionError"
                         :ui="{ base: 'px-1.5 text-[13.5px]/5', body: 'items-center' }"
                         @submit="onSubmit">
                <UChatPromptSubmit :status="status"
                                   size="sm" />
            </UChatPrompt>
        </div>
    </aside>
</template>
