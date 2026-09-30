<script setup lang="ts">
/**
 * Docked agent chat panel (design: 400px right panel that pushes the app
 * layout, non-modal). The first time it opens it picks the conversation up
 * where it was left, the most recent one. Without one, and after "new", the
 * panel holds no conversation until the first message is sent, so nothing
 * empty is ever stored; turns stream through useAgentChat.
 */
import type { Conversation } from '~/types/agent'

const { open, width, dragging, startResize, resetWidth } = useAgentPanel()
const agentStore = useAgentStore()

const resumed = ref(false)
const creating = ref(false)
const unavailable = ref(false)
const chat = shallowRef<ReturnType<typeof useAgentChat>>()

/** Make a conversation the panel's, with its history. */
async function attach(conversation: Conversation) {
    const detail = await agentStore.getConversation(conversation.id)
    chat.value = useAgentChat(detail.id, detail.messages)
}

/** Leave the current conversation for a blank one. */
function startNew() {
    if (chat.value?.busy.value) return
    chat.value = undefined
}

watch(open, async (v) => {
    if (!v || resumed.value || unavailable.value) return
    resumed.value = true
    try {
        if (!agentStore.conversations.length) await agentStore.fetchConversations()
        const latest = agentStore.conversations[0]
        if (latest) await attach(latest)
    }
    catch {
        unavailable.value = true
    }
}, { immediate: true })

const input = ref('')

function onSubmit(e: Event) {
    e.preventDefault()
    submit(input.value)
}

/** Send a message, creating the conversation on the first one. */
async function submit(text: string) {
    if (!text.trim() || creating.value) return
    if (!chat.value) {
        creating.value = true
        try {
            await attach(await agentStore.createConversation())
        }
        catch {
            unavailable.value = true
            return
        }
        finally {
            creating.value = false
        }
    }
    chat.value?.send(text)
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
            <div class="size-8 rounded-md bg-primary text-inverted flex items-center justify-center">
                <UIcon name="i-lucide-sparkles"
                       class="size-4" />
            </div>
            <div class="flex-1 min-w-0">
                <div class="text-[15px] font-bold tracking-[-0.01em] text-highlighted">Agent</div>
                <div class="text-[11.5px] text-dimmed">Reads your sources, jobs & runs</div>
            </div>
            <UButton icon="i-lucide-plus"
                     color="neutral"
                     variant="ghost"
                     size="sm"
                     class="rounded-md text-muted"
                     aria-label="New conversation"
                     :disabled="!chat || chat.busy.value"
                     @click="startNew" />
            <UButton icon="i-lucide-x"
                     color="neutral"
                     variant="soft"
                     size="sm"
                     class="rounded-md text-muted"
                     aria-label="Close agent panel"
                     @click="open = false" />
        </div>

        <!-- Messages. The relative wrapper (not the scroll container itself) anchors
             the floating scroll-to-bottom button to the visible area. -->
        <div class="relative flex-1 min-h-0 flex flex-col">
        <div class="flex-1 min-h-0 overflow-y-auto p-[18px]">
            <div v-if="unavailable"
                 class="flex flex-col items-center gap-2 py-10 text-center">
                <UIcon name="i-lucide-plug-zap"
                       class="size-6 text-dimmed" />
                <p class="text-sm text-muted">The agent isn't available in this workspace.</p>
            </div>

            <template v-else>
                <UChatMessages v-if="chat"
                               :messages="chat.messages.value"
                               :status="chat.status.value"
                               compact
                               should-auto-scroll
                               :assistant="{ ui: { body: 'flex-1', content: 'text-[13.5px]' } }"
                               :user="{ ui: { content: 'text-[13.5px]' } }"
                               :auto-scroll="{ size: 'md', color: 'neutral', variant: 'outline' }"
                               :ui="{ viewport: 'top-auto bottom-3' }"
                               class="pb-2">
                    <template #indicator>
                        <AgentThinking />
                    </template>

                    <template #leading="{ message }">
                        <AgentAvatar v-if="message.role === 'assistant'"
                                     :live="message.id === chat.liveMessageId.value"
                                     size="sm" />
                    </template>

                    <template #content="{ message }">
                        <AgentParts :message="message"
                                    :live="message.id === chat.liveMessageId.value"
                                    @approve="(id, approved) => chat?.addToolApprovalResponse({ id, approved })"
                                    @output="(tool, toolCallId, output) => chat?.addToolOutput({ tool, toolCallId, output })" />
                    </template>
                </UChatMessages>

                <p v-if="chat?.error.value && !chat.busy.value"
                   class="text-[12.5px] text-error mt-1">
                    Something went wrong — try again.
                </p>

                <!-- Suggested prompts on a fresh conversation -->
                <div v-if="!chat || (!chat.messages.value.length && !chat.busy.value)"
                     class="flex flex-col gap-2">
                    <p class="text-[13.5px] text-toned leading-relaxed mb-2">
                        Hi — I'm your Interloper agent. Ask about pipeline health, why something
                        failed, or have me draft a job or connector.
                    </p>
                    <div class="eyebrow text-dimmed pl-0.5">Suggested</div>
                    <button v-for="suggestion in SUGGESTIONS"
                            :key="suggestion"
                            type="button"
                            class="flex items-center gap-2 text-left px-3 py-2.5 border border-default rounded-lg bg-(--ui-bg-band) text-[13px] text-toned cursor-pointer transition hover:border-primary/40"
                            @click="submit(suggestion)">
                        <UIcon name="i-lucide-arrow-up-right"
                               class="size-3.5 text-dimmed shrink-0" />
                        {{ suggestion }}
                    </button>
                </div>
            </template>
        </div>
        </div>

        <!-- Composer -->
        <div class="border-t border-default p-3.5 shrink-0">
            <UChatPrompt v-model="input"
                         variant="outline"
                         placeholder="Ask about your workspace…"
                         :maxrows="6"
                         :disabled="unavailable || creating"
                         :ui="{ base: 'px-1.5 text-[13.5px]/5', body: 'items-center' }"
                         @submit="onSubmit">
                <UChatPromptSubmit :status="chat?.status.value ?? (creating ? 'submitted' : 'ready')"
                                   size="sm"
                                   @stop="chat?.stop()" />
            </UChatPrompt>
        </div>
    </aside>
</template>
