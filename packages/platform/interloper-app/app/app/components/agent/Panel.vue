<script setup lang="ts">
/**
 * Docked agent chat panel: a card that slides in from the right below the
 * navbar, non-modal; the page's toolbar and body make room for it. The first time it opens, and on an organisation switch
 * while open, it picks the conversation up where it was left in that
 * organisation, the most recent one. Without one, and after "new", the
 * panel holds no conversation until the first message is sent, so nothing
 * empty is ever stored; turns stream through useAgentChat.
 */
import type { Conversation } from '~/types/agent'

const { open, width, dragging, startResize, resetWidth } = useAgentPanel()
const panel = useTemplateRef<HTMLElement>('panel')

// The panel's width and the room the page leaves for it (the `agent-room` toolbar and body, see
// app.config) live in a small stylesheet the panel owns, as rules on just those elements. Bound in a
// template, a resize would re-render the conversation and the layout on every pointer move; as a
// variable on a shared ancestor, it would restyle the whole page.
const sizing = new CSSStyleSheet()
watchEffect(() => {
    const room = open.value ? `calc(${width.value}px + var(--agent-inset))` : '0px'
    sizing.replaceSync(`
        .agent-panel { width: ${width.value}px }
        .agent-panel-handle { right: calc(${width.value}px + var(--agent-inset) * 1.5 - 1px) }
        .agent-room { margin-inline-end: ${room}; transition-duration: ${dragging.value ? '0ms' : '300ms'} }
    `)
})
onMounted(() => {
    document.adoptedStyleSheets = [...document.adoptedStyleSheets, sizing]
})
onBeforeUnmount(() => {
    document.adoptedStyleSheets = document.adoptedStyleSheets.filter(sheet => sheet !== sizing)
})
const appConfig = useAppConfig()
const agentStore = useAgentStore()
const organisationStore = useOrganisationStore()

const resumed = ref(false)
const creating = ref(false)
const unavailable = ref(false)
const chat = shallowRef<ReturnType<typeof useAgentChat>>()
const summary = ref<string | null>(null)

/** Make a conversation the panel's, with its history. */
async function attach(conversation: Conversation) {
    const detail = await agentStore.getConversation(conversation.id)
    summary.value = detail.summary
    chat.value = useAgentChat(detail.id, detail.messages)
}

/** Leave the current conversation for a blank one. */
function startNew() {
    if (chat.value?.busy.value) return
    chat.value = undefined
    summary.value = null
}

/** Pick up the organisation's latest conversation, once it is open. */
async function resume() {
    if (!open.value || resumed.value || unavailable.value) return
    resumed.value = true
    try {
        await agentStore.fetchConversations()
        const latest = agentStore.conversations[0]
        if (latest) await attach(latest)
    }
    catch {
        unavailable.value = true
    }
}

watch(open, resume, { immediate: true })

// A conversation belongs to one organisation: a switch leaves it for the new organisation's latest.
watch(() => organisationStore.organisation?.id, () => {
    chat.value?.stop()
    chat.value = undefined
    summary.value = null
    resumed.value = false
    unavailable.value = false
    creating.value = false
    resume()
})

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

/** Start a resize from the card's current inset, which the theme sets per breakpoint. */
function onResizeStart() {
    const right = panel.value?.getBoundingClientRect().right ?? window.innerWidth
    startResize(window.innerWidth - right)
}

const SUGGESTIONS = [
    'What failed in the last 24 hours?',
    'Summarise my pipeline health',
    'Which sources are stale?',
]
</script>

<template>
    <UDashboardResizeHandle v-if="open"
                            class="agent-panel-handle fixed z-40 w-0.5 transition-colors hover:bg-accented"
                            :class="dragging && 'bg-accented'"
                            :style="{
                                top: 'calc(var(--ui-header-height) + var(--agent-inset))',
                                bottom: 'var(--agent-inset)',
                            }"
                            @mousedown.prevent="onResizeStart"
                            @touchstart.prevent="onResizeStart"
                            @dblclick="resetWidth" />
    <aside ref="panel"
           class="agent-panel fixed z-40 flex min-h-0 flex-col overflow-hidden rounded-lg bg-muted ring ring-default transition-transform duration-300"
           :style="{
               top: 'calc(var(--ui-header-height) + var(--agent-inset))',
               right: 'var(--agent-inset)',
               bottom: 'var(--agent-inset)',
               transform: open ? 'none' : 'translateX(calc(100% + var(--agent-inset)))',
           }">
        <!-- Header -->
        <div class="flex shrink-0 items-center gap-3 p-4 sm:px-6">
            <div class="flex size-8 items-center justify-center rounded-md bg-primary text-inverted">
                <UIcon :name="appConfig.icons.agent"
                       class="size-5" />
            </div>
            <div class="min-w-0 flex-1">
                <div class="font-semibold text-highlighted">Agent</div>
                <div class="text-xs text-muted">Reads your sources, jobs and runs</div>
            </div>
            <UButton :icon="appConfig.ui.icons.plus"
                     color="neutral"
                     variant="ghost"
                     size="sm"
                     aria-label="New conversation"
                     :disabled="!chat || chat.busy.value"
                     @click="startNew" />
            <UButton :icon="appConfig.ui.icons.close"
                     color="neutral"
                     variant="ghost"
                     size="sm"
                     aria-label="Close agent panel"
                     @click="open = false" />
        </div>

        <!-- Messages. The relative wrapper (not the scroll container itself) anchors
             the floating scroll-to-bottom button to the visible area. -->
        <div class="relative flex min-h-0 flex-1 flex-col">
            <div class="min-h-0 flex-1 overflow-y-auto px-4 pb-4 sm:px-6 sm:pb-6">
                <UEmpty v-if="unavailable"
                        variant="naked"
                        size="sm"
                        :icon="appConfig.icons.agent"
                        title="Agent unavailable"
                        description="The agent isn't available in this workspace." />

                <template v-else>
                    <AgentSummary v-if="chat && summary"
                                  :text="summary"
                                  class="mb-4" />
                    <UChatMessages v-if="chat"
                                   :messages="chat.messages.value"
                                   :status="chat.status.value"
                                   compact
                                   should-auto-scroll
                                   :assistant="{ ui: { body: 'flex-1' } }"
                                   :auto-scroll="{ size: 'md', color: 'neutral', variant: 'outline' }"
                                   :ui="{ root: 'px-0', viewport: 'top-auto bottom-3' }"
                                   class="pb-2">
                        <template #indicator>
                            <AgentThinking />
                        </template>

                        <template #content="{ message }">
                            <AgentParts :message="message"
                                        :live="message.id === chat.liveMessageId.value"
                                        @approve="(id, approved) => chat?.addToolApprovalResponse({ id, approved })"
                                        @output="(tool, toolCallId, output) => chat?.addToolOutput({ tool, toolCallId, output })">
                                <!-- The avatar opens the message's first row, so the message runs edge to edge between the panel's padding. -->
                                <template #leading>
                                    <AgentAvatar v-if="message.role === 'assistant'"
                                                 :live="message.id === chat.liveMessageId.value"
                                                 size="sm" />
                                </template>
                            </AgentParts>
                        </template>
                    </UChatMessages>

                    <p v-if="chat?.error.value && !chat.busy.value"
                       class="mt-1 text-sm text-error">
                        Something went wrong. Try again.
                    </p>

                    <!-- Suggested prompts on a fresh conversation -->
                    <div v-if="!chat || (!chat.messages.value.length && !chat.busy.value)"
                         class="flex flex-col gap-2">
                        <p class="mb-2 text-sm">
                            Hi, I'm your Interloper agent. Ask about pipeline health, why something
                            failed, or have me draft a job or connector.
                        </p>
                        <div class="caption text-muted">Suggested</div>
                        <UButton v-for="suggestion in SUGGESTIONS"
                                 :key="suggestion"
                                 :label="suggestion"
                                 color="neutral"
                                 variant="outline"
                                 block
                                 class="justify-start"
                                 @click="submit(suggestion)" />
                    </div>
                </template>
            </div>
        </div>

        <!-- Composer -->
        <div class="shrink-0 border-t border-default p-4 sm:px-6">
            <UChatPrompt v-model="input"
                         variant="outline"
                         placeholder="Ask about your workspace…"
                         :maxrows="6"
                         :disabled="unavailable || creating"
                         :ui="{ base: 'px-1.5', body: 'items-center' }"
                         @submit="onSubmit">
                <UChatPromptSubmit :status="chat?.status.value ?? (creating ? 'submitted' : 'ready')"
                                   size="sm"
                                   @stop="chat?.stop()" />
            </UChatPrompt>
        </div>
    </aside>
</template>
