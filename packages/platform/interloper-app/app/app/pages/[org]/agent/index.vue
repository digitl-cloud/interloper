<script setup lang="ts">
definePageMeta({ layout: 'agent' })

const agentStore = useAgentStore()
const organisationStore = useOrganisationStore()
const appConfig = useAppConfig()
const toast = useToast()
const prompt = ref('')
const creating = ref(false)

const suggestions = [
    { label: 'Set up a new source', icon: appConfig.icons.source },
    { label: 'Set up a new connection', icon: appConfig.icons.connection },
    { label: 'What failed in the last 24 hours?', icon: appConfig.ui.icons.error },
]

async function onSubmit(e: Event) {
    e.preventDefault()
    if (!prompt.value.trim() || creating.value) return
    creating.value = true
    try {
        const conversation = await agentStore.createConversation()
        const initialMessage = prompt.value
        prompt.value = ''
        await navigateTo({
            path: organisationStore.path(`/agent/chat/${conversation.id}`),
            query: { q: initialMessage },
        })
    }
    catch (e) {
        toast.add(errorToast(e, "Couldn't start the conversation"))
        creating.value = false
    }
}

function useSuggestion(text: string) {
    prompt.value = text
}
</script>

<template>
    <UDashboardPanel id="agent-home"
                     :ui="{ body: 'p-0 sm:p-0' }">
        <template #body>
            <UContainer class="flex-1 flex flex-col items-center justify-center gap-6 py-12">
                <div class="flex flex-col items-center gap-1">
                    <UIcon :name="appConfig.icons.agent"
                           class="mb-2 size-8 text-primary" />
                    <h1 class="text-2xl font-semibold text-highlighted">
                        How can I help you?
                    </h1>
                    <p class="text-sm text-muted">
                        Ask questions about your data assets, pipelines, and jobs.
                    </p>
                </div>

                <div class="w-full max-w-2xl flex flex-col gap-4">
                    <UChatPrompt v-model="prompt"
                                 placeholder="Ask anything…"
                                 variant="subtle"
                                 :disabled="creating"
                                 autofocus
                                 @submit="onSubmit">
                        <UChatPromptSubmit :loading="creating"
                                           color="neutral"
                                           size="sm" />
                    </UChatPrompt>

                    <div class="flex flex-wrap justify-center gap-2">
                        <UButton v-for="suggestion in suggestions"
                                 :key="suggestion.label"
                                 :label="suggestion.label"
                                 :icon="suggestion.icon"
                                 color="neutral"
                                 variant="outline"
                                 size="sm"
                                 @click="useSuggestion(suggestion.label)" />
                    </div>
                </div>
            </UContainer>
        </template>
    </UDashboardPanel>
</template>
