<script setup lang="ts">
definePageMeta({ layout: 'agent' })

const agentStore = useAgentStore()
const prompt = ref('')
const creating = ref(false)

const suggestions = [
    { label: 'Setup a new source', icon: 'i-lucide-plug' },
    { label: 'Setup a new connection', icon: 'i-lucide-key-round' },
    { label: 'What failed in the last 24 hours?', icon: 'i-lucide-alert-circle' },
]

async function onSubmit(e: Event) {
    e.preventDefault()
    if (!prompt.value.trim() || creating.value) return
    creating.value = true
    try {
        const session = await agentStore.createSession()
        const initialMessage = prompt.value
        prompt.value = ''
        await navigateTo({
            path: `/agent/chat/${session.id}`,
            query: { q: initialMessage },
        })
    }
    catch {
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
                    <UIcon name="i-lucide-sparkles"
                           class="size-10 text-primary mb-2" />
                    <h1 class="text-2xl font-semibold">
                        How can I help you?
                    </h1>
                    <p class="text-muted text-sm">
                        Ask questions about your data assets, pipelines, and jobs.
                    </p>
                </div>

                <div class="w-full max-w-2xl flex flex-col gap-4">
                    <UChatPrompt v-model="prompt"
                                 placeholder="Ask anything..."
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
                                 variant="subtle"
                                 size="sm"
                                 @click="useSuggestion(suggestion.label)" />
                    </div>
                </div>
            </UContainer>
        </template>
    </UDashboardPanel>
</template>
