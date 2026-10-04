import type { Conversation, ConversationDetail } from '~/types/agent'

export const useAgentStore = defineStore('agent', () => {
    const { apiFetch, fetchAll } = useApi()

    /**********************
     * State
     **********************/
    const conversations = ref<Conversation[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Actions
     **********************/
    async function fetchConversations() {
        loading.value = true
        error.value = null
        try {
            conversations.value = await fetchAll<Conversation>('/agent/conversations')
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function createConversation(): Promise<Conversation> {
        const conversation = await apiFetch<Conversation>('/agent/conversations', { method: 'POST' })
        conversations.value.unshift(conversation)
        return conversation
    }

    async function getConversation(id: string): Promise<ConversationDetail> {
        return apiFetch<ConversationDetail>(`/agent/conversations/${id}`)
    }

    async function deleteConversation(id: string) {
        await apiFetch(`/agent/conversations/${id}`, { method: 'DELETE' })
        conversations.value = conversations.value.filter(c => c.id !== id)
    }

    /** Reflect a title the server set on the first turn without refetching the list. */
    function setTitle(id: string, title: string) {
        const conversation = conversations.value.find(c => c.id === id)
        if (conversation && !conversation.title) conversation.title = title
    }

    function $reset() {
        conversations.value = []
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(() => fetchConversations(), $reset)

    return {
        conversations,
        loading,
        error,
        fetchConversations,
        createConversation,
        getConversation,
        deleteConversation,
        setTitle,
        $reset,
    }
})
