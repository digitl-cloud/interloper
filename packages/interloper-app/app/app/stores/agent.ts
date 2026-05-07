import type { AgentSession } from '~/types/agent'

export const useAgentStore = defineStore('agent', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const sessions = ref<AgentSession[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Actions
     **********************/

    async function fetchSessions() {
        loading.value = true
        error.value = null
        try {
            sessions.value = await apiFetch<AgentSession[]>('/agent/sessions')
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function createSession(): Promise<AgentSession> {
        const session = await apiFetch<AgentSession>('/agent/sessions', {
            method: 'POST',
        })
        sessions.value.unshift(session)
        return session
    }

    async function getSession(sessionId: string): Promise<any> {
        return apiFetch(`/agent/sessions/${sessionId}`)
    }

    async function deleteSession(sessionId: string) {
        await apiFetch(`/agent/sessions/${sessionId}`, { method: 'DELETE' })
        sessions.value = sessions.value.filter(s => s.id !== sessionId)
    }

    /**********************
     * Lookups
     **********************/

    function findById(id: string): AgentSession | undefined {
        return sessions.value.find(s => s.id === id)
    }

    function $reset() {
        sessions.value = []
        loading.value = false
        error.value = null
    }

    return {
        sessions,
        loading,
        error,
        fetchSessions,
        createSession,
        getSession,
        deleteSession,
        findById,
        $reset,
    }
})
