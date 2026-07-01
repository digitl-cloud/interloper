import type { AgentEvent, ChatMessage } from '~/types/agent'

/**
 * Composable for managing an agent chat session with SSE streaming.
 *
 * Handles sending messages, parsing the SSE event stream, and
 * accumulating chat messages for the UI.
 */
export function useAgentChat(sessionId: Ref<string>) {
    const messages = ref<ChatMessage[]>([])
    const streaming = ref(false)
    const error = ref<Error | null>(null)

    /** Load existing messages from a session's event history. */
    async function loadHistory() {
        const agentStore = useAgentStore()
        try {
            const session = await agentStore.getSession(sessionId.value)
            if (!session?.events) return

            const restored: ChatMessage[] = []
            for (const event of session.events) {
                const text = _extractText(event)
                if (!text) continue

                const role = event.author === 'user' ? 'user' as const : 'assistant' as const
                // Merge consecutive assistant messages from the same invocation
                const last = restored[restored.length - 1]
                if (role === 'assistant' && last?.role === 'assistant') {
                    last.text += text
                }
                else {
                    restored.push({ id: event.id || crypto.randomUUID(), role, text })
                }
            }
            messages.value = restored
        }
        catch (e) {
            error.value = e as Error
        }
    }

    /** Send a message and stream the agent's response. */
    async function send(text: string) {
        if (!text.trim() || streaming.value) return

        error.value = null

        messages.value.push({
            id: crypto.randomUUID(),
            role: 'user',
            text,
        })

        const assistantId = crypto.randomUUID()
        messages.value.push({
            id: assistantId,
            role: 'assistant',
            text: '',
            loading: true,
        })

        streaming.value = true

        try {
            const response = await fetch(`/api/agent/sessions/${sessionId.value}/chat`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'include',
                body: JSON.stringify({ message: text }),
            })

            if (!response.ok) {
                throw new Error(`Agent request failed: ${response.status}`)
            }

            const reader = response.body?.getReader()
            if (!reader) throw new Error('No response stream')

            const decoder = new TextDecoder()
            let buffer = ''

            while (true) {
                const { done, value } = await reader.read()
                if (done) break

                buffer += decoder.decode(value, { stream: true })

                // Parse SSE lines
                const lines = buffer.split('\n')
                buffer = lines.pop() || ''

                for (const line of lines) {
                    if (!line.startsWith('data: ')) continue
                    const json = line.slice(6).trim()
                    if (!json) continue

                    try {
                        const event: AgentEvent = JSON.parse(json)
                        const eventText = _extractText(event)
                        if (eventText && event.author !== 'user') {
                            const msg = messages.value.find(m => m.id === assistantId)
                            if (msg) {
                                msg.text += eventText
                                msg.loading = false
                            }
                        }
                    }
                    catch {
                        // Skip malformed events
                    }
                }
            }
        }
        catch (e) {
            error.value = e as Error
            // Remove empty assistant placeholder on error
            const msg = messages.value.find(m => m.id === assistantId)
            if (msg && !msg.text) {
                messages.value = messages.value.filter(m => m.id !== assistantId)
            }
        }
        finally {
            const msg = messages.value.find(m => m.id === assistantId)
            if (msg) msg.loading = false
            streaming.value = false
        }
    }

    return { messages, streaming, error, send, loadHistory }
}

/** Extract text content from an ADK event. */
function _extractText(event: any): string {
    if (!event?.content?.parts) return ''
    return event.content.parts
        .filter((p: any) => p.text)
        .map((p: any) => p.text)
        .join('')
}
