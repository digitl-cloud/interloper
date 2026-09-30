import type { UIMessage } from 'ai'
import { DefaultChatTransport, lastAssistantMessageIsCompleteWithToolCalls } from 'ai'
import { useChat } from '@ai-sdk/vue'

/**
 * A conversation with the assistant over the AI SDK.
 *
 * The server owns the history: `useChat` streams each turn from the
 * conversation's chat route and resumes it by itself once the user has
 * approved a call or answered a card. Loading an existing conversation is
 * one GET whose body is already in the SDK's message shape.
 */
export function useAgentChat(conversationId: string, initialMessages: UIMessage[] = []) {
    const agentStore = useAgentStore()

    const chat = useChat({
        id: conversationId,
        messages: initialMessages,
        transport: new DefaultChatTransport({
            api: `/api/agent/conversations/${conversationId}/chat`,
            credentials: 'include',
        }),
        sendAutomaticallyWhen: lastAssistantMessageIsCompleteWithToolCalls,
        onFinish: () => {
            const first = chat.messages.value.find(m => m.role === 'user')
            const text = first?.parts.find(p => p.type === 'text')
            if (text && 'text' in text) agentStore.setTitle(conversationId, text.text)
        },
    })

    const busy = computed(() => chat.status.value === 'submitted' || chat.status.value === 'streaming')

    function send(text: string) {
        if (!text.trim() || busy.value) return
        chat.sendMessage({ text })
    }

    return { ...chat, busy, send }
}
