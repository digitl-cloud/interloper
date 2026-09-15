import type {
    AgentActivity,
    AgentEvent,
    AgentFunctionCall,
    AgentFunctionResponse,
    AgentStep,
    ChatMessage,
} from '~/types/agent'

/** Tools whose response raises an interactive card instead of an activity row. */
const INTERACTION_TOOLS = new Set(['request_connection_setup', 'request_user_selection', 'request_confirmation'])

/** The ADK's built-in handover tool. */
const TRANSFER_TOOL = 'transfer_to_agent'

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

    /**
     * Status in the shape `UChatMessages` and `UChatPromptSubmit` expect.
     *
     * `submitted` covers the wait before the turn has anything to show, which is
     * what raises the messages list's own thinking indicator; from the first
     * thought or step onwards those rows report the work themselves.
     */
    const status = computed(() => {
        if (!streaming.value) return 'ready' as const
        return messages.value[messages.value.length - 1]?.role === 'user' ? 'submitted' as const : 'streaming' as const
    })

    /**
     * The message the running turn is still adding to, if any.
     *
     * A thought summary holding this id is still being reasoned out, which is
     * what puts its collapsible in the open, shimmering state.
     */
    const liveMessageId = computed(() =>
        streaming.value ? messages.value[messages.value.length - 1]?.id : undefined)


    /** Load existing messages from a session's event history. */
    async function loadHistory() {
        const agentStore = useAgentStore()
        try {
            const session = await agentStore.getSession(sessionId.value)
            if (!session?.events) return

            const restored: ChatMessage[] = []
            let previous: number | undefined
            for (const event of session.events) {
                _appendEvent(restored, event, _elapsed(previous, event.timestamp))
                previous = event.timestamp ?? previous
            }
            _settleRunning(restored)
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
        messages.value.push({ id: crypto.randomUUID(), role: 'user', text })
        streaming.value = true

        // ADK stamps events in epoch seconds, so the request start is the first
        // mark to measure the model's opening think against.
        let previous: number | undefined = Date.now() / 1000

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
                        // The message we just pushed locally comes back on the stream.
                        if (event.author === 'user') continue
                        _appendEvent(messages.value, event, _elapsed(previous, event.timestamp))
                        previous = event.timestamp ?? previous
                    }
                    catch {
                        // Skip malformed events
                    }
                }
            }
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            _settleRunning(messages.value)
            streaming.value = false
        }
    }

    return { messages, streaming, status, liveMessageId, error, send, loadHistory }
}

/**
 * Fold one ADK event into the message list.
 *
 * Parts are walked in order so the rendered conversation keeps the agent's own
 * chronology: what it said, the tools it then called, what it said after.
 */
function _appendEvent(messages: ChatMessage[], event: AgentEvent, elapsed?: number) {
    const role = event.author === 'user' ? 'user' as const : 'assistant' as const

    for (const part of event.content?.parts ?? []) {
        if (part.text && part.thought) _appendThought(messages, part.text, elapsed)
        else if (part.text) _appendText(messages, role, part.text)
        else if (part.functionCall) _startActivity(messages, part.functionCall)
        else if (part.functionResponse) _settleActivity(messages, part.functionResponse)
    }
}

/** Append text to the trailing message when it is plain prose from the same author, else start one. */
function _appendText(messages: ChatMessage[], role: 'user' | 'assistant', text: string) {
    const last = messages[messages.length - 1]
    const plain = last && !last.steps && !last.connectionSetup && !last.selection && !last.confirmation

    if (plain && last.role === role) last.text += text
    else messages.push({ id: crypto.randomUUID(), role, text })
}

/**
 * Append a thought summary to the trail, merging consecutive parts of the same one.
 *
 * The elapsed time belongs to the block the model spent it reaching, so only a
 * new block takes it; further parts of one already open are the same wait.
 */
function _appendThought(messages: ChatMessage[], text: string, elapsed?: number) {
    const steps = _trail(messages)
    const last = steps[steps.length - 1]

    if (last?.kind === 'thought') last.text += text
    else steps.push({ id: crypto.randomUUID(), kind: 'thought', text, seconds: elapsed })
}

/**
 * The steps of the trail the running turn is building, started if it has none.
 *
 * Plain prose closes a trail: once the agent has said something out loud, what
 * follows is a fresh stretch of work rather than more of the last one.
 */
function _trail(messages: ChatMessage[]): AgentStep[] {
    const last = messages[messages.length - 1]
    if (last?.steps) return last.steps

    const steps: AgentStep[] = []
    messages.push({ id: crypto.randomUUID(), role: 'assistant', text: '', steps })
    return steps
}

/**
 * Whole seconds between two ADK event timestamps.
 *
 * Undefined rather than zero when it rounds away or either end is missing:
 * the reasoning collapsible reads a zero as "still thinking", where it treats
 * an absent duration as the "no idea how long" it is.
 */
function _elapsed(previous?: number, current?: number) {
    if (previous === undefined || current === undefined) return undefined
    const seconds = Math.round(current - previous)
    return seconds > 0 ? seconds : undefined
}

/** Open a step in the trail for a tool call. */
function _startActivity(messages: ChatMessage[], call: AgentFunctionCall) {
    if (INTERACTION_TOOLS.has(call.name)) return

    const transfer = call.name === TRANSFER_TOOL
    _trail(messages).push({
        id: call.id || crypto.randomUUID(),
        name: transfer ? String(call.args?.agent_name ?? '') : call.name,
        kind: transfer ? 'transfer' : 'tool',
        state: 'running',
        args: transfer ? undefined : call.args,
    })
}

/** Close the activity a response answers, or raise the card an interaction tool stands for. */
function _settleActivity(messages: ChatMessage[], response: AgentFunctionResponse) {
    const card = _extractCard(response)
    if (card) {
        messages.push({ id: crypto.randomUUID(), role: 'assistant', text: '', ...card })
        return
    }
    if (INTERACTION_TOOLS.has(response.name)) return

    const activity = _findActivity(messages, response)
    if (!activity) return

    activity.response = response.response
    activity.state = _failed(response.response) ? 'error' : 'done'
}

/**
 * The card an interaction tool's response stands for, if any.
 *
 * Keys on the tool's function *response* (not the call), so a card only
 * renders for requests the tool validated against the catalog.
 */
function _extractCard(response: AgentFunctionResponse): Partial<ChatMessage> | null {
    const payload = response.response
    if (payload?.status !== 'success') return null

    switch (response.name) {
        case 'request_connection_setup':
            return payload.connection_key
                ? { connectionSetup: { connectionKey: payload.connection_key, name: payload.name ?? undefined } }
                : null
        case 'request_user_selection':
            return payload.options?.length
                ? { selection: { prompt: payload.prompt ?? '', options: payload.options, multi: !!payload.multi } }
                : null
        case 'request_confirmation':
            return payload.items?.length
                ? { confirmation: { title: payload.title ?? '', items: payload.items } }
                : null
        default:
            return null
    }
}

/**
 * The running activity a response belongs to.
 *
 * Responses carry the id of the call they answer when the provider mints one;
 * otherwise the most recent still-running call of the same name is the match.
 */
function _findActivity(messages: ChatMessage[], response: AgentFunctionResponse) {
    const matches = (step: AgentStep): step is AgentActivity => {
        if (step.kind === 'thought' || step.state !== 'running') return false
        if (response.id) return step.id === response.id
        return response.name === TRANSFER_TOOL ? step.kind === 'transfer' : step.name === response.name
    }

    for (let i = messages.length - 1; i >= 0; i--) {
        const found = messages[i]?.steps?.find(matches)
        if (found) return found
    }
    return undefined
}

/**
 * Whether a tool response reports a failure.
 *
 * The agent's own tools answer with an explicit status — and some report a
 * failed *subject* (an unreachable connection) as a successful call, so the
 * status wins where it exists. The toolkit-backed tools return a plain payload
 * and let the ADK wrap anything they raise as a bare `error` key.
 */
function _failed(payload: Record<string, any> | undefined) {
    if (!payload) return false
    if (payload.status) return payload.status === 'error'
    return 'error' in payload
}

/** Close whatever is still running when a turn ends, so no row spins forever. */
function _settleRunning(messages: ChatMessage[]) {
    for (const message of messages) {
        for (const step of message.steps ?? []) {
            if (step.kind !== 'thought' && step.state === 'running') step.state = 'done'
        }
    }
}
