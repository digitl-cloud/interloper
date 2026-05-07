/** Agent session returned by the API. */
export interface AgentSession {
    id: string
    user_id: string
    app_name: string
    state: Record<string, any>
    last_update_time: number
    event_count: number
}

/** A single part within an ADK event content. */
export interface AgentEventPart {
    text?: string
    functionCall?: { name: string; args: Record<string, any> }
    functionResponse?: { name: string; response: Record<string, any> }
}

/** ADK event content. */
export interface AgentEventContent {
    role?: string
    parts?: AgentEventPart[]
}

/** ADK event actions. */
export interface AgentEventActions {
    stateDelta?: Record<string, any>
    transferToAgent?: string
}

/** A single ADK event from the SSE stream. */
export interface AgentEvent {
    id: string
    invocationId: string
    author: string
    content?: AgentEventContent
    actions?: AgentEventActions
    partial?: boolean
    timestamp?: number
}

/** Simplified chat message for UI rendering. */
export interface ChatMessage {
    id: string
    role: 'user' | 'assistant'
    text: string
    loading?: boolean
}
