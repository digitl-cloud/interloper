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

/** Inline connection-setup request emitted by the agent's request_connection_setup tool. */
export interface ConnectionSetupRequest {
    connectionKey: string
    name?: string
}

/** One choice within a selection request. */
export interface SelectionOption {
    label: string
    value: string
}

/** Inline selection request emitted by the agent's request_user_selection tool. */
export interface SelectionRequest {
    prompt: string
    options: SelectionOption[]
    multi: boolean
}

/** Inline confirmation summary emitted by the agent's request_confirmation tool. */
export interface ConfirmationRequest {
    title: string
    items: { label: string, value: string }[]
}

/** Simplified chat message for UI rendering. */
export interface ChatMessage {
    id: string
    role: 'user' | 'assistant'
    text: string
    loading?: boolean
    /** When set, the message renders the inline connection setup card. */
    connectionSetup?: ConnectionSetupRequest
    /** When set, the message renders the inline selection card. */
    selection?: SelectionRequest
    /** When set, the message renders the inline confirmation summary card. */
    confirmation?: ConfirmationRequest
}
