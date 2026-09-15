/** Agent session returned by the API. */
export interface AgentSession {
    id: string
    user_id: string
    app_name: string
    state: Record<string, any>
    last_update_time: number
    event_count: number
}

/** A tool call the model emitted. `id` is absent on providers that don't mint one. */
export interface AgentFunctionCall {
    id?: string
    name: string
    args?: Record<string, any>
}

/** The result of a tool call, carrying the `id` of the call it answers when there is one. */
export interface AgentFunctionResponse {
    id?: string
    name: string
    response?: Record<string, any>
}

/** A single part within an ADK event content. */
export interface AgentEventPart {
    text?: string
    /**
     * Set on the model's thought summaries. The ADK leaves them on the event and
     * merely keeps them out of the answer it assembles, so anything reading
     * `text` has to tell the two apart itself.
     */
    thought?: boolean
    functionCall?: AgentFunctionCall
    functionResponse?: AgentFunctionResponse
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

/** How far along a tool call or handover is. */
export type AgentActivityState = 'running' | 'done' | 'error'

/**
 * One step the agent took on its way to an answer: a tool call, or a handover
 * to one of its specialists.
 *
 * Built from the functionCall/functionResponse parts the ADK already streams,
 * so a turn that spends twenty seconds in tools shows what it is doing rather
 * than nothing at all.
 */
export interface AgentActivity {
    id: string
    /** The tool's function name, or the target agent's name for a handover. */
    name: string
    kind: 'tool' | 'transfer'
    state: AgentActivityState
    args?: Record<string, any>
    response?: Record<string, any>
}

/** A thought summary the model emitted on its way to an answer. */
export interface AgentThought {
    id: string
    kind: 'thought'
    text: string
}

/**
 * One entry in a turn's work trail: what the model thought, or what it did.
 *
 * Kept as a single ordered list because the two interleave, and the order is
 * the account: thought, the tool it reached for, what it made of the result.
 */
export type AgentStep = AgentThought | AgentActivity

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
    /** When set, the message renders the turn's work trail: thoughts and steps, in order. */
    steps?: AgentStep[]
    /** Whole seconds that trail has taken so far. */
    workSeconds?: number
}
