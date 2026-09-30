import type { UIMessage } from 'ai'

/** A conversation with the assistant, as the list shows it. */
export interface Conversation {
    id: string
    title: string | null
    created_at: string | null
    updated_at: string | null
}

/** A conversation with its history in the AI SDK's message shape; `summary` is what older turns were compacted into. */
export interface ConversationDetail extends Conversation {
    summary: string | null
    messages: UIMessage[]
}

/** One choice within a selection request. */
export interface SelectionOption {
    label: string
    value: string
}

/** The input of a `request_user_selection` tool call. */
export interface SelectionRequest {
    prompt: string
    options: SelectionOption[]
    multi?: boolean
}

/** The input of a `request_connection_setup` tool call. */
export interface ConnectionSetupRequest {
    connection_key: string
    name?: string | null
}

/** What the app reports back for a `request_connection_setup` call. */
export interface ConnectionSetupResult {
    connection_id: string
    name: string
    verified: boolean
}
