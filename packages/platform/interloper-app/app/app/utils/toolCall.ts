import type { UIMessage } from 'ai'
import { isToolUIPart } from 'ai'

/** A tool call that failed: the SDK's error state, or a result the toolkit reports as an error. */
export function isFailedToolCall(part: UIMessage['parts'][number]): boolean {
    if (!isToolUIPart(part)) return false
    if (part.state === 'output-error') return true
    const output: unknown = part.state === 'output-available' ? part.output : undefined
    return typeof output === 'object' && output !== null && (output as { status?: string }).status === 'error'
}
