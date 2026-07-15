import { LazyConfirmModal } from '#components'
import type { UsedByRef } from '~/utils/apiErrors'

interface ConfirmOptions {
    title?: string
    description?: string
    confirmLabel?: string
    cancelLabel?: string
    confirmColor?: 'error' | 'primary' | 'neutral'
    icon?: string
    /** Referrers that block the action — listed, confirm disabled. */
    blocking?: UsedByRef[]
    /** Referrers the target will be detached from — listed as a heads-up. */
    detaching?: UsedByRef[]
}

export function useConfirm() {
    const overlay = useOverlay()

    async function confirm(options: ConfirmOptions = {}): Promise<boolean> {
        const modal = overlay.create(LazyConfirmModal)
        const instance = modal.open(options)
        const result = await instance.result
        return result === true
    }

    return { confirm }
}
