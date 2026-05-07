import { LazyConfirmModal } from '#components'

interface ConfirmOptions {
    title?: string
    description?: string
    confirmLabel?: string
    cancelLabel?: string
    confirmColor?: 'error' | 'primary' | 'neutral'
    icon?: string
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
