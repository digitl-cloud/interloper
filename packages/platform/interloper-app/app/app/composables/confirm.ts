import { LazyConfirmModal } from '#components'
import type { ConfirmOptions } from '~/components/ui/ConfirmModal.vue'

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
