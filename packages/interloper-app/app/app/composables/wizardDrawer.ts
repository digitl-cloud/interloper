/**
 * State machine for the create/edit wizard drawers: open flag, the item
 * being edited (null = create), and an optional preselected type key
 * (from the empty-state catalogs) forwarded to the stepper.
 */
export function useWizardDrawer<T>() {
    const open = ref(false)
    const editing = ref<T | null>(null) as Ref<T | null>
    const presetTypeKey = ref<string>()

    function openCreate() {
        editing.value = null
        presetTypeKey.value = undefined
        open.value = true
    }

    function openCreateWithType(key: string) {
        editing.value = null
        presetTypeKey.value = key
        open.value = true
    }

    function openEdit(item: T) {
        editing.value = item
        presetTypeKey.value = undefined
        open.value = true
    }

    function close() {
        open.value = false
    }

    return { open, editing, presetTypeKey, openCreate, openCreateWithType, openEdit, close }
}
