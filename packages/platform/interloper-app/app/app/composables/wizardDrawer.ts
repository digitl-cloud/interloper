/**
 * State machine for the create/edit wizard drawers: open flag, the item
 * being edited (null = create), and an optional preselected type key and
 * name (from the empty-state catalogs or a deep link) forwarded to the
 * stepper.
 */
export function useWizardDrawer<T>() {
    const open = ref(false)
    const editing = ref<T | null>(null) as Ref<T | null>
    const presetTypeKey = ref<string>()
    const presetName = ref<string>()

    function openCreate() {
        editing.value = null
        presetTypeKey.value = undefined
        presetName.value = undefined
        open.value = true
    }

    function openCreateWithType(key: string, name?: string) {
        editing.value = null
        presetTypeKey.value = key
        presetName.value = name
        open.value = true
    }

    function openEdit(item: T) {
        editing.value = item
        presetTypeKey.value = undefined
        presetName.value = undefined
        open.value = true
    }

    function close() {
        open.value = false
    }

    return { open, editing, presetTypeKey, presetName, openCreate, openCreateWithType, openEdit, close }
}
