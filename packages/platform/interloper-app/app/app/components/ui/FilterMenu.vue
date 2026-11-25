<script setup lang="ts" generic="T extends string">
/**
 * A choice of how a list shows, kept behind a button so the toolbar's inline
 * controls stay its filters. Inline (a wide `FilterBar`), the button names the
 * current choice and opens a menu of the options; in the phone's filter sheet
 * the options are a labelled list, as every filter's are there.
 */
const model = defineModel<T>({ required: true })

const props = defineProps<{
    /** What the options choose: the menu's heading, and the sheet's. */
    label: string
    /** The options. */
    items: Array<{ label: string, value: T, icon?: string }>
    /** The button's text for the current choice. */
    trigger: string
    /** The button's icon. */
    icon?: string
}>()

const appConfig = useAppConfig()
const bar = inject(FILTER_BAR_STACKED, null)
const stacked = computed(() => bar?.value ?? false)

const menu = computed(() => [[
    { type: 'label' as const, label: props.label },
    ...props.items.map(item => ({
        ...item,
        type: 'checkbox' as const,
        checked: model.value === item.value,
        onUpdateChecked: () => { model.value = item.value },
    })),
]])
</script>

<template>
    <FilterBarField v-if="stacked"
                    :label="label">
        <UTabs v-model="model"
               :items="items" />
    </FilterBarField>
    <UDropdownMenu v-else
                   :items="menu">
        <UButton :icon="icon"
                 :label="trigger"
                 :trailing-icon="appConfig.ui.icons.chevronDown"
                 color="neutral"
                 variant="soft" />
    </UDropdownMenu>
</template>
