<script setup lang="ts" generic="T extends string">
/**
 * A filter, or a choice of how a list shows, kept behind a button so a toolbar
 * holds only what is used most. Inline (a wide `FilterBar`), the button names
 * the current choice and opens a menu of the options; in the phone's filter
 * sheet the options are a labelled list, as every other filter's are there.
 * The `leading` slot draws what goes before an option (a status dot) in both.
 */
const model = defineModel<T>({ required: true })

const props = defineProps<{
    /** What the options choose: the menu's heading, and the sheet's. */
    label: string
    /** The options; a `badge` is a count shown beside one. */
    items: Array<{ label: string, value: T, icon?: string, badge?: number }>
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
    // Each option keeps what the caller gave it, so the `leading` slot sees the same item in both.
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
               :items="items">
            <template v-if="$slots.leading"
                      #leading="{ item }">
                <slot name="leading"
                      :item="item" />
            </template>
        </UTabs>
    </FilterBarField>
    <UDropdownMenu v-else
                   :items="menu">
        <UButton :icon="icon"
                 :label="trigger"
                 :trailing-icon="appConfig.ui.icons.chevronDown"
                 color="neutral"
                 variant="soft" />
        <template #item-label="{ item }">
            {{ item.label }}<span v-if="'badge' in item && item.badge !== undefined"
                                  class="ms-1.5 text-dimmed tabular-nums">{{ item.badge }}</span>
        </template>
        <template v-if="$slots.leading"
                  #item-leading="{ item }">
            <slot name="leading"
                  :item="item" />
        </template>
    </UDropdownMenu>
</template>
