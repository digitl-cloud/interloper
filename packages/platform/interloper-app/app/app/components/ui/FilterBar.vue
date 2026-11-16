<script setup lang="ts">
/**
 * The row above a list or canvas: its search, its filters and its actions.
 * Wide, they share the row: search and filters at the start, actions at the
 * end. On a phone the row keeps the search and the actions, as icons, and the
 * filters move into a bottom sheet behind a Filters button that counts the
 * active ones.
 */
withDefaults(defineProps<{
    /** How many filters differ from their default: the Filters button's count, and whether the sheet offers Reset. */
    active?: number
}>(), { active: 0 })

const emit = defineEmits<{ reset: [] }>()

const appConfig = useAppConfig()
const wide = useMediaQuery('(min-width: 640px)')
const sheetOpen = ref(false)
provide(FILTER_BAR_STACKED, computed(() => !wide.value))
</script>

<template>
    <div v-if="wide"
         class="flex flex-wrap items-center justify-between gap-3">
        <div class="flex min-w-0 flex-wrap items-center gap-x-4 gap-y-3">
            <slot name="search" />
            <slot />
        </div>
        <div v-if="$slots.actions"
             class="flex items-center gap-2">
            <slot name="actions" />
        </div>
    </div>

    <div v-else
         class="flex items-center gap-2">
        <div v-if="$slots.search"
             class="min-w-0 flex-1">
            <slot name="search" />
        </div>
        <UChip v-if="$slots.default"
               :show="active > 0"
               :text="active"
               size="3xl">
            <UButton :icon="appConfig.icons.filter"
                     :label="$slots.search ? undefined : 'Filters'"
                     aria-label="Filters"
                     color="neutral"
                     variant="outline"
                     @click="sheetOpen = true" />
        </UChip>
        <UTheme v-if="$slots.actions"
                :props="{ button: { square: true } }"
                :ui="{ button: { label: 'sr-only' } }">
            <div class="ms-auto flex shrink-0 items-center gap-2">
                <slot name="actions" />
            </div>
        </UTheme>

        <UDrawer v-if="$slots.default"
                 v-model:open="sheetOpen"
                 title="Filters">
            <template #actions>
                <UButton v-if="active > 0"
                         label="Reset"
                         color="neutral"
                         variant="link"
                         @click="emit('reset')" />
            </template>
            <template #body>
                <div class="flex flex-col gap-6">
                    <slot />
                </div>
            </template>
        </UDrawer>
    </div>
</template>
