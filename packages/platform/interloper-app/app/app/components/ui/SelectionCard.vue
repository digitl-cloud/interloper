<script setup lang="ts">
/**
 * Wizard "pick an option" tile: the card tone on the drawer's panel, ringed and
 * tinted in the accent colour when selected. Clicking anywhere on it emits
 * `select`.
 *
 * Given a `label`, it lays the tile out itself, so every picker reads the
 * same: a row of the `#leading` slot (a checkbox), the icon, the label over its
 * `description` and `#details`, and the `#trailing` slot on the right, with
 * the default slot underneath (a selected row's settings). `vertical` stacks
 * the icon over the label instead, for a grid of types, with `#trailing` in the
 * top corner. Without a `label`, the default slot is the whole content.
 */
const props = withDefaults(defineProps<{
    selected?: boolean
    /** Root element: `button` for single-select rows, `div` when the card hosts interactive children. */
    as?: 'button' | 'div'
    /** The option's name. */
    label?: string
    /** One line under the label. */
    description?: string
    icon?: string
    /** `vertical` stacks the icon over the label. */
    orientation?: 'horizontal' | 'vertical'
}>(), { selected: false, as: 'button', label: undefined, description: undefined, icon: undefined, orientation: 'horizontal' })

const emit = defineEmits<{ select: [] }>()
</script>

<template>
    <component :is="as"
               :type="as === 'button' ? 'button' : undefined"
               class="cursor-pointer rounded-lg text-left transition"
               :class="selected
                   ? 'bg-primary/10 ring-2 ring-primary'
                   : 'bg-muted ring ring-default hover:ring-primary/50'"
               @click="emit('select')">
        <div v-if="props.label && props.orientation === 'vertical'"
             class="relative flex flex-col items-center gap-2 px-3 py-4">
            <UIcon v-if="props.icon"
                   :name="props.icon"
                   class="size-6 shrink-0" />
            <span class="line-clamp-2 text-center text-sm font-semibold text-highlighted">{{ props.label }}</span>
            <div v-if="$slots.trailing"
                 class="absolute top-2 right-2">
                <slot name="trailing" />
            </div>
        </div>

        <template v-else-if="props.label">
            <div class="flex items-center gap-3 px-4 py-3">
                <slot name="leading" />
                <UIcon v-if="props.icon"
                       :name="props.icon"
                       class="size-5 shrink-0" />
                <div class="flex min-w-0 flex-col">
                    <span class="truncate text-sm font-semibold text-highlighted">{{ props.label }}</span>
                    <span v-if="props.description"
                          class="truncate text-xs text-muted">{{ props.description }}</span>
                    <slot name="details" />
                </div>
                <div v-if="$slots.trailing"
                     class="ml-auto flex shrink-0 items-center gap-2">
                    <slot name="trailing" />
                </div>
            </div>
            <slot />
        </template>

        <slot v-else />
    </component>
</template>
