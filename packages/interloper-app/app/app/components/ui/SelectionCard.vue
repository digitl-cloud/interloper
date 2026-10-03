<script setup lang="ts">
/**
 * Wizard "pick an option" tile: a container-tone card on the drawer's panel
 * tone, with the accent ring + tint when selected. Content comes from the
 * default slot.
 */
withDefaults(defineProps<{
    selected?: boolean
    /** Root element — `button` for single-select rows, `div` when the card hosts interactive children. */
    as?: 'button' | 'div'
}>(), { selected: false, as: 'button' })

const emit = defineEmits<{ select: [] }>()
</script>

<template>
    <component :is="as"
               :type="as === 'button' ? 'button' : undefined"
               class="rounded-lg text-left cursor-pointer transition"
               :class="selected
                   ? 'ring-2 ring-primary bg-primary/5'
                   : 'ring ring-default bg-muted hover:ring-primary/40'"
               @click="emit('select')">
        <slot />
    </component>
</template>
