<script setup lang="ts">
/**
 * Wizard "pick an option" card: bordered card that shows the accent
 * border + tint when selected. Content comes from the default slot.
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
               class="rounded-[13px] border-2 text-left cursor-pointer transition"
               :class="selected
                   ? 'border-primary bg-primary/5'
                   : 'border-default bg-default hover:border-primary/40'"
               @click="emit('select')">
        <slot />
    </component>
</template>
