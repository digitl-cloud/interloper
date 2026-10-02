<script setup lang="ts">
/**
 * A section title in the card theme's own title and description styles, with
 * optional controls (default slot) on the right. Goes in a UCard's #header for
 * a content card, or above a card whose body is rows (a key/value or list
 * card), where a title inside would read as one of the rows.
 */
defineProps<{
    title?: string
    description?: string
}>()

const slots = useAppConfig().ui.card.slots
</script>

<template>
    <div class="flex flex-wrap items-center gap-3">
        <div class="min-w-0 flex-1">
            <div :class="slots.title">
                <slot name="title">{{ title }}</slot>
            </div>
            <div v-if="description"
                 :class="slots.description">{{ description }}</div>
        </div>
        <div v-if="$slots.default"
             class="flex shrink-0 items-center gap-2">
            <slot />
        </div>
    </div>
</template>
