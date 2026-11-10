<script setup lang="ts">
/** Empty-state hero: a plain card with an accent icon tile, title, description and actions. */
defineProps<{
    icon: string
    title: string
    description?: string
}>()
</script>

<template>
    <UCard class="w-full"
           :ui="{ body: 'px-8 py-10 sm:px-8 sm:py-10 text-center' }">
        <div class="size-14 mx-auto rounded-lg bg-primary/10 text-primary flex items-center justify-center">
            <UIcon :name="icon"
                   class="size-7" />
        </div>
        <h2 class="text-2xl font-semibold text-highlighted mt-4">
            {{ title }}
        </h2>
        <p v-if="description"
           class="text-base text-muted leading-relaxed max-w-xl mx-auto mt-3">
            {{ description }}
        </p>
        <slot />
    </UCard>
</template>
