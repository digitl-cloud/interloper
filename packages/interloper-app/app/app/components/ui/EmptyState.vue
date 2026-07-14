<script setup lang="ts">
/** Design empty-state hero: gradient card with an accent icon tile. */
defineProps<{
    icon: string
    title: string
    description?: string
}>()
</script>

<template>
    <div class="w-full border border-default rounded-[18px] px-8 py-[42px] text-center bg-gradient-to-b from-[#FCFCFD] to-[#F6F7F9] dark:from-transparent dark:to-(--ui-bg-muted)">
        <div class="size-[58px] mx-auto rounded-[16px] bg-primary/10 text-primary flex items-center justify-center">
            <UIcon :name="icon"
                   class="size-7" />
        </div>
        <h2 class="text-[22px] font-bold tracking-[-0.02em] text-highlighted mt-4">
            {{ title }}
        </h2>
        <p v-if="description"
           class="text-[15px] text-muted leading-relaxed max-w-[560px] mx-auto mt-3">
            {{ description }}
        </p>
        <slot />
    </div>
</template>
