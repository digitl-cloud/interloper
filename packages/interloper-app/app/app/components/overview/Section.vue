<script setup lang="ts">
/** Design section: h2 + muted meta on the left, actions / accent link on the right, content below. */
defineProps<{
    title: string
    meta?: string
    linkLabel?: string
    linkTo?: string
}>()
</script>

<template>
    <section class="min-w-0">
        <div class="mb-3 flex flex-wrap items-center gap-3">
            <h2 class="text-[15px] font-semibold tracking-[-0.01em] text-highlighted whitespace-nowrap">{{ title }}</h2>
            <span v-if="meta"
                  class="min-w-0 truncate text-[12.5px] text-dimmed">{{ meta }}</span>
            <div class="ml-auto flex items-center gap-2.5">
                <slot name="actions" />
                <ULink v-if="linkLabel && linkTo"
                       :to="linkTo"
                       class="whitespace-nowrap text-[12.5px] font-medium text-primary">{{ linkLabel }}</ULink>
            </div>
        </div>
        <slot />
    </section>
</template>
