<script setup lang="ts">
/**
 * What the older turns of a conversation were compacted into, above the
 * messages that remain. Closed by default: the row says it happened, the
 * summary itself is one click away.
 */
defineProps<{ text: string }>()
</script>

<template>
    <UCollapsible class="w-full">
        <button type="button"
                class="group flex items-center gap-1.5 w-full min-h-6 text-left text-sm text-muted hover:text-highlighted transition-colors cursor-pointer">
            <UIcon name="i-lucide-history"
                   class="size-4 shrink-0" />
            <span>Earlier conversation summarised</span>
            <UIcon name="i-lucide-chevron-down"
                   class="size-4 shrink-0 transition-transform group-data-[state=open]:rotate-180" />
        </button>

        <template #content>
            <div class="mt-2 rounded-lg border border-default bg-elevated/50 px-3 py-2.5 text-[13px] text-toned">
                <Markdown :value="text"
                          class="chat-prose *:first:mt-0 *:last:mb-0" />
            </div>
        </template>
    </UCollapsible>
</template>
