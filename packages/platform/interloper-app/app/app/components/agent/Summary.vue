<script setup lang="ts">
/**
 * What the older turns of a conversation were compacted into, above the
 * messages that remain. Closed by default: the row says it happened, the
 * summary itself is one click away, in a card.
 */
defineProps<{ text: string }>()
</script>

<template>
    <AgentFold icon="i-lucide-message-square-text"
               text="Earlier conversation summarised">
        <UCard class="mt-2">
            <Markdown :value="text"
                      class="chat-prose *:first:mt-0 *:last:mb-0" />
        </UCard>
    </AgentFold>
</template>
