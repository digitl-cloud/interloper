<script setup lang="ts">
/**
 * The model's thought summary for one turn.
 *
 * Open and shimmering while the turn is still reasoning, then folded away into
 * "Thought for N seconds", the way the Nuxt UI chat template presents its own.
 */
const props = defineProps<{ text: string, cacheKey: string, streaming?: boolean, duration?: number }>()
</script>

<template>
    <UChatReasoning :text="props.text"
                    :streaming="props.streaming"
                    :duration="props.duration"
                    chevron="leading">
        <MDC :value="props.text"
             :cache-key="props.cacheKey"
             class="*:first:mt-0 *:last:mb-0" />
    </UChatReasoning>
</template>
