<script setup lang="ts">
/**
 * The assistant's mark beside a message: the dot matrix at work while the
 * turn is still running, the agent's mark once it has answered. Sized like the
 * message's own leading icon: `sm` in the compact panel, `md` on the page.
 */
withDefaults(defineProps<{ live?: boolean, size?: 'sm' | 'md' }>(), { live: false, size: 'md' })

const appConfig = useAppConfig()
</script>

<template>
    <span class="flex items-center justify-center shrink-0 text-primary"
          :class="size === 'sm' ? 'size-5' : 'size-7'">
        <AgentIndicator v-if="live"
                        :class="size === 'sm' ? 'size-4' : 'size-5'" />
        <UIcon v-else
               :name="appConfig.icons.agent"
               class="size-full" />
    </span>
</template>
