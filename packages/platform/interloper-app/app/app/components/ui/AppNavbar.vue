<script setup lang="ts">
/**
 * Page navbar: sidebar toggle, the page title (or the page's own #title
 * content, such as a crumb and status), search, the page's #right controls,
 * then the agent toggle wherever the layout hosts the agent panel.
 */
defineProps<{
    title?: string
}>()

const agentHost = inject(AGENT_PANEL_HOST, false)
const userStore = useUserStore()
const { open: agentOpen } = useAgentPanel()
const wide = useMediaQuery('(min-width: 640px)')
</script>

<template>
    <UDashboardNavbar :title="title">
        <template #leading>
            <UDashboardSidebarCollapse />
        </template>
        <template v-if="$slots.title"
                  #title>
            <slot name="title" />
        </template>
        <template #right>
            <UDashboardSearchButton :collapsed="!wide"
                                    class="bg-elevated ring-default sm:w-60" />
            <slot name="right" />
            <UButton v-if="agentHost && userStore.agentAvailable"
                     icon="icon:agent"
                     :variant="agentOpen ? 'soft' : 'solid'"
                     square
                     aria-label="Toggle agent panel"
                     @click="agentOpen = !agentOpen" />
        </template>
    </UDashboardNavbar>
</template>
