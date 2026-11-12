<script setup lang="ts">
/**
 * Page navbar, the same on every page: sidebar toggle, the page title (or the
 * page's own #title content, such as a breadcrumb and status), search, then the
 * agent toggle wherever the layout hosts the agent panel. It takes no actions:
 * an action sits with the card or toolbar it belongs to.
 */
defineProps<{
    title?: string
}>()

const agentHost = inject(AGENT_PANEL_HOST, false)
const userStore = useUserStore()
const appConfig = useAppConfig()
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
                                    class="sm:w-60" />
            <UButton v-if="agentHost && userStore.agentAvailable"
                     :icon="appConfig.icons.agent"
                     :variant="agentOpen ? 'soft' : 'solid'"
                     square
                     aria-label="Toggle agent panel"
                     @click="agentOpen = !agentOpen" />
        </template>
    </UDashboardNavbar>
</template>
