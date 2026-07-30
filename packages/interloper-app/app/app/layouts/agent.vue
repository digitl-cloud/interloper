<script setup lang="ts">

const route = useRoute()
const agentStore = useAgentStore()

const sidebarItems = computed(() => {
    if (!agentStore.sessions.length) return []

    return [
        { label: 'Recent', type: 'label' as const, class: 'mt-2' },
        ...agentStore.sessions.map(s => ({
            id: s.id,
            label: `Chat ${s.id.slice(0, 8)}`,
            icon: 'i-lucide-message-square',
            to: `/agent/chat/${s.id}`,
            slot: 'chat' as const,
        })),
    ]
})

async function deleteChat(id: string) {
    await agentStore.deleteSession(id)
    if (route.params.id === id) {
        navigateTo('/agent')
    }
}

onMounted(() => {
    agentStore.fetchSessions()
})
</script>

<template>
    <div>
        <UDashboardGroup storage-key="dashboard-ai">
            <UDashboardSidebar collapsible
                               resizable
                               :ui="{ footer: 'border-t border-default' }">
                <template #header="{ collapsed }">
                    <NavLogo v-if="!collapsed" />
                    <UDashboardSidebarCollapse :class="collapsed ? 'mx-auto' : 'ms-auto'" />
                </template>

                <template #default="{ collapsed }">
                    <UButton :label="collapsed ? undefined : 'New chat'"
                             icon="i-lucide-plus"
                             color="primary"
                             block
                             :square="collapsed"
                             to="/agent" />

                    <UNavigationMenu v-if="sidebarItems.length"
                                     :collapsed="collapsed"
                                     :items="sidebarItems"
                                     orientation="vertical"
                                     :ui="{ link: 'overflow-hidden' }">
                        <template #chat-trailing="{ item }">
                            <div class="flex -mr-1.25 translate-x-full group-hover:translate-x-0 transition-transform">
                                <UButton icon="i-lucide-x"
                                         color="neutral"
                                         variant="ghost"
                                         size="xs"
                                         class="text-muted hover:text-primary hover:bg-accented/50 p-0.5"
                                         tabindex="-1"
                                         @click.stop.prevent="deleteChat((item as any).id)" />
                            </div>
                        </template>
                    </UNavigationMenu>

                    <div v-else-if="!collapsed"
                         class="mt-4 px-2 text-sm text-muted text-center">
                        No conversations yet
                    </div>
                </template>

                <template #footer="{ collapsed }">
                    <NavUser :collapsed="collapsed" />
                </template>
            </UDashboardSidebar>

            <UDashboardPanel :ui="{ body: '!p-0 !gap-0 overflow-hidden' }">
                <template #body>
                    <slot />
                </template>
            </UDashboardPanel>
        </UDashboardGroup>
    </div>
</template>
