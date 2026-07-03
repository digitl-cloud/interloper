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
        <UDashboardGroup storage-key="dashboard-ai"
                         :ui="{ base: 'fixed inset-0 flex flex-col overflow-hidden' }">
            <UDashboardNavbar>
                <template #leading>
                    <UDashboardSidebarCollapse />
                </template>
                <template #left>
                    <NuxtLink to="/"
                              class="flex items-center gap-2">
                        <div class="bg-primary text-primary-foreground flex aspect-square size-6 items-center justify-center rounded-md">
                            <img src="/favicon.ico"
                                 alt="Interloper"
                                 class="size-3.5">
                        </div>
                        <span class="font-semibold text-sm">Interloper</span>
                    </NuxtLink>
                </template>

            </UDashboardNavbar>

            <div class="flex flex-1 min-h-0 overflow-hidden">
                <UDashboardSidebar collapsible
                                   resizable
                                   :ui="{ root: 'relative hidden lg:flex flex-col min-h-full min-w-16 w-(--width) shrink-0 border-e border-default', footer: 'border-t border-default' }">
                    <template #default="{ collapsed }">
                        <UButton :label="collapsed ? undefined : 'New chat'"
                                 icon="i-lucide-plus"
                                 color="primary"
                                 block
                                 class="mt-2"
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

                <UDashboardPanel :ui="{ root: 'relative flex flex-col min-w-0 min-h-full lg:not-last:border-e lg:not-last:border-default shrink-0 flex-1', body: '!p-0 !gap-0 overflow-hidden' }">
                    <template #body>
                        <slot />
                    </template>
                </UDashboardPanel>
            </div>
        </UDashboardGroup>
    </div>
</template>
