<script setup lang="ts">

const route = useRoute()
const appConfig = useAppConfig()
const toast = useToast()
const agentStore = useAgentStore()
const organisationStore = useOrganisationStore()

const sidebarItems = computed(() => {
    if (!agentStore.conversations.length) return []

    return [
        { label: 'Recent', type: 'label' as const, class: 'mt-2' },
        ...agentStore.conversations.map(c => ({
            id: c.id,
            label: c.title ?? 'New conversation',
            icon: appConfig.icons.conversation,
            to: organisationStore.path(`/agent/chat/${c.id}`),
            slot: 'chat' as const,
        })),
    ]
})

async function deleteChat(id: string) {
    try {
        await agentStore.deleteConversation(id)
    }
    catch (e) {
        toast.add(errorToast(e, "Couldn't delete the conversation"))
        return
    }
    if (route.params.id === id) {
        navigateTo(organisationStore.path('/agent'))
    }
}

// The layout outlives its pages, so it follows the organisation they are in.
watch(() => organisationStore.organisation?.id, () => agentStore.fetchConversations(), { immediate: true })
</script>

<template>
    <div>
        <UDashboardGroup storage="local"
                         storage-key="dashboard-ai">
            <UDashboardSidebar collapsible
                               resizable>
                <template #header="{ collapsed }">
                    <NavLogo v-if="!collapsed" />
                    <UDashboardSidebarCollapse :class="collapsed ? 'mx-auto' : 'ms-auto'" />
                </template>

                <template #default="{ collapsed }">
                    <UButton :label="collapsed ? undefined : 'New chat'"
                             :icon="appConfig.ui.icons.plus"
                             block
                             :square="collapsed"
                             :aria-label="collapsed ? 'New chat' : undefined"
                             :to="organisationStore.path('/agent')" />

                    <UNavigationMenu v-if="sidebarItems.length"
                                     :collapsed="collapsed"
                                     :items="sidebarItems"
                                     orientation="vertical"
                                     :ui="{ link: 'overflow-hidden' }">
                        <template #chat-trailing="{ item }">
                            <div class="-me-1 flex translate-x-full transition-transform group-hover:translate-x-0">
                                <UButton :icon="appConfig.icons.delete"
                                         color="neutral"
                                         variant="ghost"
                                         size="xs"
                                         aria-label="Delete conversation"
                                         tabindex="-1"
                                         @click.stop.prevent="deleteChat((item as any).id)" />
                            </div>
                        </template>
                    </UNavigationMenu>

                    <UEmpty v-else-if="!collapsed"
                            variant="naked"
                            size="sm"
                            title="No conversations yet" />
                </template>

                <template #footer="{ collapsed }">
                    <NavUser :collapsed="collapsed" />
                </template>
            </UDashboardSidebar>

            <slot />
        </UDashboardGroup>
    </div>
</template>
