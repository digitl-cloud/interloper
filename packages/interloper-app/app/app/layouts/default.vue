<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()

const userStore = useUserStore()
const {
    open: searchOpen,
    searchTerm,
    loading: searchLoading,
    groups: searchGroups,
} = useCommandPalette()
const { open: agentOpen, width: agentWidth, dragging: agentDragging } = useAgentPanel()
provide(AGENT_PANEL_HOST, true)

const destinations = useNavDestinations()

/**
 * Hubs start folded and open whenever the route enters one, so the active
 * view is never hidden; the user can fold them again.
 */
const openMenus = ref<string[]>([])
watch(() => route.path, (path) => {
    for (const page of destinations.value) {
        if (page.views && isNavActive(page, path) && !openMenus.value.includes(page.to)) openMenus.value.push(page.to)
    }
}, { immediate: true })

/** A hub lights up only while its views are hidden (folded, or a collapsed sidebar); otherwise its active view does. */
function navItems(collapsed: boolean): NavigationMenuItem[] {
    return destinations.value.map(page => ({
        value: page.to,
        label: page.label,
        icon: page.icon,
        to: page.to,
        active: isNavActive(page, route.path) && (!page.views || collapsed || !openMenus.value.includes(page.to)),
        children: page.views?.map(view => ({
            label: view.label,
            icon: view.icon,
            to: view.to,
            active: route.path === view.to || route.path.startsWith(`${view.to}/`),
        })),
    }))
}
</script>

<template>
    <div>
        <UDashboardGroup storage-key="dashboard-data"
                         :style="{ right: agentOpen && userStore.agentAvailable ? `${agentWidth}px` : '0px' }"
                         :ui="{ base: `fixed top-0 bottom-0 left-0 flex overflow-hidden ${agentDragging ? '' : 'transition-[right] duration-300'}` }">
            <UDashboardSidebar collapsible
                               resizable
                               :ui="{ footer: 'border-t border-default' }">
                <template #header="{ collapsed }">
                    <NavLogo v-if="!collapsed" />
                    <LogoIcon v-else
                              class="mx-auto h-6 w-auto text-primary" />
                </template>

                <template #default="{ collapsed }">
                    <UDashboardSearchButton :collapsed="collapsed"
                                            class="bg-transparent ring-default" />
                    <UNavigationMenu v-model="openMenus"
                                     :collapsed="collapsed"
                                     :items="navItems(collapsed)"
                                     type="multiple"
                                     popover
                                     color="neutral"
                                     orientation="vertical" />
                </template>

                <template #footer="{ collapsed }">
                    <div class="flex flex-col gap-1 w-full">
                        <NavOrganisation :collapsed="collapsed" />
                        <NavUser :collapsed="collapsed" />
                    </div>
                </template>
            </UDashboardSidebar>

            <slot />

            <UDashboardSearch v-model:open="searchOpen"
                              v-model:search-term="searchTerm"
                              :groups="searchGroups"
                              :loading="searchLoading"
                              :color-mode="false"
                              :fuse="{ fuseOptions: { keys: ['label', 'suffix', 'keywords'] } }"
                              placeholder="Search..." />
        </UDashboardGroup>

        <AgentPanel v-if="userStore.agentAvailable" />
    </div>
</template>
