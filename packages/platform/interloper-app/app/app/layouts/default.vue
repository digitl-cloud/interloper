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
function buildNavItems(collapsed: boolean): NavigationMenuItem[] {
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

// One stable list per sidebar state: built inside the sidebar's slot, a new list on every render
// would re-render the whole menu on each pointer move while the sidebar is resized.
const expandedNavItems = computed(() => buildNavItems(false))
const collapsedNavItems = computed(() => buildNavItems(true))
</script>

<template>
    <div>
        <UDashboardGroup storage="local"
                         storage-key="dashboard-data">
            <UDashboardSidebar collapsible
                               resizable>
                <template #header="{ collapsed }">
                    <NavLogo v-if="!collapsed" />
                    <LogoIcon v-else
                              class="mx-auto h-6 text-primary" />
                </template>

                <template #default="{ collapsed }">
                    <UNavigationMenu v-model="openMenus"
                                     :collapsed="collapsed"
                                     :items="collapsed ? collapsedNavItems : expandedNavItems"
                                     type="multiple"
                                     popover
                                     color="neutral"
                                     orientation="vertical" />
                </template>

                <template #footer="{ collapsed }">
                    <div class="flex w-full flex-col gap-1">
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
                              placeholder="Search…" />
        </UDashboardGroup>

        <AgentPanel v-if="userStore.agentAvailable" />
    </div>
</template>
