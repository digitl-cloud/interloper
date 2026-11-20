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

/** The sidebar names each hub once; its views are the tabs on its pages. */
const navItems = computed<NavigationMenuItem[]>(() => destinations.value.map(page => ({
    label: page.label,
    icon: page.icon,
    to: page.to,
    active: isNavActive(page, route.path),
})))
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
                    <UNavigationMenu :collapsed="collapsed"
                                     :items="navItems"
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
