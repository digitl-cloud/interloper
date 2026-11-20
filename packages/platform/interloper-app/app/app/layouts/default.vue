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

const sections = useNavSections()
const withLastQuery = useLastQuery()

/** A sidebar link returns to its page as it was left: its view and its filters. */
function navItem(destination: NavDestination): NavigationMenuItem {
    return { label: destination.label, icon: destination.icon, to: withLastQuery(destination.to), active: isNavActive(destination, route.path) }
}

/** One list per section under its caption; a collapsed sidebar drops the captions and keeps the lists' gaps. */
const navItems = computed<NavigationMenuItem[][]>(() => sections.value.map(section => [
    ...(section.label ? [{ label: section.label, type: 'label' as const }] : []),
    ...section.destinations.map(navItem),
]))
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
