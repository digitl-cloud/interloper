<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()

const modeItems: NavigationMenuItem[] = [
    { label: 'Data', icon: 'i-lucide-database', to: '/' },
    { label: 'Analytics', icon: 'i-lucide-chart-column', to: '/analytics', active: true },
    { label: 'Agent', icon: 'i-lucide-sparkles', to: '/agent' },
]

const sidebarItems = computed<NavigationMenuItem[]>(() => [
    {
        label: 'Dashboards',
        type: 'label',
    },
    {
        label: 'Overview',
        icon: 'i-lucide-layout-dashboard',
        to: '/analytics',
        active: route.path === '/analytics',
    },
    {
        label: 'Campaign Performance',
        icon: 'i-lucide-target',
        to: '/analytics/campaigns',
        active: route.path === '/analytics/campaigns',
    },
    {
        label: 'Marketing Mix Modeling',
        icon: 'i-lucide-blend',
        to: '/analytics/mmm',
        active: route.path === '/analytics/mmm',
    },
])
</script>

<template>
    <div>
        <UDashboardGroup storage-key="dashboard-analytics"
                         :ui="{ base: 'fixed inset-0 flex flex-col overflow-hidden' }">
            <UDashboardNavbar>
                <template #leading>
                    <UDashboardSidebarCollapse />
                </template>
                <template #left>
                    <NuxtLink to="/"
                              class="flex items-center gap-2">
                        <div
                             class="bg-primary text-primary-foreground flex aspect-square size-6 items-center justify-center rounded-md">
                            <img src="/favicon.ico"
                                 alt="Interloper"
                                 class="size-3.5">
                        </div>
                        <span class="font-semibold text-sm">Interloper</span>
                    </NuxtLink>
                </template>

                <UNavigationMenu :items="modeItems" />
            </UDashboardNavbar>

            <div class="flex flex-1 min-h-0 overflow-hidden">
                <UDashboardSidebar collapsible
                                   resizable
                                   :ui="{ root: 'relative hidden lg:flex flex-col min-h-full min-w-16 w-(--width) shrink-0 border-e border-default' }">
                    <template #default="{ collapsed }">
                        <UNavigationMenu :collapsed="collapsed"
                                         :items="sidebarItems"
                                         orientation="vertical" />
                    </template>
                </UDashboardSidebar>

                <UDashboardPanel
                                 :ui="{ root: 'relative flex flex-col min-w-0 min-h-full lg:not-last:border-e lg:not-last:border-default shrink-0 flex-1', body: '!p-0 !gap-0 overflow-hidden' }">
                    <template #body>
                        <slot />
                    </template>
                </UDashboardPanel>
            </div>
        </UDashboardGroup>
    </div>
</template>
