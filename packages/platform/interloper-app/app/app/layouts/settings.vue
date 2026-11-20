<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()
const appConfig = useAppConfig()

/** One link per settings page; Authentication's views are the tabs on its page. */
const navItems = computed<NavigationMenuItem[]>(() => [
    {
        label: 'Profile',
        icon: appConfig.icons.user,
        to: '/settings/profile',
        active: route.path === '/settings/profile',
    },
    {
        label: 'Authentication',
        icon: appConfig.icons.authentication,
        to: '/settings/authentication',
        active: route.path.startsWith('/settings/authentication'),
    },
])
</script>

<template>
    <UDashboardGroup storage="local"
                     storage-key="dashboard-settings">
        <UDashboardSidebar collapsible
                           resizable>
            <template #header="{ collapsed }">
                <NavLogo v-if="!collapsed" />
                <LogoIcon v-else
                          class="mx-auto h-6 text-primary" />
            </template>

            <template #default="{ collapsed }">
                <UBadge v-if="!collapsed"
                        color="neutral"
                        size="lg"
                        class="caption w-full justify-center"
                        label="User settings" />
                <UNavigationMenu :collapsed="collapsed"
                                 :items="navItems"
                                 color="neutral"
                                 orientation="vertical" />
            </template>

            <template #footer="{ collapsed }">
                <div class="flex w-full flex-col gap-1">
                    <UButton :label="collapsed ? undefined : 'Back to app'"
                             :icon="appConfig.ui.icons.arrowLeft"
                             color="neutral"
                             variant="ghost"
                             block
                             class="justify-start"
                             :square="collapsed"
                             :aria-label="collapsed ? 'Back to app' : undefined"
                             to="/" />
                    <NavUser :collapsed="collapsed" />
                </div>
            </template>
        </UDashboardSidebar>

        <slot />
    </UDashboardGroup>
</template>
