<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()
const appConfig = useAppConfig()

/** Authentication lights up only while the collapsed sidebar hides its views; otherwise its active view does. */
function buildNavItems(collapsed: boolean): NavigationMenuItem[] {
    const onAuth = route.path.startsWith('/settings/authentication')
    const tab = route.query.tab
    return [
        {
            label: 'Profile',
            icon: appConfig.icons.user,
            to: '/settings/profile',
            active: route.path === '/settings/profile',
        },
        {
            label: 'Authentication',
            icon: appConfig.icons.authentication,
            // Collapsed by default, but never hide the active child.
            defaultOpen: onAuth,
            active: onAuth && collapsed,
            children: [
                {
                    label: 'Sign in',
                    icon: appConfig.icons.signIn,
                    to: { path: '/settings/authentication', query: { tab: 'signin' } },
                    active: onAuth && tab !== 'tokens' && tab !== 'apps',
                },
                {
                    label: 'Personal access tokens',
                    icon: appConfig.icons.token,
                    to: { path: '/settings/authentication', query: { tab: 'tokens' } },
                    active: onAuth && tab === 'tokens',
                },
                {
                    label: 'Authorised apps',
                    icon: appConfig.icons.app,
                    to: { path: '/settings/authentication', query: { tab: 'apps' } },
                    active: onAuth && tab === 'apps',
                },
            ],
        },
    ]
}

// One stable list per sidebar state: built inside the sidebar's slot, a new list on every render
// would re-render the whole menu on each pointer move while the sidebar is resized.
const expandedNavItems = computed(() => buildNavItems(false))
const collapsedNavItems = computed(() => buildNavItems(true))
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
                                 :items="collapsed ? collapsedNavItems : expandedNavItems"
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
