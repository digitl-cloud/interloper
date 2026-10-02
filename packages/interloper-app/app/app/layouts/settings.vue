<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()

/** Authentication lights up only while the collapsed sidebar hides its views; otherwise its active view does. */
function navItems(collapsed: boolean): NavigationMenuItem[] {
    const onAuth = route.path.startsWith('/settings/authentication')
    const tab = route.query.tab
    return [
        {
            label: 'Profile',
            icon: 'i-lucide-user',
            to: '/settings/profile',
            active: route.path === '/settings/profile',
        },
        {
            label: 'Authentication',
            icon: 'i-lucide-shield-check',
            // Collapsed by default, but never hide the active child.
            defaultOpen: onAuth,
            active: onAuth && collapsed,
            children: [
                {
                    label: 'Sign in',
                    to: { path: '/settings/authentication', query: { tab: 'signin' } },
                    active: onAuth && tab !== 'tokens',
                },
                {
                    label: 'Personal Access Tokens',
                    to: { path: '/settings/authentication', query: { tab: 'tokens' } },
                    active: onAuth && tab === 'tokens',
                },
            ],
        },
    ]
}
</script>

<template>
    <UDashboardGroup storage-key="dashboard-settings">
        <UDashboardSidebar collapsible
                           resizable
                           :ui="{ footer: 'border-t border-default' }">
            <template #header="{ collapsed }">
                <NavLogo v-if="!collapsed" />
                <LogoIcon v-else
                          class="mx-auto h-6 w-auto text-primary" />
            </template>

            <template #default="{ collapsed }">
                <UBadge v-if="!collapsed"
                        color="neutral"
                        variant="soft"
                        size="lg"
                        class="eyebrow w-full justify-center py-2"
                        label="User Settings" />
                <UNavigationMenu :collapsed="collapsed"
                                 :items="navItems(collapsed)"
                                 color="neutral"
                                 orientation="vertical" />
            </template>

            <template #footer="{ collapsed }">
                <div class="flex flex-col gap-1 w-full">
                    <UButton :label="collapsed ? undefined : 'Back to app'"
                             icon="i-lucide-arrow-left"
                             color="neutral"
                             variant="ghost"
                             block
                             class="justify-start"
                             :square="collapsed"
                             @click="navigateTo('/')" />
                    <NavUser :collapsed="collapsed" />
                </div>
            </template>
        </UDashboardSidebar>

        <slot />
    </UDashboardGroup>
</template>
