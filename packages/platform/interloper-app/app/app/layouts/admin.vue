<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()
const appConfig = useAppConfig()

const items = computed<NavigationMenuItem[]>(() => [
    {
        label: 'Overview',
        icon: appConfig.icons.overview,
        to: '/admin',
        active: route.path === '/admin',
    },
    {
        label: 'Organisations',
        icon: appConfig.icons.organisation,
        to: '/admin/organisations',
        active: route.path.startsWith('/admin/organisations'),
    },
    {
        label: 'Users',
        icon: appConfig.icons.user,
        to: '/admin/users',
        active: route.path.startsWith('/admin/users'),
    },
    {
        label: 'OAuth clients',
        icon: appConfig.icons.app,
        to: '/admin/oauth-clients',
        active: route.path.startsWith('/admin/oauth-clients'),
    },
    {
        label: 'Config',
        icon: appConfig.icons.settings,
        to: '/admin/config',
        active: route.path.startsWith('/admin/config'),
    },
])
</script>

<template>
    <UDashboardGroup storage="local"
                     storage-key="dashboard-admin">
        <UDashboardSidebar collapsible
                           resizable>
            <template #header="{ collapsed }">
                <NavLogo v-if="!collapsed" />
                <LogoIcon v-else
                          class="mx-auto h-6 text-primary" />
            </template>

            <template #default="{ collapsed }">
                <UBadge v-if="!collapsed"
                        size="lg"
                        class="caption w-full justify-center"
                        label="Admin portal" />
                <UNavigationMenu :collapsed="collapsed"
                                 :items="items"
                                 color="neutral"
                                 orientation="vertical" />
            </template>

            <template #footer="{ collapsed }">
                <div class="flex w-full flex-col gap-1">
                    <UButton :label="collapsed ? undefined : 'Exit admin'"
                             :icon="appConfig.ui.icons.arrowLeft"
                             color="neutral"
                             variant="ghost"
                             block
                             class="justify-start"
                             :square="collapsed"
                             :aria-label="collapsed ? 'Exit admin' : undefined"
                             to="/" />
                    <NavUser :collapsed="collapsed" />
                </div>
            </template>
        </UDashboardSidebar>

        <slot />
    </UDashboardGroup>
</template>
