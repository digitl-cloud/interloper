<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()

const items = computed<NavigationMenuItem[]>(() => [
    {
        label: 'Overview',
        icon: 'i-lucide-layout-dashboard',
        to: '/admin',
        active: route.path === '/admin',
    },
    {
        label: 'Organisations',
        icon: 'i-lucide-building-2',
        to: '/admin/organisations',
        active: route.path.startsWith('/admin/organisations'),
    },
    {
        label: 'Users',
        icon: 'i-lucide-users',
        to: '/admin/users',
        active: route.path.startsWith('/admin/users'),
    },
    {
        label: 'Config',
        icon: 'i-lucide-settings-2',
        to: '/admin/config',
        active: route.path.startsWith('/admin/config'),
    },
])
</script>

<template>
    <UDashboardGroup storage-key="dashboard-admin">
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
                        color="primary"
                        size="lg"
                        class="eyebrow w-full justify-center py-2"
                        label="Admin portal" />
                <UNavigationMenu :collapsed="collapsed"
                                 :items="items"
                                 color="neutral"
                                 orientation="vertical" />
            </template>

            <template #footer="{ collapsed }">
                <div class="flex flex-col gap-1 w-full">
                    <UButton :label="collapsed ? undefined : 'Exit Admin'"
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
