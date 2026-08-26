<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()
const appVersion = useRuntimeConfig().public.version

/** Design page header rendered by the layout, declared via definePageMeta({ pageHeader }). */
interface PageHeaderMeta {
    title: string
    description?: string
    eyebrow?: string
}
const pageHeader = computed(() => route.meta.pageHeader as PageHeaderMeta | undefined)

const pageTitle = computed(() => pageHeader.value?.title ?? (route.meta.title as string | undefined))

const items = computed<NavigationMenuItem[]>(() => {
    const onAuth = route.path.startsWith('/settings/authentication')
    const tab = (route.query.tab as string) || 'signin'
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
            defaultOpen: true,
            active: onAuth,
            children: [
                {
                    label: 'Sign in',
                    to: { path: '/settings/authentication', query: { tab: 'signin' } },
                    active: onAuth && tab === 'signin',
                },
                {
                    label: 'Personal Access Tokens',
                    to: { path: '/settings/authentication', query: { tab: 'tokens' } },
                    active: onAuth && tab === 'tokens',
                },
            ],
        },
    ]
})
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
                                 :items="items"
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
                    <span v-if="!collapsed && appVersion"
                          class="px-2.5 text-[10px] text-dimmed">
                        v{{ appVersion }}
                    </span>
                </div>
            </template>
        </UDashboardSidebar>

        <UDashboardPanel
                         :ui="{ body: '!p-0 !gap-0 overflow-hidden [&>*]:flex-1 [&>*]:flex [&>*]:flex-col [&>*]:min-h-0' }">
            <template #header>
                <UDashboardNavbar :title="pageTitle"
                                  :ui="{ root: 'sm:px-4', title: 'text-[15px]' }">
                    <template #leading>
                        <UDashboardSidebarCollapse />
                    </template>
                    <template #right>
                        <div id="navbar-right"
                             class="flex items-center gap-2" />
                    </template>
                </UDashboardNavbar>
            </template>
            <template #body>
                <!-- Full-bleed pages (tabbed views) manage their own frame. -->
                <slot v-if="route.meta.fullBleed" />
                <div v-else
                     class="flex-1 min-h-0 w-full overflow-y-auto">
                    <div class="p-6 w-full">
                        <slot />
                    </div>
                </div>
            </template>
        </UDashboardPanel>
    </UDashboardGroup>
</template>
