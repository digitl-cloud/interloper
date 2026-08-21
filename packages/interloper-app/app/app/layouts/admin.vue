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

/** Navbar title; pages with a bespoke navbar teleport into it instead. */
const pageTitle = computed(() => pageHeader.value?.title ?? (route.meta.title as string | undefined))
/** Pages that fill the navbar themselves (crumb + name) via #navbar-title. */
const customNavbar = computed(() => !!route.meta.customNavbar)

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
                <UDashboardNavbar :title="customNavbar ? undefined : pageTitle"
                                  :ui="{ root: 'sm:px-4', title: 'text-[15px]' }">
                    <template #leading>
                        <UDashboardSidebarCollapse />
                    </template>
                    <template v-if="customNavbar"
                              #title>
                        <div id="navbar-title"
                             class="flex min-w-0 items-center gap-2.5" />
                    </template>
                    <template #right>
                        <div id="navbar-right"
                             class="flex items-center gap-2" />
                    </template>
                </UDashboardNavbar>
            </template>
            <template #body>
                <!-- Full-bleed pages (tabbed detail views) manage their own frame. -->
                <slot v-if="route.meta.fullBleed" />
                <div v-else
                     class="flex-1 min-h-0 w-full overflow-y-auto">
                    <div class="p-6 w-full">
                        <div v-if="pageHeader"
                             class="mb-6 max-w-[660px]">
                            <p v-if="pageHeader.eyebrow"
                               class="eyebrow text-primary">{{ pageHeader.eyebrow }}</p>
                            <h1 class="mt-2.5 text-[28px] font-bold leading-tight tracking-[-0.022em]">{{ pageHeader.title }}</h1>
                            <p v-if="pageHeader.description"
                               class="mt-2.5 text-[15px] leading-relaxed text-muted">{{ pageHeader.description }}</p>
                        </div>
                        <slot />
                    </div>
                </div>
            </template>
        </UDashboardPanel>
    </UDashboardGroup>
</template>
