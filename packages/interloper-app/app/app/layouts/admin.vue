<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()
const appVersion = useRuntimeConfig().public.version

/** Design page header rendered by the layout, declared via definePageMeta({ pageHeader }). */
interface PageHeaderMeta {
    title: string
    description?: string
}
const pageHeader = computed(() => route.meta.pageHeader as PageHeaderMeta | undefined)

/** Eyebrow-styled page title; breadcrumb pages set titleInBreadcrumb and render it as their first crumb. */
const pageTitle = computed(() => route.meta.titleInBreadcrumb
    ? undefined
    : pageHeader.value?.title ?? (route.meta.title as string | undefined))

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
                <UDashboardSidebarCollapse :class="collapsed ? 'mx-auto' : 'ms-auto'" />
            </template>

            <template #default="{ collapsed }">
                <UBadge v-if="!collapsed"
                        color="primary"
                        size="lg"
                        class="eyebrow w-full justify-center py-2"
                        label="Admin portal" />
                <UNavigationMenu :collapsed="collapsed"
                                 :items="items"
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
            <template #body>
                <div class="flex-1 min-h-0 w-full overflow-y-auto">
                    <div class="p-4 w-full max-w-[1040px] mx-auto">
                        <div v-if="pageTitle"
                             class="mb-4">
                            <PageBreadcrumb :items="[titleCrumb(pageTitle)]" />
                            <p v-if="pageHeader?.description"
                               class="text-[15px] text-muted leading-relaxed max-w-[660px] mt-2.5">
                                {{ pageHeader.description }}
                            </p>
                        </div>
                        <slot />
                    </div>
                </div>
            </template>
        </UDashboardPanel>
    </UDashboardGroup>
</template>
