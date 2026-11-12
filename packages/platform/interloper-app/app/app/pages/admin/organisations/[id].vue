<script setup lang="ts">
import type { BreadcrumbItem } from '@nuxt/ui'

definePageMeta({ layout: 'admin', middleware: 'super-admin' })

const route = useRoute()
const appConfig = useAppConfig()
const { orgId, org, members, loading, error, reload, openWorkspace } = provideAdminOrganisation(computed(() => route.params.id as string))

const tabs = computed<PageTab[]>(() => {
    const base = `/admin/organisations/${orgId.value}`
    return [
        { label: 'Usage & quotas', icon: appConfig.icons.quota, to: `${base}/usage` },
        { label: 'Members', icon: appConfig.icons.members, to: `${base}/members`, badge: loading.value ? undefined : members.value.length },
        { label: 'Activity', icon: appConfig.icons.activity, to: `${base}/activity` },
        { label: 'Settings', icon: appConfig.icons.settings, to: `${base}/settings` },
    ]
})

const crumbs = computed<BreadcrumbItem[]>(() => [
    { label: 'Organisations', to: '/admin/organisations' },
    { label: org.value?.name ?? '…' },
])
</script>

<template>
    <UDashboardPanel id="admin-organisation">
        <template #header>
            <AppNavbar>
                <template #title>
                    <UBreadcrumb :items="crumbs" />
                </template>
            </AppNavbar>
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="tabs" />
                </template>
                <template #right>
                    <UButton v-if="org && !org.deleted_at"
                             :icon="appConfig.icons.organisation"
                             label="Open workspace"
                             color="neutral"
                             variant="outline"
                             @click="openWorkspace" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <LoadError v-if="error"
                       title="Couldn't load the organisation"
                       :error="error"
                       @retry="reload" />
            <NuxtPage />
        </template>
    </UDashboardPanel>
</template>
