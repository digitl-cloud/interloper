<script setup lang="ts">
definePageMeta({ layout: 'admin', middleware: 'super-admin' })

const route = useRoute()
const { orgId, org, members, loading, openWorkspace } = provideAdminOrganisation(computed(() => route.params.id as string))

const tabs = computed<PageTab[]>(() => {
    const base = `/admin/organisations/${orgId.value}`
    return [
        { label: 'Usage & quotas', to: `${base}/usage` },
        { label: 'Members', to: `${base}/members`, badge: loading.value ? undefined : members.value.length },
        { label: 'Activity', to: `${base}/activity` },
        { label: 'Settings', to: `${base}/settings` },
    ]
})
</script>

<template>
    <UDashboardPanel id="admin-organisation">
        <template #header>
            <AppNavbar>
                <template #title>
                    <ULink to="/admin/organisations"
                           class="text-base font-medium text-muted hover:text-highlighted">Organisations</ULink>
                    <span class="text-base text-dimmed">/</span>
                    <span class="truncate text-base font-semibold">{{ org?.name ?? '…' }}</span>
                </template>
            </AppNavbar>
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="tabs" />
                </template>
                <template #right>
                    <UButton v-if="org && !org.deleted_at"
                             icon="i-lucide-external-link"
                             label="Open workspace"
                             color="neutral"
                             variant="outline"
                             @click="openWorkspace" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <div class="mx-auto w-full max-w-5xl">
                <NuxtPage />
            </div>
        </template>
    </UDashboardPanel>
</template>
