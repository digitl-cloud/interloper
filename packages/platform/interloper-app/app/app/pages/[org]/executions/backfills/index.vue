<script setup lang="ts">
const views = useExecutionViews()
</script>

<template>
    <UDashboardPanel id="backfills">
        <template #header>
            <AppNavbar title="Executions" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="views" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <ExecutionsBackfillsTable />
        </template>
    </UDashboardPanel>
</template>
