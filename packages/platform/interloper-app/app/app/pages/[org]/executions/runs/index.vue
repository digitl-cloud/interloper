<script setup lang="ts">
const views = useExecutionViews()
</script>

<template>
    <UDashboardPanel id="runs">
        <template #header>
            <AppNavbar title="Executions" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="views" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <ExecutionsRunsTable />
        </template>
    </UDashboardPanel>
</template>
