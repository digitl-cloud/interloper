<template>
    <UDashboardPanel id="backfills">
        <template #header>
            <AppNavbar title="Executions" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="EXECUTION_VIEWS" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <ExecutionsBackfillsTable />
        </template>
    </UDashboardPanel>
</template>
