<template>
    <UDashboardPanel id="backfills">
        <template #header>
            <AppNavbar title="Executions" />
            <UDashboardToolbar>
                <template #left>
                    <UNavigationMenu :items="EXECUTION_VIEWS"
                                     highlight
                                     class="-mx-1 flex-1" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <ExecutionsBackfillsTable />
        </template>
    </UDashboardPanel>
</template>
