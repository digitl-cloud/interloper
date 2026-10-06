<template>
    <UDashboardPanel id="runs">
        <template #header>
            <AppNavbar title="Executions" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="EXECUTION_VIEWS" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <ExecutionsRunsTable />
        </template>
    </UDashboardPanel>
</template>
