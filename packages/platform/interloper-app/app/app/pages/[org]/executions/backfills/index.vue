<template>
    <UDashboardPanel id="backfills"
                     class="page-column">
        <template #header>
            <AppNavbar title="Backfills" />
        </template>
        <template #body>
            <ExecutionsBackfillsTable />
        </template>
    </UDashboardPanel>
</template>
