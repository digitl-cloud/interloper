<script setup lang="ts">
definePageMeta({ title: 'Executions', fullBleed: true })
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <NavExecutionsHub>
            <ExecutionsBackfillsTable />
        </NavExecutionsHub>
    </div>
</template>
