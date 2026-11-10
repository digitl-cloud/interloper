<script setup lang="ts">
// Executions is a hub whose views are routes; land on the first one.
definePageMeta({ redirect: to => `/${to.params.org}/executions/runs` })
</script>

<template>
    <div />
</template>
