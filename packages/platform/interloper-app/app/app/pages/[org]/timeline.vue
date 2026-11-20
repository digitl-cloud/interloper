<script setup lang="ts">
// The timeline is a view of the Runs page now; old links land there with their query.
definePageMeta({ redirect: to => ({ path: `/${to.params.org}/executions/runs`, query: { ...to.query, view: 'timeline' } }) })
</script>

<template>
    <div />
</template>
