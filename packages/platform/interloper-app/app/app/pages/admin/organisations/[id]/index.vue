<script setup lang="ts">
// The organisation's tabs are routes; land on the first one.
definePageMeta({ redirect: to => `/admin/organisations/${to.params.id}/usage` })
</script>

<template>
    <div />
</template>
