<script setup lang="ts">
// Organization is a hub whose views are routes; land on the first one.
definePageMeta({ redirect: to => `/${to.params.org}/organization/members` })
</script>

<template>
    <div />
</template>
