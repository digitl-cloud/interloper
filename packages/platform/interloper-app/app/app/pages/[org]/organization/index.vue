<script setup lang="ts">
// The organisation's pages are Members and Settings; its bare address lands on the first.
definePageMeta({ redirect: to => `/${to.params.org}/organization/members` })
</script>

<template>
    <div />
</template>
