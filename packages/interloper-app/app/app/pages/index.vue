<script setup lang="ts">
// The app's front door holds no page of its own: it opens the organisation the user last worked in.
definePageMeta({ middleware: [() => navigateTo(useOrganisationStore().home(), { replace: true })] })
</script>

<template>
    <div />
</template>
