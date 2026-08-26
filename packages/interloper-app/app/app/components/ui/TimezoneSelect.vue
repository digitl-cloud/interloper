<script setup lang="ts">
/** Searchable select over the browser's IANA timezone list. */
const model = defineModel<string>()

const timezones = Intl.supportedValuesOf('timeZone')
</script>

<template>
    <USelectMenu v-model="model"
                 :items="timezones" />
</template>
