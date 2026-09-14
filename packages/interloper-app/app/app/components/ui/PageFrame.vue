<script setup lang="ts">
/** The layout's page frame (scroll area plus padding) for a full-bleed page that puts its own chrome above it. */
</script>

<template>
    <div class="flex-1 min-h-0 w-full overflow-y-auto">
        <div class="p-6 w-full">
            <slot />
        </div>
    </div>
</template>
