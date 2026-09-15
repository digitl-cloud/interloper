<script setup lang="ts">
/**
 * The layout's page frame (scroll area plus padding) for a full-bleed page
 * that puts its own chrome above it. The inner column is exactly the
 * panel's height, so a list table can take the remaining room and pin its
 * footer to the bottom; taller flowing content overflows it and scrolls
 * the frame as before.
 */
</script>

<template>
    <div class="flex-1 min-h-0 w-full overflow-y-auto">
        <div class="p-6 w-full h-full flex flex-col">
            <slot />
        </div>
    </div>
</template>
