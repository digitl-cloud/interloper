<script setup lang="ts">
/**
 * Says once, at the sidebar's foot, that `interloper dev` serves the
 * collection from files: read-only here, changed in those files.
 */
defineProps<{
    collapsed?: boolean
}>()

const componentsStore = useComponentsStore()
const organisationStore = useOrganisationStore()
const appConfig = useAppConfig()

watchEffect(() => {
    if (organisationStore.organisation) void componentsStore.fetchServedFrom()
})
</script>

<template>
    <UTooltip v-if="componentsStore.servedFrom"
              :text="`Served from ${componentsStore.servedFrom}`">
        <div class="flex items-center gap-2 px-2 py-1.5 text-xs text-muted"
             :class="collapsed && 'justify-center'">
            <UIcon :name="appConfig.icons.served"
                   class="size-3.5 shrink-0" />
            <span v-if="!collapsed"
                  class="truncate">Read-only collection</span>
        </div>
    </UTooltip>
</template>
