<script setup lang="ts">
/**
 * The card a tool call that waits for the user renders in the chat (a
 * selection, a connection setup): a UCard at the conversation's card width.
 * With a `title`, its header leads with the optional `icon` and carries the
 * `description` under the title and the `actions` slot on its right; the
 * default slot is the body.
 */
defineProps<{
    icon?: string
    title?: string
    description?: string
}>()

defineSlots<{
    default?: () => unknown
    actions?: () => unknown
}>()
</script>

<template>
    <UCard class="mx-auto my-2 w-full min-w-80 max-w-md">
        <template v-if="title"
                  #header>
            <div class="flex items-center gap-3">
                <UIcon v-if="icon"
                       :name="icon"
                       class="size-5 shrink-0" />
                <CardHeader :title="title"
                            :description="description"
                            class="min-w-0 flex-1">
                    <template v-if="$slots.actions"
                              #default>
                        <slot name="actions" />
                    </template>
                </CardHeader>
            </div>
        </template>

        <template v-if="$slots.default"
                  #default>
            <slot />
        </template>
    </UCard>
</template>
