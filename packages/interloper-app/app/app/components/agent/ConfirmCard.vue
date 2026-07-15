<script setup lang="ts">
/**
 * Inline confirmation summary card rendered in the agent chat.
 *
 * Triggered by the agent's `request_confirmation` tool: the wizard's
 * WizardRecap summary, compacted for the agent panel, with Confirm /
 * Cancel actions. The parent reports the decision back into the chat as
 * the user's message so the agent proceeds (or stops) with it.
 */
import type { ConfirmationRequest } from '~/types/agent'

const props = defineProps<{
    request: ConfirmationRequest
}>()

const emit = defineEmits<{
    decided: [confirmed: boolean]
}>()

const decision = ref<boolean | null>(null)

/** Recap rows want icons; the agent only sends labels — map the usual suspects. */
const ROW_ICONS: [RegExp, string][] = [
    [/source|type/i, 'i-lucide-plug'],
    [/name/i, 'i-lucide-tag'],
    [/account|profile|config|dataset/i, 'i-lucide-settings-2'],
    [/asset/i, 'i-lucide-layers'],
    [/connection/i, 'i-lucide-key-round'],
    [/destination/i, 'i-lucide-hard-drive'],
]

const rows = computed(() => props.request.items.map(item => ({
    icon: ROW_ICONS.find(([pattern]) => pattern.test(item.label))?.[1] ?? 'i-lucide-dot',
    label: item.label,
    value: item.value,
})))

function decide(confirmed: boolean) {
    decision.value = confirmed
    emit('decided', confirmed)
}
</script>

<template>
    <div class="border border-default rounded-[13px] p-4 my-2 w-full min-w-80 max-w-md flex flex-col gap-3">
        <span class="text-[13px] font-semibold text-highlighted">{{ request.title }}</span>

        <WizardRecap :rows="rows" />

        <!-- Decided: locked outcome -->
        <div v-if="decision !== null"
             class="flex items-center gap-2 text-[13px]">
            <UIcon :name="decision ? 'i-lucide-check-circle-2' : 'i-lucide-circle-x'"
                   class="size-4 shrink-0"
                   :class="decision ? 'text-success' : 'text-muted'" />
            <span class="text-muted">{{ decision ? 'Confirmed' : 'Cancelled' }}</span>
        </div>

        <div v-else
             class="flex gap-2">
            <UButton label="Cancel"
                     color="neutral"
                     variant="outline"
                     class="flex-1 justify-center"
                     @click="decide(false)" />
            <UButton label="Confirm"
                     class="flex-1 justify-center"
                     @click="decide(true)" />
        </div>
    </div>
</template>
