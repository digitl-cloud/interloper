<script setup lang="ts">
/**
 * Inline selection card rendered in the agent chat.
 *
 * Triggered by the agent's `request_user_selection` tool call: the wizard's
 * SelectionCard rows, checkbox rows for multi-select and plain selectable
 * rows for single. The confirmed values
 * become the tool call's output, and the agent continues with them; a call
 * already answered (`answer`) renders locked.
 */
import type { SelectionRequest } from '~/types/agent'

const props = defineProps<{
    request: SelectionRequest
    answer?: string[]
}>()

const emit = defineEmits<{
    selected: [values: string[]]
}>()

const picked = ref<string[]>(props.answer ?? [])
const submitted = ref(!!props.answer)

function toggle(value: string) {
    if (submitted.value) return
    if (!props.request.multi) {
        picked.value = [value]
        return
    }
    picked.value = picked.value.includes(value)
        ? picked.value.filter(v => v !== value)
        : [...picked.value, value]
}

const allSelected = computed(() => picked.value.length === props.request.options.length)

function toggleAll() {
    picked.value = allSelected.value ? [] : props.request.options.map(o => o.value)
}

const selectedLabels = computed(() =>
    props.request.options.filter(o => picked.value.includes(o.value)).map(o => o.label))

function confirm() {
    if (!picked.value.length) return
    submitted.value = true
    emit('selected', [...picked.value])
}

const appConfig = useAppConfig()
</script>

<template>
    <AgentCard :title="request.prompt">
        <template v-if="request.multi && request.options.length > 3 && !submitted"
                  #actions>
            <UButton :label="allSelected ? 'Deselect all' : 'Select all'"
                     variant="link"
                     size="sm"
                     @click="toggleAll" />
        </template>

        <!-- Answered: locked summary -->
        <div v-if="submitted"
             class="flex items-center gap-2 text-sm">
            <UIcon :name="appConfig.ui.icons.success"
                   class="size-4 shrink-0 text-success" />
            <span class="truncate text-muted">{{ selectedLabels.join(', ') }}</span>
        </div>

        <div v-else
             class="flex flex-col gap-3">
            <div class="flex max-h-64 flex-col gap-1.5 overflow-y-auto p-0.5">
                <SelectionCard v-for="option in request.options"
                               :key="option.value"
                               :as="request.multi ? 'div' : 'button'"
                               :selected="picked.includes(option.value)"
                               :label="option.label"
                               class="shrink-0"
                               @select="toggle(option.value)">
                    <template v-if="request.multi"
                              #leading>
                        <UCheckbox :model-value="picked.includes(option.value)"
                                   @click.stop
                                   @update:model-value="toggle(option.value)" />
                    </template>
                    <template v-if="!request.multi && picked.includes(option.value)"
                              #trailing>
                        <UIcon :name="appConfig.ui.icons.check"
                               class="size-4 shrink-0 text-primary" />
                    </template>
                </SelectionCard>
            </div>

            <UButton :label="request.multi ? `Confirm selection (${picked.length})` : 'Confirm'"
                     :disabled="!picked.length"
                     block
                     @click="confirm" />
        </div>
    </AgentCard>
</template>
