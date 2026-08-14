<script setup lang="ts">
/**
 * Right drawer for editing an organisation's quota overrides.
 * String-typed fields so an emptied input means "inherit the instance
 * default"; each field hints whether it inherits or overrides.
 */
import type { AdminQuotaLimits } from '~/types/admin'

const open = defineModel<boolean>('open', { required: true })

const props = defineProps<{
    orgId: string
    orgName: string
    /** The org's current overrides (nulls where inherited). */
    limits: AdminQuotaLimits | null
    /** The instance defaults the empty fields fall back to. */
    defaults: AdminQuotaLimits | null
}>()

const emit = defineEmits<{ saved: [] }>()

const adminStore = useAdminStore()
const toast = useToast()

const LIMIT_FIELDS = [
    { key: 'max_sources', label: 'Max sources' },
    { key: 'max_assets_per_source', label: 'Max assets per source' },
    { key: 'max_successful_runs_per_month', label: 'Max successful runs / month' },
    { key: 'max_backfill_days', label: 'Max backfill days' },
] as const

type LimitKey = typeof LIMIT_FIELDS[number]['key']

const form = ref<Record<LimitKey, string>>({
    max_sources: '',
    max_assets_per_source: '',
    max_successful_runs_per_month: '',
    max_backfill_days: '',
})
const saving = ref(false)

watch(open, (opened) => {
    if (!opened) return
    form.value = {
        max_sources: props.limits?.max_sources?.toString() ?? '',
        max_assets_per_source: props.limits?.max_assets_per_source?.toString() ?? '',
        max_successful_runs_per_month: props.limits?.max_successful_runs_per_month?.toString() ?? '',
        max_backfill_days: props.limits?.max_backfill_days?.toString() ?? '',
    }
})

function defaultLabel(key: LimitKey): string {
    const value = props.defaults?.[key]
    return value != null ? value.toLocaleString() : 'unlimited'
}

function hint(key: LimitKey): string {
    return form.value[key] === ''
        ? `Inheriting ${defaultLabel(key)}`
        : `Overrides the instance default of ${defaultLabel(key)}`
}

async function submit() {
    saving.value = true
    try {
        await adminStore.updateOrgQuota(props.orgId, {
            max_sources: form.value.max_sources === '' ? null : Number(form.value.max_sources),
            max_assets_per_source: form.value.max_assets_per_source === ''
                ? null
                : Number(form.value.max_assets_per_source),
            max_successful_runs_per_month: form.value.max_successful_runs_per_month === ''
                ? null
                : Number(form.value.max_successful_runs_per_month),
            max_backfill_days: form.value.max_backfill_days === '' ? null : Number(form.value.max_backfill_days),
        })
        toast.add({ title: `Quota limits updated for ${props.orgName}`, color: 'success' })
        open.value = false
        emit('saved')
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to update quota limits'))
    }
    finally {
        saving.value = false
    }
}
</script>

<template>
    <UDrawer v-model:open="open"
             direction="right"
             :handle="false"
             :handle-only="true"
             title="Edit quotas"
             :description="orgName"
             :ui="{ content: 'w-[420px] max-w-[92vw]' }">
        <template #body>
            <UButton icon="i-lucide-x"
                     color="neutral"
                     variant="soft"
                     size="md"
                     class="absolute top-[22px] right-[26px] rounded-[9px] text-muted"
                     aria-label="Close"
                     @click="open = false" />
            <p class="text-[13px] text-muted leading-normal mb-4">
                Leave a field empty to inherit the instance default.
            </p>
            <div class="flex flex-col gap-4">
                <div v-for="field in LIMIT_FIELDS"
                     :key="field.key"
                     class="flex flex-col gap-1.5">
                    <label class="text-[13px] font-semibold">{{ field.label }}</label>
                    <UInput v-model="form[field.key]"
                            type="number"
                            min="0"
                            :placeholder="`Instance default (${defaultLabel(field.key)})`"
                            class="w-full font-mono"
                            @keydown.enter="submit" />
                    <span class="text-xs text-dimmed">{{ hint(field.key) }}</span>
                </div>
            </div>
        </template>
        <template #footer>
            <div class="flex justify-end gap-2 w-full">
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="open = false" />
                <UButton label="Save"
                         :disabled="saving"
                         :loading="saving"
                         @click="submit" />
            </div>
        </template>
    </UDrawer>
</template>
