<script setup lang="ts">
/**
 * Right drawer for editing an organisation's quota overrides.
 * The quota set comes from the API's field descriptors (key + label +
 * instance default), so a new quota needs no change here. String-typed
 * fields so an emptied input means "inherit the instance default"; each
 * field hints whether it inherits or overrides.
 */
import type { AdminQuotaField, AdminQuotaLimits } from '~/types/admin'

const open = defineModel<boolean>('open', { required: true })

const props = defineProps<{
    orgId: string
    orgName: string
    /** The org's current overrides (nulls where inherited). */
    limits: AdminQuotaLimits | null
    /** The quotas to edit, with the defaults the empty fields fall back to. */
    fields: AdminQuotaField[]
}>()

const emit = defineEmits<{ saved: [] }>()

const adminStore = useAdminStore()
const toast = useToast()

const form = ref<Record<string, string>>({})
const saving = ref(false)

watch(open, (opened) => {
    if (!opened) return
    form.value = Object.fromEntries(
        props.fields.map(field => [field.key, props.limits?.[field.key]?.toString() ?? '']),
    )
})

function defaultLabel(field: AdminQuotaField): string {
    return field.default != null ? field.default.toLocaleString() : 'unlimited'
}

function hint(field: AdminQuotaField): string {
    return form.value[field.key] === ''
        ? `Inheriting ${defaultLabel(field)}`
        : `Overrides the instance default of ${defaultLabel(field)}`
}

async function submit() {
    saving.value = true
    try {
        await adminStore.updateOrgQuota(props.orgId, Object.fromEntries(
            props.fields.map(field => [field.key, form.value[field.key] === '' ? null : Number(form.value[field.key])]),
        ))
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
                <div v-for="field in fields"
                     :key="field.key"
                     class="flex flex-col gap-1.5">
                    <label class="text-[13px] font-semibold">{{ field.label }}</label>
                    <UInput v-model="form[field.key]"
                            type="number"
                            min="0"
                            :placeholder="`Instance default (${defaultLabel(field)})`"
                            class="w-full font-mono"
                            @keydown.enter="submit" />
                    <span class="text-xs text-dimmed">{{ hint(field) }}</span>
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
