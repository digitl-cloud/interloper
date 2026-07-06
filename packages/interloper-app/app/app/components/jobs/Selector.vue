<script setup lang="ts">
/**
 * Job selector: pick an existing job or create a new one.
 *
 * Shows existing jobs in a searchable list. Selecting one emits it
 * via `collected`. A "Create new" button opens a nested drawer
 * containing the JobsStepper.
 *
 * Container-agnostic: the parent wraps this in a UDrawer, modal, etc.
 */
import type { ComponentRecord } from '~/types/component'
import { jobBackfillDays, jobCron, jobEnabled, jobPartitioned, jobTags, jobTargetIds } from '~/types/component'
import cronstrue from 'cronstrue'

const props = withDefaults(defineProps<{
    /** Asset keys from the parent wizard, forwarded to the nested JobsStepper. */
    assetKeys?: string[]
}>(), {
    assetKeys: () => [],
})

const emit = defineEmits<{
    collected: [config: { id?: string; name: string; cron: string; tags: string[]; enabled: boolean; partitioned: boolean; backfillDays: number | null }]
}>()

const componentsStore = useComponentsStore()

// ── State ────────────────────────────────────────────────────────
const searchQuery = ref('')
const createDrawerOpen = ref(false)
const createStepperRef = ref<any>(null)

// ── Derived ──────────────────────────────────────────────────────
const jobs = computed(() => componentsStore.byKind('job'))

const filteredJobs = computed(() => {
    if (!searchQuery.value) return jobs.value
    const q = searchQuery.value.toLowerCase()
    return jobs.value.filter(j =>
        (j.name?.toLowerCase().includes(q) ?? false)
        || jobCron(j).includes(q),
    )
})

// ── Data fetching ────────────────────────────────────────────────
onMounted(async () => {
    await componentsStore.fetchAll(['job'])
})

// ── Handlers ─────────────────────────────────────────────────────

function selectExisting(job: ComponentRecord) {
    emit('collected', {
        id: job.id,
        name: job.name ?? '',
        cron: jobCron(job),
        tags: [...jobTags(job)],
        enabled: jobEnabled(job),
        partitioned: jobPartitioned(job),
        backfillDays: jobBackfillDays(job),
    })
}

function handleCreated(config: { name: string; cron: string; tags: string[]; enabled: boolean; partitioned: boolean; backfillDays: number | null }) {
    createDrawerOpen.value = false
    emit('collected', config)
}

function cronLabel(expression: string): string {
    try {
        return cronstrue.toString(expression, { use24HourTimeFormat: true })
    }
    catch {
        return expression
    }
}

// ── Expose ───────────────────────────────────────────────────────
const title = computed(() => 'Add Job')

defineExpose({ title })
</script>

<template>
    <div class="flex flex-col gap-4">
        <div class="flex items-center justify-between">
            <p class="text-sm text-muted">
                Select an existing job or create a new one.
            </p>
            <UButton size="xs"
                     variant="ghost"
                     icon="i-lucide-plus"
                     label="Create new"
                     @click="createDrawerOpen = true" />
        </div>

        <UInput v-if="jobs.length > 5"
                v-model="searchQuery"
                icon="i-lucide-search"
                placeholder="Search jobs..."
                class="w-full" />

        <!-- Empty state -->
        <div v-if="filteredJobs.length === 0"
             class="flex flex-col items-center justify-center rounded-md p-6 gap-2">
            <span class="text-sm text-muted">No jobs found.</span>
            <UButton icon="i-lucide-plus"
                     label="Create one"
                     @click="createDrawerOpen = true" />
        </div>

        <!-- Job list -->
        <div v-else
             class="flex flex-col gap-1">
            <div v-for="item in filteredJobs"
                 :key="item.id"
                 class="flex items-center gap-3 px-3 py-2.5 rounded-md cursor-pointer bg-elevated/50 hover:bg-elevated transition-colors"
                 @click="selectExisting(item)">
                <UIcon name="i-lucide-clock"
                       class="size-5 shrink-0" />
                <div class="flex flex-col min-w-0 flex-1">
                    <span class="text-sm font-medium">{{ item.name }}</span>
                    <span class="text-xs text-muted">{{ cronLabel(jobCron(item)) }}</span>
                </div>
                <div class="flex items-center gap-2">
                    <UBadge v-if="jobPartitioned(item)"
                            color="neutral"
                            variant="subtle"
                            size="xs">
                        <UIcon name="i-lucide-calendar-days"
                               class="size-3" />
                        Partitioned
                    </UBadge>
                    <UBadge v-if="!jobEnabled(item)"
                            color="warning"
                            variant="subtle"
                            size="xs">
                        Disabled
                    </UBadge>
                    <UBadge color="neutral"
                            variant="subtle"
                            size="xs">
                        {{ jobTargetIds(item, 'source').length }} source{{ jobTargetIds(item, 'source').length !== 1 ? 's' : '' }}
                    </UBadge>
                </div>
            </div>
        </div>

        <!-- Nested create drawer (the Selector owns this child dialog) -->
        <UDrawer v-model:open="createDrawerOpen"
                 direction="right"
                 nested
                 :handle="false"
                 :handle-only="true"
                 :title="createStepperRef?.title ?? 'New Job'"
                 :ui="{ content: 'w-[36rem]', description: 'sr-only' }">
            <template #description>
                Configure a new job
            </template>
            <template #body>
                <JobsStepper v-if="createDrawerOpen"
                             ref="createStepperRef"
                             :job="null"
                             :asset-keys="props.assetKeys"
                             mode="collect"
                             @collected="handleCreated" />
            </template>
            <template #footer>
                <StepperNav v-if="createStepperRef"
                            :can-proceed="createStepperRef.canProceed"
                            :has-prev="createStepperRef.hasPrev"
                            :submitting="createStepperRef.submitting"
                            :submit-label="createStepperRef.submitLabel"
                            @next="createStepperRef.next()"
                            @prev="createStepperRef.prev()" />
            </template>
        </UDrawer>
    </div>
</template>
