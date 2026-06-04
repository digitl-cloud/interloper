import type { Job, JobInput } from '~/types/job'

export const useJobsStore = defineStore('jobs', () => {
    const { apiFetch } = useApi()

    /**********************
     * State
     **********************/
    const jobs = ref<Job[]>([])
    const loading = ref(false)
    const error = ref<Error | null>(null)

    /**********************
     * Internals
     **********************/
    function _upsert(job: Job) {
        const idx = jobs.value.findIndex(j => j.id === job.id)
        if (idx >= 0) jobs.value[idx] = { ...jobs.value[idx], ...job }
        else jobs.value.push(job)
    }

    function _remove(id: string) {
        jobs.value = jobs.value.filter(j => j.id !== id)
    }

    /**********************
     * Actions
     **********************/
    async function fetch() {
        loading.value = true
        error.value = null
        try {
            jobs.value = await apiFetch<Job[]>('/jobs')
        }
        catch (e) {
            error.value = e as Error
        }
        finally {
            loading.value = false
        }
    }

    async function create(input: JobInput): Promise<Job> {
        const job = await apiFetch<Job>('/jobs', {
            method: 'POST',
            body: input,
        })
        _upsert(job)
        return job
    }

    async function update(id: string, input: JobInput): Promise<Job> {
        const job = await apiFetch<Job>(`/jobs/${id}`, {
            method: 'PUT',
            body: input,
        })
        _upsert(job)
        return job
    }

    async function remove(ids: string | string[]) {
        const list = Array.isArray(ids) ? ids : [ids]
        await Promise.all(list.map(id => apiFetch(`/jobs/${id}`, { method: 'DELETE' })))
        list.forEach(_remove)
    }

    async function queueRun(jobId: string, partitionDate?: string): Promise<string> {
        const res = await apiFetch<{ run_id: string }>(`/jobs/${jobId}/run`, {
            method: 'POST',
            body: partitionDate ? { partition_date: partitionDate } : undefined,
        })
        return res.run_id
    }

    async function queueBackfill(
        jobId: string,
        startDate: string,
        endDate: string,
        options?: { concurrency?: number; failFast?: boolean },
    ): Promise<string> {
        const res = await apiFetch<{ backfill_id: string }>(`/jobs/${jobId}/backfill`, {
            method: 'POST',
            body: {
                start_date: startDate,
                end_date: endDate,
                concurrency: options?.concurrency ?? 1,
                fail_fast: options?.failFast ?? false,
            },
        })
        return res.backfill_id
    }

    /**********************
     * Lookups
     **********************/
    function findById(id: string): Job | undefined {
        return jobs.value.find(j => j.id === id)
    }

    function search(query: string): Job[] {
        if (!query) return jobs.value
        const q = query.toLowerCase()
        return jobs.value.filter(j => j.name.toLowerCase().includes(q))
    }

    function $reset() {
        jobs.value = []
        loading.value = false
        error.value = null
    }

    useOrgScopedRefetch(() => fetch(), $reset)

    return {
        jobs,
        loading,
        error,
        fetch,
        create,
        update,
        remove,
        queueRun,
        queueBackfill,
        findById,
        search,
        _upsert,
        _remove,
        $reset,
    }
})
