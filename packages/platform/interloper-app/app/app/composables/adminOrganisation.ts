import type { ComputedRef, InjectionKey, Ref } from 'vue'
import type { AdminOrganisation } from '~/types/admin'
import type { OrgMember } from '~/types/organisation'

/** What the admin organisation page shares with its tabs: the organisation, its members, why they failed to load, and what changes them. */
export interface AdminOrganisationPage {
    orgId: ComputedRef<string>
    org: Ref<AdminOrganisation | null>
    members: Ref<OrgMember[]>
    loading: Ref<boolean>
    error: Ref<unknown>
    isMember: ComputedRef<boolean>
    reload: () => Promise<void>
    join: () => Promise<void>
    openWorkspace: () => Promise<void>
}

const ADMIN_ORGANISATION_PAGE: InjectionKey<AdminOrganisationPage> = Symbol('admin-organisation-page')

/** Load an organisation for the admin page and share it with the tab routes beneath it. */
export function provideAdminOrganisation(orgId: ComputedRef<string>): AdminOrganisationPage {
    const adminStore = useAdminStore()
    const organisationStore = useOrganisationStore()
    const userStore = useUserStore()
    const toast = useToast()

    const org = ref<AdminOrganisation | null>(null)
    const members = ref<OrgMember[]>([])
    const loading = ref(false)
    const error = ref<unknown>(null)
    const isMember = computed(() =>
        members.value.some(member => member.status === 'active' && member.id === userStore.user?.id))

    async function reload() {
        loading.value = true
        error.value = null
        try {
            const [memberRows, organisations] = await Promise.all([
                organisationStore.fetchMemberRows(orgId.value, true),
                adminStore.listOrganisations(),
            ])
            org.value = organisations.find(o => o.id === orgId.value) ?? null
            members.value = memberRows
        }
        catch (err) {
            error.value = err
        }
        finally {
            loading.value = false
        }
    }

    async function join() {
        try {
            await organisationStore.joinOrganisation(orgId.value)
            toast.add({ title: `Joined ${org.value?.name ?? 'organisation'}`, color: 'success' })
            await reload()
        }
        catch (err) {
            toast.add(errorToast(err, "Couldn't join the organisation"))
        }
    }

    async function openWorkspace() {
        if (!org.value) return
        await navigateTo(`/${org.value.key}`)
    }

    watch(orgId, reload, { immediate: true })

    const page: AdminOrganisationPage = { orgId, org, members, loading, error, isMember, reload, join, openWorkspace }
    provide(ADMIN_ORGANISATION_PAGE, page)
    return page
}

/** The admin organisation page's shared state, from one of its tabs. */
export function useAdminOrganisation(): AdminOrganisationPage {
    const page = inject(ADMIN_ORGANISATION_PAGE)
    if (!page) throw new Error('useAdminOrganisation() called outside the admin organisation page')
    return page
}
