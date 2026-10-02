import type { MaybeRefOrGetter } from 'vue'

/**
 * Whether a loaded resource belongs to an organisation other than the active one.
 *
 * Detail reads are authorized by membership, so a link can load a resource
 * from a non-selected org. `OrganizationGate` swaps its content for a
 * switch-org interstitial on `mismatch`; pages use the same flag to hide the
 * actions and status they render outside the gate.
 *
 * @param orgId Org that owns the resource; empty while it loads (never a mismatch).
 */
export function useOrgGate(orgId: MaybeRefOrGetter<string | null | undefined>) {
    const orgStore = useOrganisationStore()

    const mismatch = computed(() => {
        const id = toValue(orgId)
        return !!id && !!orgStore.organisation && id !== orgStore.organisation.id
    })

    return { mismatch }
}
