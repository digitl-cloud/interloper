/**
 * Refetch an org-scoped store whenever the active organisation changes.
 *
 * Entity stores (sources, assets, destinations, resources, jobs) fetch their
 * data on page mount. Because switching organisation doesn't remount the page,
 * they would otherwise keep showing the previous org's data until a full
 * reload. Execution stores (runs, backfills, events, …) already get this for
 * free through `useRealtimeSubscription`'s `scope` option; this is the
 * equivalent for the manually-fetched entity stores.
 *
 * Call it once inside a store's setup. The watcher is non-immediate, so it
 * never races with the page's own initial fetch — it only reacts to a switch.
 *
 * @param refetch Reloads the store's data for the active org.
 * @param reset   Optional: clears state before refetching (and on sign-out,
 *                when the org becomes null).
 */
export function useOrgScopedRefetch(
    refetch: () => void | Promise<void>,
    reset?: () => void,
) {
    const organisationStore = useOrganisationStore()

    watch(
        () => organisationStore.organisation?.id,
        (orgId) => {
            reset?.()
            if (orgId) refetch()
        },
    )
}
