# Releasing

A release takes two workflows, one responsibility each:

- **Release** ([`.github/workflows/release.yaml`](.github/workflows/release.yaml)),
  dispatched by hand (`stable` or `release-candidate`), cuts the version:
  `python-semantic-release` (run with `uvx`) bumps the shared version, commits,
  tags and creates the GitHub release. It runs no checks of its own: it
  releases only a commit the Checks workflow passed. Dispatched while Checks is
  still running for that commit, as right after a merge, it waits for it (up to
  15 minutes) and fails if Checks fails. Checks skips a commit that touches only
  the paths it ignores (docs, Markdown) and a `[skip ci]` one such as a release
  commit; to release one of those, run Checks on main first
  (`gh workflow run checks.yaml --ref main`). Release publishes nothing;
  re-running it after a release finds the tag and does nothing.
- **Publish** ([`.github/workflows/publish.yaml`](.github/workflows/publish.yaml)),
  triggered by that GitHub release, publishes the tag everywhere:
  1. **Build**: the `build` job builds the SPA and the extension UIs, every
     workspace package (`uv build --all-packages`) and the `@interloper/ui`
     tarball (`pnpm pack`) once, into one artifact. A build that fails
     publishes nothing.
  2. **PyPI**: the `pypi` job publishes the artifact's wheels and sdists with
     trusted publishing (see below).
  3. **npm**: the `npm` job publishes `@interloper/ui`, the SDK extension UIs
     build with, with trusted publishing and provenance (see below).
  4. **Docker images → GHCR**: the `docker` job builds each role's images once
     per architecture and pushes them by digest to
     `ghcr.io/<owner>/interloper-<role>`; the `docker-merge` job then tags each
     image's two digests as one multi-arch image (see below).
  5. **Helm chart → GHCR**: the `helm` job packages `chart/interloper` and
     pushes it as an OCI artifact beside the images (stable releases only; see
     below).
  6. **Downstream**: once every image is tagged, the `downstream` job triggers
     the pipeline the `DOWNSTREAM_TRIGGER_URL` and `DOWNSTREAM_TRIGGER_TOKEN`
     secrets name, a GitLab pipeline trigger, with the version as
     `PLATFORM_VERSION` (stable releases only). A deployment that builds its
     own images on top of these sets them; without them the job does nothing.

  PyPI and npm wait on the build, the image merges on the image builds, and no
  publish job waits on another, so one registry's outage holds up only its own
  job.

A release candidate gets a prerelease version (`<version>-rc.<n>`): PyPI
publishes it as a prerelease, npm under the `rc` dist-tag so it never becomes
`latest`, the images without moving `latest`, and the chart not at all.

`semantic-release` bumps the chart's `version` and `appVersion` in
[`chart/interloper/Chart.yaml`](chart/interloper/Chart.yaml) and the `version`
of [`packages/platform/interloper-app/sdk/package.json`](packages/platform/interloper-app/sdk/package.json)
alongside the Python version, so the published chart, the image tag it deploys
and the SDK always match the release.

## Recovering a failed publish

Every publish job makes the version present and skips what already is: the
`pypi` job skips the files PyPI lists, the `npm` job a version npm has, and
the image and chart pushes overwrite the same tags. So a failed job is
recovered with **Re-run failed jobs** on the Publish run: the jobs that
succeeded stay done, and a re-run `pypi` or `npm` job publishes the same build
artifact.

GitHub re-runs a workflow run for 30 days. Past that, or if the release event
never started the Publish workflow, dispatch it with the tag; it rebuilds the
tag and publishes whatever is still missing:

```bash
gh workflow run publish.yaml -f tag=v<version>
```

A rebuild of interloper-app is not byte-identical to the first (Nuxt stamps
each SPA build with a fresh id), which is why the `pypi` job skips a file by
its name: a file name PyPI lists can never take other content.

Never cut another version to retry a publish.

## Before the first release

Things to configure on the repo:

- **GitHub App credentials**: secrets `SEMANTIC_RELEASE_APP_ID` and
  `SEMANTIC_RELEASE_APP_KEY` (a GitHub App with `contents: write`), used to
  author the release commit and tag and to create the GitHub release, whose
  event starts the Publish workflow (an event created with the built-in token
  starts none). These may be set at the repo or org level; if set on the org,
  make sure the `interloper` repository is granted access.
- **PyPI trusted publishing** for each published package: see below.
- **npm trusted publishing** for `@interloper/ui`: see below.
- **Package visibility**: the first chart push creates the `charts/interloper`
  package as private. Make it public (repo → *Packages* → the package →
  *Package settings*) so unauthenticated clients can pull it.

The Docker and Helm pushes use the built-in `GITHUB_TOKEN` (`packages: write`),
so they need no extra secret.

## PyPI trusted publishing (one-time bootstrap)

The `pypi` job publishes via [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC), so no long-lived API tokens live in CI. Each published package needs a
trusted publisher on PyPI pointing at this repository and the Publish workflow.
Every workspace package (`packages/*/*`) is published.

### The monorepo limitation

PyPI's account-level **pending publisher** page
(`https://pypi.org/manage/account/publishing/`) does not allow registering
multiple pending publishers that share the same **owner + repository + workflow**.
Because every package here is published from the same repo and the same
`publish.yaml`, the packages cannot all be pre-registered as pending publishers
from that global page.

### Workaround: token-publish once, then add the publisher per project

A trusted publisher *can* be added on an existing project's own settings page even
when it shares the repo + workflow with other projects. So bootstrap each new
package once with a token to create the project, then attach the trusted
publisher there.

The first release that carries a new package fails its `pypi` job on that
package. Then:

1. Create a PyPI **API token** (account-scoped, since the project doesn't exist
   yet): PyPI → *Account settings* → *API tokens*.

2. Publish that package's files from the failed run's artifact with the token
   (this creates the project):

   ```bash
   gh run download <run-id> --name dist --dir dist
   uv publish --token <pypi-token> dist/pypi/<dist-prefix>-*
   ```

   Distribution filenames normalize hyphens to underscores, e.g.
   `interloper-google-cloud` → `dist/pypi/interloper_google_cloud-*`.

3. On the now-existing project, add the trusted publisher:
   PyPI → *your project* → *Manage* → *Publishing* → *Add a new publisher*
   (GitHub Actions):

   - **Owner:** `digitl-cloud`
   - **Repository:** `interloper`
   - **Workflow name:** `publish.yaml`
   - **Environment:** *(leave blank, the workflow uses none)*

4. **Re-run failed jobs** on the Publish run: the `pypi` job skips what PyPI
   has and publishes the rest.

5. (Recommended) Revoke the temporary API token.

## npm trusted publishing (one-time bootstrap)

The `npm` job publishes `@interloper/ui` with
[npm trusted publishing](https://docs.npmjs.com/trusted-publishers) (OIDC) and
provenance, so no npm token lives in CI. The package belongs to the npm
organisation `interloper`. npm adds a trusted publisher on the package's own
settings page, so a package must exist before CI can publish it. A new
package's first version is the one by-hand publish:

1. The first release that carries the package fails its `npm` job: the package
   does not exist yet.

2. Publish the tarball from that run's artifact, signed in to npm with an
   account that can publish to the `interloper` organisation:

   ```bash
   gh run download <run-id> --name dist --dir dist
   npm publish dist/npm/interloper-ui-<version>.tgz --access public
   ```

   Publish what `pnpm pack` wrote, never the package directory itself: the
   packed `package.json` takes its `exports` from `publishConfig`, which points
   at the compiled preset in `dist/`.

3. In the package's settings on npmjs.com, add a trusted publisher for GitHub
   Actions: repository `digitl-cloud/interloper`, workflow `publish.yaml`, no
   environment (the workflow uses none).

Every later version goes out from the `npm` job.

`make check-sdk-package` checks the package as npm installs it, from a project
outside the workspace; CI runs the same check.

## Docker images (GitHub Container Registry)

The `docker` and `docker-merge` jobs publish one image per role to GHCR. Each
role ships two variants: the **loaded** image on the bare tag, carrying the
role's packages plus every component class a catalog can name (the vendor SDKs
only on the roles that execute assets), and the extras-free **slim** one on a
`-slim` tag. The image list (one YAML anchor, `&images`, that both jobs'
matrices use) mirrors the image catalog in the [`Makefile`](Makefile) (`ROLES` +
`SLIM_ROLES`), keep the two in sync if a role or variant is added or removed.
No build args are passed: the dockerfile's `ARG` defaults define what loaded
means, and the slim stages declare no extras `ARG` at all.

Every image is `linux/amd64` and `linux/arm64`, each built natively, with no
emulation: the `docker` job builds an image per architecture, amd64 on
`ubuntu-latest` and arm64 on GitHub's ARM runner (`ubuntu-24.04-arm`), and
pushes it by digest, untagged. Each architecture keeps its own build cache
(`docker-<target>-<arch>`), whose export never fails the build. The
`docker-merge` job then creates, per image, the multi-arch index from its two
digests under the release tags (`docker buildx imagetools create`). A merge
waits on every build but runs whenever its own two digests exist, so a failed
build holds back only its own image, and **Re-run failed jobs** rebuilds it and
merges again. To check a published image:

```bash
docker buildx imagetools inspect ghcr.io/digitl-cloud/interloper-api:<version>
```

| Image:tag | Dockerfile target | Carries |
| --- | --- | --- |
| `ghcr.io/<owner>/interloper-api:<version>` | `api` | assets, destinations, Slack, agent, otel |
| `ghcr.io/<owner>/interloper-api:<version>-slim` | `api-slim` | core + db + api |
| `ghcr.io/<owner>/interloper-frontend:<version>` | `frontend` | nginx serving the built SPA |
| `ghcr.io/<owner>/interloper-core:<version>` | `core` | assets + vendor SDKs, destinations, otel |
| `ghcr.io/<owner>/interloper-core:<version>-slim` | `core-slim` | the framework alone |
| `ghcr.io/<owner>/interloper-scheduler:<version>` | `scheduler` | assets + vendor SDKs, destinations, both launchers, otel |
| `ghcr.io/<owner>/interloper-scheduler:<version>-slim` | `scheduler-slim` | core + db + scheduler |
| `ghcr.io/<owner>/interloper-mcp:<version>` | `mcp` | assets, destinations, otel |
| `ghcr.io/<owner>/interloper-mcp:<version>-slim` | `mcp-slim` | core + db + toolkit + mcp |

Each image is tagged with the released version (plus the `-slim` variant). On
**stable** releases the `latest` / `latest-slim` tags are also moved;
release candidates push the version tags only.

One-time setup: the first push creates each package as **private** and not yet
linked to the repository. After the first release, for each package go to the
package page → *Package settings* and (a) set the visibility you want (public if
the chart/consumers pull without auth) and (b) under *Manage Actions access*,
confirm the `interloper` repo has write access. Public images need no pull
secret; private images require an `imagePullSecrets` entry in the Helm values.

> **Registry consistency (action needed):** `chart/interloper/values.yaml` and
> the `Makefile` still default `image.registry` to the GCP Artifact Registry
> (`europe-docker.pkg.dev/dc-int-connectors-prd/docker`). Now that releases push
> to GHCR, decide whether GHCR is the canonical registry — if so, update both
> defaults to `ghcr.io/<owner>` so deployed charts pull the images that were
> actually published.

## Helm chart (GHCR)

The `helm` job packages `chart/interloper` on **stable** releases and pushes it
to `oci://ghcr.io/<owner>/charts`, so the chart is a registry artifact beside
the images it deploys rather than an index file on a web host. Release
candidates are skipped to keep the registry clean.

Consumers install straight from the registry (Helm 3.8+), with no `helm repo add`:

```bash
helm install interloper oci://ghcr.io/digitl-cloud/charts/interloper --version <version>
```

Chart versions released before 0.79.1 predate the registry. The index that
served them is gone, so `helm repo` no longer resolves them; their `.tgz`
assets stay attached to the matching no-`v` GitHub releases and a direct URL
still fetches them.

> The chart's `home`/`sources` URLs in `Chart.yaml` currently read
> `github.com/digitlcloud/interloper` (no hyphen) while the repo is
> `digitl-cloud/interloper`, so both links 404. Fix them to point at the repo.
