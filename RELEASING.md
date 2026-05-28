# Releasing

Releases are automated by the **Release** workflow
([`.github/workflows/release.yaml`](.github/workflows/release.yaml)), triggered
manually (`workflow_dispatch`, `stable` or `release-candidate`). The `release`
job runs `python-semantic-release` to bump the shared version, tag, and create
the GitHub release. When a release is cut, three sets of artifacts are then
published in parallel:

1. **PyPI** — `uv build --all-packages` + `uv publish --trusted-publishing always`
   publishes every workspace package (in the `release` job).
2. **Docker images → GHCR** — the `docker` job builds each component image and
   pushes it to `ghcr.io/<owner>/interloper-<role>` (see below).
3. **Helm chart → GitHub Pages** — the `helm` job packages `chart/interloper`
   and publishes it to the gh-pages Helm repo via `chart-releaser` (stable
   releases only; see below).

`semantic-release` bumps the chart's `version` and `appVersion` in
[`chart/interloper/Chart.yaml`](chart/interloper/Chart.yaml) alongside the
Python version, so the published chart and the image tag it deploys always
match the release.

Things to configure on the repo before the first release:

- **GitHub App credentials** — variable `SEMANTIC_RELEASE_APP_ID` and secret
  `SEMANTIC_RELEASE_APP_KEY` (a GitHub App with `contents: write`), used to author
  the release commit and tag.
- **PyPI trusted publishing** for each published package — see below.
- **GitHub Pages** enabled with the `gh-pages` branch as source — see the Helm
  section below.

The Docker push uses the built-in `GITHUB_TOKEN` (`packages: write`) — no extra
secret needed.

## PyPI trusted publishing (one-time bootstrap)

The workflow publishes via [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC), so no long-lived API tokens live in CI. Each published package needs a
trusted publisher on PyPI pointing at this repository and the Release workflow.

### The monorepo limitation

PyPI's account-level **pending publisher** page
(`https://pypi.org/manage/account/publishing/`) does not allow registering
multiple pending publishers that share the same **owner + repository + workflow**.
Because every package here is released from the same repo and the same
`release.yaml`, the packages cannot all be pre-registered as pending publishers
from that global page.

### Workaround: token-publish once, then add the publisher per project

A trusted publisher *can* be added on an existing project's own settings page even
when it shares the repo + workflow with other projects. So bootstrap each package
once with a token to create the project, then attach the trusted publisher there.

For each package that does not yet exist on PyPI:

1. Create a PyPI **API token** (account-scoped — the project doesn't exist yet):
   PyPI → *Account settings* → *API tokens*.

2. Build and publish that single package with the token (this creates the project):

   ```bash
   uv build --package <package-name> -o dist/
   uv publish --token <pypi-token> dist/<dist-prefix>-*
   ```

   Distribution filenames normalize hyphens to underscores, e.g.
   `interloper-google-cloud` → `dist/interloper_google_cloud-*`,
   `interloper-k8s` → `dist/interloper_k8s-*`. Use `-o dist/` (and keep `dist/`
   clean) so the glob uploads only the intended package.

3. On the now-existing project, add the trusted publisher:
   PyPI → *your project* → *Manage* → *Publishing* → *Add a new publisher*
   (GitHub Actions):

   - **Owner:** `digitl-cloud`
   - **Repository:** `interloper`
   - **Workflow name:** `release.yaml`
   - **Environment:** *(leave blank — the workflow uses none)*

4. (Recommended) Revoke the temporary API token once trusted publishing is verified.

Once every package has a trusted publisher, the Release workflow's
`uv publish --trusted-publishing always` succeeds for all of them and no tokens are
needed again.

### Published packages

All workspace packages are published:

`interloper-core`, `interloper-assets`, `interloper-pandas`, `interloper-db`,
`interloper-google-cloud`, `interloper-docker`, `interloper-k8s`,
`interloper-scheduler`, `interloper-api`, `interloper-agent`, `interloper-app`.

## Docker images (GitHub Container Registry)

The `docker` job builds one image per component and pushes it to GHCR. The
matrix mirrors the image catalog in the [`Makefile`](Makefile) — keep the two in
sync if a role is added or removed.

| Image | Dockerfile target | Notes |
| --- | --- | --- |
| `ghcr.io/<owner>/interloper-api` | `api` | |
| `ghcr.io/<owner>/interloper-frontend` | `frontend` | nginx serving the built SPA |
| `ghcr.io/<owner>/interloper-worker` | `worker` | |
| `ghcr.io/<owner>/interloper-scheduler` | `scheduler` | no launcher extras |
| `ghcr.io/<owner>/interloper-scheduler-k8s` | `scheduler` | `SCHEDULER_EXTRAS=k8s` |
| `ghcr.io/<owner>/interloper-scheduler-docker` | `scheduler` | `SCHEDULER_EXTRAS=docker` |

Each image is tagged with the released version. On **stable** releases the
`latest` tag is also moved; release candidates push the version tag only.

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

## Helm chart (GitHub Pages)

The `helm` job runs [`chart-releaser`](https://github.com/helm/chart-releaser-action)
on **stable** releases. It packages `chart/interloper`, creates a per-chart
GitHub release (tagged `interloper-<version>`), and updates `index.yaml` on the
`gh-pages` branch. Release candidates are skipped to keep the public repo clean.

One-time setup:

1. Let the first stable release run — `chart-releaser` creates the `gh-pages`
   branch and the initial `index.yaml`.
2. Repo → *Settings* → *Pages* → set **Source** to *Deploy from a branch*,
   branch `gh-pages`, folder `/ (root)`.

Consumers then add the repo and install:

```bash
helm repo add interloper https://<owner>.github.io/interloper
helm repo update
helm install interloper interloper/interloper
```

> The chart's `home`/`sources` URLs in `Chart.yaml` currently read
> `github.com/digitlcloud/interloper` (no hyphen) while the repo is
> `digitl-cloud/interloper`. Fix those so the generated Pages URL
> (`https://digitl-cloud.github.io/interloper`) and chart metadata line up.
