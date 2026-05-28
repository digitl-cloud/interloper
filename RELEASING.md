# Releasing

Releases are automated by the **Release** workflow
([`.github/workflows/release.yaml`](.github/workflows/release.yaml)), triggered
manually (`workflow_dispatch`, `stable` or `release-candidate`). It runs
`python-semantic-release` to bump the shared version, tag, and create the GitHub
release, then `uv build --all-packages` + `uv publish --trusted-publishing always`
to publish every workspace package to PyPI.

Two things must be configured on the repo before the first release:

- **GitHub App credentials** — variable `SEMANTIC_RELEASE_APP_ID` and secret
  `SEMANTIC_RELEASE_APP_KEY` (a GitHub App with `contents: write`), used to author
  the release commit and tag.
- **PyPI trusted publishing** for each published package — see below.

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
