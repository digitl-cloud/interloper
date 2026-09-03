# Documentation screenshots

Regenerates the images under `docs/assets/ui/` from a throwaway database, so the Web UI pages of
the documentation can follow UI changes.

```sh
# 1. Throwaway database, seeded, plus the demo content and a session token
export INTERLOPER_POSTGRES_HOST=localhost INTERLOPER_POSTGRES_PORT=5432 \
       INTERLOPER_POSTGRES_USER=postgres INTERLOPER_POSTGRES_PASSWORD=postgres \
       INTERLOPER_POSTGRES_DATABASE=interloper_docs INTERLOPER_LAUNCHER_TYPE=in_process \
       INTERLOPER_RUNNER_TYPE=async INTERLOPER_ENCRYPTION_KEY=docs-throwaway-key
cd dev
uv run interloper db reset --yes
uv run python seed.py

# 2. The app on a non-3000 port; dummy OAuth app credentials make the sign-in tabs render
INTERLOPER_SERVER_PORT=3100 INTERLOPER_AUTH_COOKIE_SECURE=false \
INTERLOPER_FACEBOOK_CLIENT_ID=docs INTERLOPER_FACEBOOK_CLIENT_SECRET=docs \
INTERLOPER_FACEBOOK_REDIRECT_URI=http://localhost:3100/x \
uv run interloper app --api --cron --worker --reaper --dev &

# 3. Demo content (waits for the worker to run everything, then backdates the runs)
uv run python docs_screenshots/populate.py

# 4. Screenshots, light and dark, 1440x900 at 2x
cd docs_screenshots && npm i playwright-core
node shoot.mjs http://localhost:3100 ./session_token ./out

# 5. Resize to 1.5x and copy into the docs
uv run python -c "
from PIL import Image; import pathlib
for p in pathlib.Path('out').glob('*.png'):
    Image.open(p).convert('RGB').resize((2160, 1350), Image.LANCZOS).save(f'../../docs/assets/ui/{p.name}', optimize=True)"
```

`shoot.mjs` drives the system Chrome headlessly through `playwright-core`; each entry in its
`shots` list is a page and, where needed, the clicks that open a wizard step. Pass a
comma-separated list of shot names as the fourth argument to recapture a subset. Drop the
database afterwards: `psql -c "drop database interloper_docs with (force)"`.
