# Configuration

## Config class

`il.Config` is a [resource](resources.md) for arbitrary settings. It extends
[Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/), providing
environment-variable resolution, type validation, and defaults. Define one by subclassing
`il.Config` or decorating a plain class with `@il.config`:

```py
import interloper as il

class MyConfig(il.Config):
    api_key: str
    base_url: str = "https://api.example.com"
    timeout: int = 30
```

```py
@il.config
class MyConfig:
    api_key: str
    base_url: str = "https://api.example.com"
    timeout: int = 30
```

!!! tip "Config vs. Connection"

    Use `Config` for plain settings (base URLs, limits, feature flags). Use a
    [Connection](connections.md) for service **credentials** — it adds secret handling and an
    OAuth connect flow. Both are resources and are injected the same way.

## Environment variable resolution

Config fields are resolved from environment variables automatically:

```sh
export API_KEY="my-secret-key"
export BASE_URL="https://custom.api.com"
```

```py
config = MyConfig()        # Resolves from the environment
print(config.api_key)      # "my-secret-key"
print(config.base_url)     # "https://custom.api.com"
```

## Injecting config into assets

A config is injected into an asset function by **type annotation** — the framework inspects the
signature and resolves the resource at run time:

```py
@il.asset
def my_asset(config: MyConfig):
    return fetch_data(config.api_key, base_url=config.base_url)
```

This works for source assets too:

```py
@il.source
def my_source():
    @il.asset
    def data(config: MyConfig):
        return fetch(config.base_url)

    return [data]
```

## Providing config explicitly

When auto-resolution from the environment isn't what you want, supply an instance via the
`resources` mapping (keyed by the parameter name):

```py
asset_instance = my_asset(resources={"config": MyConfig(api_key="explicit-key")})

# Or shared across a source's assets:
source = my_source(resources={"config": MyConfig(base_url="https://staging.api.com")})
```

You can also declare resources up front on the decorator with `resources={...}` and let the
type annotation bind them at call time. See [Resources](resources.md) for the full resolution
cascade.
