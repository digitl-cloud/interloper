"""interloper-app: the Nuxt SPA, packaged as static assets, and the all-in-one ``interloper app`` command."""

from interloper_app.assets import SpaFallback, source_dir, static_dir

__all__ = ["SpaFallback", "source_dir", "static_dir"]
