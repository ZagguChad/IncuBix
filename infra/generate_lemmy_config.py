import os
from pathlib import Path

template = Path("/templates/lemmy-config.template.hjson").read_text(encoding="utf-8")
rendered = template.replace("__LEMMY_DATABASE_URL__", os.environ["LEMMY_DATABASE_URL"])
rendered = rendered.replace("__LEMMY_PICTRS_KEY__", os.environ["LEMMY_PICTRS_KEY"])
Path("/generated/config.hjson").write_text(rendered, encoding="utf-8")
