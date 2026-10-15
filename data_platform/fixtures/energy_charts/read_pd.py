import json
import pandas as pd

with open("public_power_de_2025-10-20_2026-03-30.json") as f:
    payload = json.load(f)

df = pd.DataFrame(
    {t["name"]: t["data"] for t in payload["production_types"]},
    index=pd.to_datetime(payload["unix_seconds"], unit="s", utc=True),
)
df.index.name = "ts_utc"
print(df)
