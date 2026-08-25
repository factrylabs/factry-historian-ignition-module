# Jython Scripting Reference

Copy-pastable scripts for the Ignition Script Console (**Designer → Tools → Script Console**).

The snippets below use the historian profile `Factry Historian` and the gateway system
name `Ignition-FactryTest`. If your names differ, adjust them in the paths.

Path format used throughout:

```
histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:<TagPath>
```

`<TagPath>` (e.g. `ManualTest/Temperature`) is stored in Factry as the measurement
`default/<TagPath>`.

## Supported functions

Only the `system.historian.*` API is supported by this module:

- `system.historian.storeDataPoints` — store data points
- `system.historian.queryRawPoints` — query raw data
- `system.historian.queryAggregatedPoints` — query with aggregation
- `system.historian.queryMetadata` — query measurement metadata
- `system.historian.storeMetadata` — store measurement metadata
- `system.historian.browse` — browse the historian hierarchy

> `system.tag.storeTagHistory` / `system.tag.queryTagHistory` are **not** supported by
> this module — use the `system.historian.*` equivalents above.

---

## system.historian.storeDataPoints

`storeDataPoints` takes parallel lists (one entry per point). Values can be numeric,
boolean, or string — the measurement is created in Factry on first write.

```python
now = system.date.now()

system.historian.storeDataPoints(
    paths=[
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Numeric",
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Numeric",
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Numeric",
    ],
    values=[10.0, 11.0, 12.0],
    timestamps=[
        system.date.addSeconds(now, -3),
        system.date.addSeconds(now, -2),
        system.date.addSeconds(now, -1),
    ],
    qualities=[192, 192, 192],
)
print "Stored 3 numeric points"
```

## system.historian.queryRawPoints

Returns a `Dataset`: column 0 is `t_stamp`, the remaining columns are one per tag.

```python
end = system.date.now()
start = system.date.addHours(end, -1)

ds = system.historian.queryRawPoints(
    paths=["histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Numeric"],
    startTime=start,
    endTime=end,
)

print "Columns:", [ds.getColumnName(c) for c in range(ds.getColumnCount())]
print "Rows:", ds.getRowCount()
for r in range(ds.getRowCount()):
    print ds.getValueAt(r, 0), ds.getValueAt(r, 1)
```

## String tags (regression check)

String tags have history and can be queried like any other tag. Previously, querying a
string tag threw a coercion error; this verifies the fix.

```python
now = system.date.now()

# Store a few string points
system.historian.storeDataPoints(
    paths=[
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/StringTag",
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/StringTag",
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/StringTag",
    ],
    values=["alpha", "beta", "gamma"],
    timestamps=[
        system.date.addSeconds(now, -3),
        system.date.addSeconds(now, -2),
        system.date.addSeconds(now, -1),
    ],
    qualities=[192, 192, 192],
)

# Query it back — should return the string values
ds = system.historian.queryRawPoints(
    paths=["histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/StringTag"],
    startTime=system.date.addHours(now, -1),
    endTime=system.date.now(),
)
print "String rows:", ds.getRowCount()
for r in range(ds.getRowCount()):
    print ds.getValueAt(r, 0), ds.getValueAt(r, 1)
```

## Mixed string + numeric query (regression check)

Querying a string tag together with a numeric tag must return **both** — a bad tag no
longer aborts the whole query. (Store data into both `ManualTest/Numeric` and
`ManualTest/StringTag` first, using the snippets above.)

```python
end = system.date.now()
start = system.date.addHours(end, -1)

ds = system.historian.queryRawPoints(
    paths=[
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Numeric",
        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/StringTag",
    ],
    startTime=start,
    endTime=end,
)

print "Columns:", [ds.getColumnName(c) for c in range(ds.getColumnCount())]
print "Rows:", ds.getRowCount()
for r in range(ds.getRowCount()):
    print [ds.getValueAt(r, c) for c in range(ds.getColumnCount())]
```

## system.historian.queryAggregatedPoints

```python
end = system.date.now()
start = system.date.addHours(end, -1)

ds = system.historian.queryAggregatedPoints(
    paths=["histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Numeric"],
    startTime=start,
    endTime=end,
    aggregates=["Average", "Minimum", "Maximum"],
    returnSize=1,
)

print "Columns:", [ds.getColumnName(c) for c in range(ds.getColumnCount())]
for r in range(ds.getRowCount()):
    print [ds.getValueAt(r, c) for c in range(ds.getColumnCount())]
```

`returnSize` controls the number of time windows (buckets); pass `1` for a single
aggregate over the whole range, or e.g. `10` for ten evenly-spaced windows. Omitting
`aggregates` and `returnSize` lets the historian apply its defaults.

Supported aggregation modes:

| Mode | Description |
|------|-------------|
| `Average` | Time-weighted mean of values |
| `SimpleAverage` | Arithmetic mean of values |
| `Minimum` | Smallest value |
| `Maximum` | Largest value |
| `Sum` | Sum of all values |
| `Count` | Number of data points |
| `LastValue` | Most recent value |
| `Range` | Max minus Min |
| `MinMax` | Returns both the min and the max |

> `Variance` and `StdDev` are advertised but currently return `NaN` from the Factry
> backend — avoid relying on them.

## system.historian.storeMetadata

Store metadata such as the engineering unit. Metadata is cached by the module and applied
when the measurement is **created** in Factry, so store it *before* (or together with) the
first data point for a new tag. `description` maps to the Factry measurement description;
other keys (`engUnit`, `engLow`, `engHigh`, …) are stored as measurement metadata.

```python
system.historian.storeMetadata(
    paths=["histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Temperature"],
    timestamps=[system.date.now()],
    properties={
        "engUnit": "degC",
        "engLow": "0",
        "engHigh": "100",
        "description": "Temperature sensor",
    },
)

# Create the measurement so the cached metadata is applied
system.historian.storeDataPoints(
    paths=["histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Temperature"],
    values=[42.0],
    timestamps=[system.date.now()],
    qualities=[192],
)
print "Metadata stored and measurement created"
```

> **Read-back limitation:** the values above are written into Factry (visible in the
> Factry measurement's description/metadata), but Factry's collector API does not return
> stored custom metadata, so `queryMetadata` below reports only `datatype`, `name`, and
> `status` — not `engUnit`. The write side is correct; the read side is a Factry-side gap.

## system.historian.queryMetadata

Returns a `Results` of `MetadataPoint`; each point exposes `source()` (the path) and
`value()` (a `PropertySet` of properties).

```python
result = system.historian.queryMetadata(
    paths=["histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest/Temperature"]
)

for mp in result.getResults():
    print "Path:", mp.source()
    for pv in mp.value():
        print "  ", pv.getProperty().getName(), "=", pv.getValue()
```

## system.historian.browse

```python
# Browse the historian root
results = system.historian.browse("histprov:Factry Historian:/")
for r in results.getResults():
    print r.getPath(), "hasChildren=", r.hasChildren()

# Browse into a folder
results = system.historian.browse(
    "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:ManualTest"
)
for r in results.getResults():
    print r.getPath()
```
