
# TODO

  ## Tag-path handling — port checks from the TimescaleDB sibling module

  Context: the sibling TimescaleDB historian module (../../mustry/timescaledb-historian-module)
  fixed several tag-path bugs. Most don't apply here because the stored keys differ:
  Factry stores `provider/tagPath` (2 segments, NO gateway); the sibling stores
  `gateway/provider/tagPath` (3 segments). But these two are worth checking in THIS repo:

  [] Check `if (tag.startsWith(prov + "/")) return tag;` in TagPathUtil.queryPathToStoredPath
       This is the same "provider == first folder" heuristic the sibling removed. It can
       misfire for a TYPED query whose tag's first folder literally equals the provider
       name (sibling repro: `[default]default/sub1/tagname1`): it returns the tag as-is
       instead of `prov/tag` → lookup miss → 0 rows. Normal tags are unaffected
       (e.g. `default/FactrySim/ii1`, first folder `FactrySim` != provider `default`);
       only ones where folder == provider hit it.
       To verify: add a unit test in TagPathUtilTest — query path
       `sys:X:/prov:default:/tag:default/sub/x` should map to `default/default/sub/x`.
       If it returns `default/sub/x`, it's the same bug → remove the heuristic.

  [] Check driver-schema (`drv:`) handling in TagPathUtil
       The sibling added `extractSysProv()` to also parse the framework's normalized
       `drv:gateway:provider` form, not just `sys:`/`prov:`. Here we only read `prov:`,
       so if the framework ever hands us a `drv:` path, `prov` comes back null and the
       measurement is mis-routed. Capture a real path in storagePathToStoredPath /
       queryPathToStoredPath to see whether `drv:`-schema paths occur; if so, port the
       driver-schema parsing.

  Not applicable here (recorded so we don't re-investigate):
   - Remote-historian source-gateway problem (sibling has a gateway segment to mismatch;
     we don't). Plan here: use two separate collectors, one per source gateway, so the
     originating gateway is distinguished at the collector level, not in the stored key.
   - getOriginalPath() switch — we only need `prov`+`tag`; low value.
   - Annotations — not implemented here.

  [] So metadata flows one direction only: 
         Factry → Ignition create tag with metadata in Ignition 
         Other direction: we can't change the metadata, but we can send what we have

  [] Setup automation (setup-factry.sh)
  

  [] Support legacy system.tag.* historian methods (currently PyList error in S&F bridge)
    - system.tag.storeTagHistory
    - system.tag.queryTagHistory
    - system.tag.queryTagCalculations

  some bugs:
    - check the default values if they work with the new release 8.3.6

  [] TSDB created via API
      "Influx" TSDB UUID: ff6392ac-4872-11f1-bd09-e69c80a3afdc (host: influx:8086, db: factry)
      Created under Root organization. May need to be under "Mustry" org instead.

## Wannnes's list

Condensed below, more detail and minor items are in the attached document.

6. Editing an array tag in ignition only sends the changes indexes to historian, for example changing [1,2,3,4] to [1,2,4,5] gets sent as the value [4,5]
''' 
question asked form factry
The Ignition SDK hands only the changed elements over to the module's storage engine, e.g. changing [1,2,3,4] → [1,2,4,5] arrives as just {2:4, 3:5}.

We can't reliably reconstruct the full array on our side: the changed elements come through Store & Forward, so by the time we process them the live tag value may have moved on. Storing the last values in the module would make it stateful, and its initialization could be error-prone.

implemented suggestion: query from factry

query array doesn't work, email sent to Wannes


'''
 
  


+ 1. When configuring a new historian in ignition and leaving "Use TLS" unchecked, after submitting it gets enabled anyway
 ( I agree with your reasoning: since Ignition's create form doesn't pre-fill @DefaultValue, the checkbox renders unchecked, but on submit the old default (true) was applied — so "looks off, saved on." Setting the default to false (FactryHistorianConfig.java:25) makes what you see match what you get.)
+ 2. TLS only connects with "Skip TLS Verification" on, which leaves it unauthenticated. Confirmed against a deployed historian using a normal Let's Encrypt cert: the module trusts only its bundled certificate and rejects everything else, so verification always fails. It should also trust the system/public CAs alongside ours.



+ 3. Writes to new tag during historian outage(or not reachable) are silently dropped, it tries to create the new measurement a couple of times but ends up dropping the point

+ 4. Not sure how far implementation ever got for this but I couldn't get metadata or engineering specs to work when creating new tags (as it only is supported on creation)

+ 5. Version is on 1.0.7 at the moment, would be good to make it will be 1.0.0 when we do the first release
+ 7. Querying the raw history of a string tag throws an error and aborts the whole query, so a query mixing string and numeric tags fails entirely instead of just skipping the string one.
+ 8. The Batch size and Batch interval configurations do not seem to be used, so unless I'm wrong could be better to remove those




  
  
   
   
  



