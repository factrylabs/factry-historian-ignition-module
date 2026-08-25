package io.factry.historian.gateway;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TagPathUtilTest {

    // --- extractComponent ---

    @Test
    void extractComponent_sys() {
        assertEquals("Ignition-abc",
                TagPathUtil.extractComponent(
                        "histprov:test:/sys:Ignition-abc:/prov:default:/tag:Temp", "sys:"));
    }

    @Test
    void extractComponent_prov_notMatchingHistprov() {
        assertEquals("default",
                TagPathUtil.extractComponent(
                        "histprov:timescale historian:/sys:Ignition-abc:/prov:default:/tag:Temp", "prov:"));
    }

    @Test
    void extractComponent_tag() {
        assertEquals("FactrySim/ii2",
                TagPathUtil.extractComponent(
                        "histprov:test:/sys:Ignition-abc:/prov:default:/tag:FactrySim/ii2", "tag:"));
    }

    @Test
    void extractComponent_tagAtEnd() {
        assertEquals("Temperature",
                TagPathUtil.extractComponent(
                        "sys:Ignition-abc:/prov:default:/tag:Temperature", "tag:"));
    }

    @Test
    void extractComponent_notFound() {
        assertNull(TagPathUtil.extractComponent("sys:Ignition-abc:/tag:Temp", "prov:"));
    }

    @Test
    void extractComponent_atStartOfString() {
        assertEquals("Ignition-abc",
                TagPathUtil.extractComponent("sys:Ignition-abc:/prov:default", "sys:"));
    }

    // --- buildStoredPath ---

    @Test
    void buildStoredPath_standard() {
        assertEquals("default/Temperature",
                TagPathUtil.buildStoredPath("default", "Temperature"));
    }

    @Test
    void buildStoredPath_nullProv_defaultsToDefault() {
        assertEquals("default/Temperature",
                TagPathUtil.buildStoredPath(null, "Temperature"));
    }

    @Test
    void buildStoredPath_nestedTag() {
        assertEquals("default/FactrySim/ff1",
                TagPathUtil.buildStoredPath("default", "FactrySim/ff1"));
    }

    // --- storagePathToStoredPath ---

    @Test
    void storagePath_simple() {
        assertEquals("default/Temperature",
                TagPathUtil.storagePathToStoredPath(
                        "prov:default:/tag:Temperature"));
    }

    @Test
    void storagePath_nestedTag() {
        assertEquals("default/Simulation/Pressure",
                TagPathUtil.storagePathToStoredPath(
                        "prov:default:/tag:Simulation/Pressure"));
    }

    @Test
    void storagePath_withSysComponent() {
        // Storage pipeline may include sys: — it's ignored, only prov: and tag: matter
        assertEquals("default/Temperature",
                TagPathUtil.storagePathToStoredPath(
                        "histprov:timescale historian:/sys:Ignition-296a8ca4b6cd:/prov:default:/tag:Temperature"));
    }

    @Test
    void storagePath_noProv() {
        assertEquals("default/Temperature",
                TagPathUtil.storagePathToStoredPath(
                        "tag:Temperature"));
    }

    @Test
    void storagePath_folderNameMatchesProvider() {
        // The tag's first folder has the same name as the provider.
        // Must NOT be mistaken for a composite path — components are trusted as-is.
        assertEquals("PRF/PRF/SubFolder/UDT/Value",
                TagPathUtil.storagePathToStoredPath(
                        "prov:PRF:/tag:PRF/SubFolder/UDT/Value"));
    }

    // --- queryPathToStoredPath ---

    @Test
    void queryPath_fullPath() {
        assertEquals("default/Temperature",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:timescale historian:/sys:Ignition-296a8ca4b6cd:/prov:default:/tag:Temperature"));
    }

    @Test
    void queryPath_nestedTag() {
        assertEquals("default/Simulation/Pressure",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:test:/sys:GW-01:/prov:default:/tag:Simulation/Pressure"));
    }

    @Test
    void queryPath_withoutHistprov() {
        assertEquals("myProvider/Temp",
                TagPathUtil.queryPathToStoredPath(
                        "sys:Ignition-abc:/prov:myProvider:/tag:Temp"));
    }

    @Test
    void queryPath_noProv_tagOnly_returnsTagAsIs() {
        assertEquals("default/Temperature",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:test:/tag:default/Temperature"));
    }

    @Test
    void queryPath_bindingPath_provAlreadyInTag() {
        // Tag history binding: [Factry Historian]default/FactrySim/ff1
        // Ignition sends prov:default AND tag:default/FactrySim/ff1 — don't double the prefix
        assertEquals("default/FactrySim/ff1",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:Factry Historian:/prov:default:/tag:default/FactrySim/ff1"));
    }

    @Test
    void queryPath_sysProvTag_tagAlreadyHasProvPrefix() {
        // Tag history binding from historian browse: [Factry Historian]default/Manual Test/ii1
        assertEquals("default/Manual Test/ii1",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:Factry Historian:/sys:Ignition-FactryTest:/prov:default:/tag:default/Manual Test/ii1"));
    }

    @Test
    void queryPath_withFolders() {
        assertEquals("default/Simulation/Pressure",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:test:/folder:default:/folder:Simulation:/tag:Pressure"));
    }

    @Test
    void queryPath_withFolders_measurementCategory() {
        assertEquals("default/Temperature",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:test:/folder:Measurements:/folder:default:/tag:Temperature"));
    }

    @Test
    void queryPath_withFolders_assetCategory() {
        assertEquals("Plant/Line1/Motor1",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:test:/folder:Assets:/folder:Plant:/folder:Line1:/tag:Motor1"));
    }

    @Test
    void queryPath_measurementCategory() {
        assertEquals("default/Temperature",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:test:/tag:Measurements/default/Temperature"));
    }

    @Test
    void queryPath_assetCategory() {
        assertEquals("Plant/Line1/Motor1",
                TagPathUtil.queryPathToStoredPath(
                        "histprov:test:/tag:Assets/Plant/Line1/Motor1"));
    }

    // --- parseFolderPrefix ---

    @Test
    void parseFolderPrefix_null_returnsEmpty() {
        assertEquals("", TagPathUtil.parseFolderPrefix(null));
    }

    @Test
    void parseFolderPrefix_empty_returnsEmpty() {
        assertEquals("", TagPathUtil.parseFolderPrefix(""));
    }

    @Test
    void parseFolderPrefix_noFolderComponent_returnsEmpty() {
        assertEquals("", TagPathUtil.parseFolderPrefix("histprov:Timescale historian"));
    }

    @Test
    void parseFolderPrefix_singleFolder() {
        assertEquals("default/",
                TagPathUtil.parseFolderPrefix(
                        "histprov:Timescale historian:/folder:default"));
    }

    @Test
    void parseFolderPrefix_twoFolders() {
        assertEquals("default/Simulation/",
                TagPathUtil.parseFolderPrefix(
                        "histprov:Timescale historian:/folder:default:/folder:Simulation"));
    }

    @Test
    void parseFolderPrefix_threeFolders() {
        assertEquals("default/Simulation/Sub/",
                TagPathUtil.parseFolderPrefix(
                        "histprov:Timescale historian:/folder:default:/folder:Simulation:/folder:Sub"));
    }

    // --- extractCategory ---

    @Test
    void extractCategory_measurements() {
        assertEquals("Measurements", TagPathUtil.extractCategory("Measurements/default/Temp"));
    }

    @Test
    void extractCategory_assets() {
        assertEquals("Assets", TagPathUtil.extractCategory("Assets/Plant/Line1/Motor1"));
    }

    @Test
    void extractCategory_noCategory() {
        assertNull(TagPathUtil.extractCategory("default/Temp"));
    }

    @Test
    void extractCategory_null() {
        assertNull(TagPathUtil.extractCategory(null));
    }

    // --- stripCategory ---

    @Test
    void stripCategory_measurements() {
        assertEquals("default/Temp",
                TagPathUtil.stripCategory("Measurements/default/Temp"));
    }

    @Test
    void stripCategory_noCategory() {
        assertEquals("default/Temp",
                TagPathUtil.stripCategory("default/Temp"));
    }

    // --- roundtrip: storage → browse query → stored ---

    @Test
    void roundtrip_storageAndBrowseBack() {
        String stored = TagPathUtil.storagePathToStoredPath(
                "prov:default:/tag:Simulation/Pressure");
        assertEquals("default/Simulation/Pressure", stored);

        String browseQuery = "histprov:test:/tag:" + stored;
        String roundtripped = TagPathUtil.queryPathToStoredPath(browseQuery);
        assertEquals(stored, roundtripped);
    }

    @Test
    void roundtrip_storageAndFolderBrowseBack() {
        String stored = TagPathUtil.storagePathToStoredPath(
                "prov:default:/tag:Simulation/Pressure");
        assertEquals("default/Simulation/Pressure", stored);

        String browsePath = "histprov:test:/folder:Measurements:/folder:default:/folder:Simulation:/tag:Pressure";
        String result = TagPathUtil.queryPathToStoredPath(browsePath);
        assertEquals(stored, result);
    }

    // --- toBrowseName: stored measurement name -> browse name ---

    private static final String FS = " ∕ "; // escaped '/' sentinel (U+2215)

    @Test
    void toBrowseName_slashDelimiter_isNoOp() {
        // Default '/' : Ignition splits the path into a tree as-is.
        assertEquals("default/FactrySim/ff1",
                TagPathUtil.toBrowseName("default/FactrySim/ff1", "/"));
    }

    @Test
    void toBrowseName_emptyDelimiter_escapesAllSlashes() {
        // Flat: every '/' escaped so the whole name stays one leaf.
        assertEquals("default" + FS + "FactrySim" + FS + "ff1",
                TagPathUtil.toBrowseName("default/FactrySim/ff1", ""));
    }

    @Test
    void toBrowseName_nullDelimiter_treatedAsFlat() {
        assertEquals("a" + FS + "b",
                TagPathUtil.toBrowseName("a/b", null));
    }

    @Test
    void toBrowseName_otherDelimiter_escapesSlashThenPromotesDelimiter() {
        // Names that use '.' as separator become a tree; any real '/' is escaped.
        assertEquals("Plant/Area/Tag",
                TagPathUtil.toBrowseName("Plant.Area.Tag", "."));
    }

    @Test
    void toBrowseName_otherDelimiter_realSlashIsPreserved() {
        // 'a/b.c' with '.' delimiter: '/' escaped, '.' promoted -> tree split only on the dot.
        assertEquals("a" + FS + "b/c",
                TagPathUtil.toBrowseName("a/b.c", "."));
    }

    // --- fromBrowseName: browse name -> stored measurement name (inverse) ---

    @Test
    void fromBrowseName_slashDelimiter_isNoOp() {
        assertEquals("default/FactrySim/ff1",
                TagPathUtil.fromBrowseName("default/FactrySim/ff1", "/"));
    }

    @Test
    void fromBrowseName_emptyDelimiter_restoresSlashes() {
        assertEquals("default/FactrySim/ff1",
                TagPathUtil.fromBrowseName("default" + FS + "FactrySim" + FS + "ff1", ""));
    }

    @Test
    void fromBrowseName_otherDelimiter_restoresDelimiterThenSlash() {
        assertEquals("Plant.Area.Tag",
                TagPathUtil.fromBrowseName("Plant/Area/Tag", "."));
    }

    // --- round trip: toBrowseName then fromBrowseName == identity ---

    @Test
    void roundtrip_browseName_slash() {
        String name = "default/FactrySim/ff1";
        assertEquals(name, TagPathUtil.fromBrowseName(TagPathUtil.toBrowseName(name, "/"), "/"));
    }

    @Test
    void roundtrip_browseName_empty() {
        String name = "default/FactrySim/ff1";
        assertEquals(name, TagPathUtil.fromBrowseName(TagPathUtil.toBrowseName(name, ""), ""));
    }

    @Test
    void roundtrip_browseName_otherDelimiterWithRealSlash() {
        String name = "a/b.c.d";
        assertEquals(name, TagPathUtil.fromBrowseName(TagPathUtil.toBrowseName(name, "."), "."));
    }

    // --- isAssetQueryPath ---

    @Test
    void isAssetQueryPath_assetFolders_true() {
        assertTrue(TagPathUtil.isAssetQueryPath(
                "histprov:test:/folder:Assets:/folder:Plant:/folder:Line1:/tag:Motor1"));
    }

    @Test
    void isAssetQueryPath_assetCategoryInTag_true() {
        assertTrue(TagPathUtil.isAssetQueryPath(
                "histprov:test:/tag:Assets/Plant/Line1/Motor1"));
    }

    @Test
    void isAssetQueryPath_measurementFolders_false() {
        assertFalse(TagPathUtil.isAssetQueryPath(
                "histprov:test:/folder:Measurements:/folder:default:/tag:Temperature"));
    }

    @Test
    void isAssetQueryPath_plainMeasurement_false() {
        assertFalse(TagPathUtil.isAssetQueryPath(
                "histprov:test:/tag:default/FactrySim/ff1"));
    }
}
