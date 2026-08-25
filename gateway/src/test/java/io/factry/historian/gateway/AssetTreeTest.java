package io.factry.historian.gateway;

import io.factry.historian.proto.Asset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AssetTreeTest {

    private static Asset asset(String uuid, String name, String parentUUID) {
        return Asset.newBuilder()
                .setUuid(uuid)
                .setName(name)
                .setParentUUID(parentUUID)
                .build();
    }

    @Nested
    class BuildChildrenByParent {

        @Test
        void emptyList() {
            Map<String, List<Asset>> result = FactryQueryEngine.buildChildrenByParent(Collections.emptyList());
            assertTrue(result.isEmpty());
        }

        @Test
        void rootAssets_groupedUnderEmptyString() {
            Asset motor = asset("u1", "Motor", "");
            Asset pump = asset("u2", "Pump", "");

            Map<String, List<Asset>> result = FactryQueryEngine.buildChildrenByParent(List.of(motor, pump));

            assertEquals(1, result.size());
            assertEquals(2, result.get("").size());
            assertTrue(result.get("").contains(motor));
            assertTrue(result.get("").contains(pump));
        }

        @Test
        void childAssets_groupedUnderParentUUID() {
            Asset motor = asset("u1", "Motor", "");
            Asset another = asset("u2", "another", "u1");

            Map<String, List<Asset>> result = FactryQueryEngine.buildChildrenByParent(List.of(motor, another));

            assertEquals(List.of(motor), result.get(""));
            assertEquals(List.of(another), result.get("u1"));
        }
    }

    @Nested
    class ComputeAssetPaths {

        @Test
        void singleRootAsset() {
            Asset motor = asset("u1", "Motor", "");
            Map<String, List<Asset>> children = FactryQueryEngine.buildChildrenByParent(List.of(motor));

            Map<String, Asset> paths = new HashMap<>();
            FactryQueryEngine.computeAssetPaths(children, "", "", paths);

            assertEquals(1, paths.size());
            assertSame(motor, paths.get("Motor"));
        }

        @Test
        void nestedAsset_pathJoinedWithSlash() {
            Asset motor = asset("u1", "Motor", "");
            Asset another = asset("u2", "another", "u1");
            Map<String, List<Asset>> children = FactryQueryEngine.buildChildrenByParent(List.of(motor, another));

            Map<String, Asset> paths = new HashMap<>();
            FactryQueryEngine.computeAssetPaths(children, "", "", paths);

            assertEquals(2, paths.size());
            assertSame(motor, paths.get("Motor"));
            assertSame(another, paths.get("Motor/another"));
        }

        @Test
        void deeplyNestedAssets() {
            Asset root = asset("u1", "Plant", "");
            Asset area = asset("u2", "Area1", "u1");
            Asset line = asset("u3", "Line1", "u2");
            Asset station = asset("u4", "Station1", "u3");
            Map<String, List<Asset>> children = FactryQueryEngine.buildChildrenByParent(
                    List.of(root, area, line, station));

            Map<String, Asset> paths = new HashMap<>();
            FactryQueryEngine.computeAssetPaths(children, "", "", paths);

            assertEquals(4, paths.size());
            assertSame(root, paths.get("Plant"));
            assertSame(area, paths.get("Plant/Area1"));
            assertSame(line, paths.get("Plant/Area1/Line1"));
            assertSame(station, paths.get("Plant/Area1/Line1/Station1"));
        }

        @Test
        void multipleRootsWithChildren() {
            Asset motor = asset("u1", "Motor", "");
            Asset pump = asset("u2", "Pump", "");
            Asset motorChild = asset("u3", "another", "u1");
            Asset pumpChild = asset("u4", "valve", "u2");
            Map<String, List<Asset>> children = FactryQueryEngine.buildChildrenByParent(
                    List.of(motor, pump, motorChild, pumpChild));

            Map<String, Asset> paths = new HashMap<>();
            FactryQueryEngine.computeAssetPaths(children, "", "", paths);

            assertEquals(4, paths.size());
            assertSame(motor, paths.get("Motor"));
            assertSame(pump, paths.get("Pump"));
            assertSame(motorChild, paths.get("Motor/another"));
            assertSame(pumpChild, paths.get("Pump/valve"));
        }

        @Test
        void siblingAssets_underSameParent() {
            Asset motor = asset("u1", "Motor", "");
            Asset child1 = asset("u2", "rpm", "u1");
            Asset child2 = asset("u3", "temp", "u1");
            Map<String, List<Asset>> children = FactryQueryEngine.buildChildrenByParent(
                    List.of(motor, child1, child2));

            Map<String, Asset> paths = new HashMap<>();
            FactryQueryEngine.computeAssetPaths(children, "", "", paths);

            assertEquals(3, paths.size());
            assertSame(child1, paths.get("Motor/rpm"));
            assertSame(child2, paths.get("Motor/temp"));
        }

        @Test
        void noAssets_emptyResult() {
            Map<String, Asset> paths = new HashMap<>();
            FactryQueryEngine.computeAssetPaths(Collections.emptyMap(), "", "", paths);
            assertTrue(paths.isEmpty());
        }
    }
}
