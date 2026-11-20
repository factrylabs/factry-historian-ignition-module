package io.factry.historian.gateway;

import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class FactryRecord implements HistoricalNode {
    private final UUID nodeId;
    private final String measurementUUID;
    private final int element;
    private final boolean array;
    private final QualifiedPath source;
    private final DataType dataType;
    private final Instant createdTime;

    public FactryRecord(String measurementUUID, QualifiedPath source, String factryDataType, Instant createdTime) {
        this(measurementUUID, -1, source, factryDataType, null, createdTime);
    }

    /**
     * @param element          for one element of an array measurement ({@code Arr[2]}), its index;
     *                         -1 for a node that reads the measurement's own values. Ignition
     *                         historizes each element as its own path, PointWriter stores them all
     *                         in the measurement {@code Arr}.
     * @param factryDataType   the measurement's data type, or for an element the element's type
     *                         ("number" for a "[]number" measurement)
     * @param ignitionDataType the tag's Ignition data type as recorded on the measurement, or null.
     *                         Only DateTime is used: Factry keeps dates as numbers.
     */
    public FactryRecord(String measurementUUID, int element, QualifiedPath source, String factryDataType,
                        String ignitionDataType, Instant createdTime) {
        // Elements share a measurement, so the index is part of the node id to keep it unique.
        String id = element >= 0 ? measurementUUID + "[" + element + "]" : measurementUUID;
        this.nodeId = UUID.nameUUIDFromBytes(id.getBytes());
        this.measurementUUID = measurementUUID;
        this.element = element;
        this.array = element < 0 && factryDataType != null && factryDataType.startsWith("[]");
        this.source = source;
        this.dataType = "DateTime".equals(ignitionDataType) ? DataType.DateTime : toIgnitionDataType(factryDataType);
        this.createdTime = createdTime != null ? createdTime : Instant.EPOCH;
    }

    /** The Factry UUID this node reads from. */
    public String measurementUUID() {
        return measurementUUID;
    }

    /** The array element this node reads, or -1 when it reads the measurement's own values. */
    public int element() {
        return element;
    }

    /** True for the base path of an array measurement, whose values are whole lists. */
    public boolean isArray() {
        return array;
    }

    @Override
    public UUID nodeId() {
        return nodeId;
    }

    @Override
    public QualifiedPath source() {
        return source;
    }

    @Override
    public Optional<DataType> dataType() {
        return Optional.ofNullable(dataType);
    }

    @Override
    public Instant createdTime() {
        return createdTime;
    }

    @Override
    public Optional<Instant> retiredTime() {
        return Optional.empty();
    }

    static DataType toIgnitionDataType(String factryDataType) {
        if (factryDataType == null) {
            return DataType.Float8;
        }
        switch (factryDataType) {
            case "boolean":
                return DataType.Boolean;
            case "number":
                return DataType.Float8;
            case "string":
                return DataType.String;
            default:
                return DataType.Float8;
        }
    }
}
