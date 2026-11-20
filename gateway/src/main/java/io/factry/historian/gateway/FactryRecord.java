package io.factry.historian.gateway;

import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class FactryRecord implements HistoricalNode {
    private final UUID nodeId;
    private final QualifiedPath source;
    private final DataType dataType;
    private final Instant createdTime;

    public FactryRecord(String measurementUUID, QualifiedPath source, String factryDataType, Instant createdTime) {
        this(measurementUUID, source, factryDataType, null, createdTime);
    }

    /**
     * @param ignitionDataType the tag's Ignition data type as recorded on the measurement, or null.
     *                         Only DateTime is used: Factry keeps dates as numbers.
     */
    public FactryRecord(String measurementUUID, QualifiedPath source, String factryDataType,
                        String ignitionDataType, Instant createdTime) {
        this.nodeId = UUID.nameUUIDFromBytes(measurementUUID.getBytes());
        this.source = source;
        this.dataType = "DateTime".equals(ignitionDataType) ? DataType.DateTime : toIgnitionDataType(factryDataType);
        this.createdTime = createdTime != null ? createdTime : Instant.EPOCH;
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
