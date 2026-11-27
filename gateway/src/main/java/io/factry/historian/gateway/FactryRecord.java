package io.factry.historian.gateway;

import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.ignition.common.FillMode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.sqltags.history.InterpolationMode;
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
    private final String ignitionDataType;
    private final InterpolationMode deadbandStyle;

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
     * @param ignitionDataType the tag's Ignition data type, or null. DateTime reads as dates
     *                         (Factry keeps dates as numbers); the rest only picks the fill.
     */
    public FactryRecord(String measurementUUID, int element, QualifiedPath source, String factryDataType,
                        String ignitionDataType, Instant createdTime) {
        this(measurementUUID, element, source, factryDataType, ignitionDataType, null, createdTime);
    }

    /**
     * @param ignitionDataType the tag's Ignition data type, from the metadata or the measurement,
     *                         or null
     * @param deadbandStyle    the tag's History Deadband Style (an InterpolationMode name), or null
     */
    public FactryRecord(String measurementUUID, int element, QualifiedPath source, String factryDataType,
                        String ignitionDataType, String deadbandStyle, Instant createdTime) {
        // Elements share a measurement, so the index is part of the node id to keep it unique.
        String id = element >= 0 ? measurementUUID + "[" + element + "]" : measurementUUID;
        this.nodeId = UUID.nameUUIDFromBytes(id.getBytes());
        this.measurementUUID = measurementUUID;
        this.element = element;
        this.array = element < 0 && factryDataType != null && factryDataType.startsWith("[]");
        this.source = source;
        this.dataType = "DateTime".equals(ignitionDataType) ? DataType.DateTime : toIgnitionDataType(factryDataType);
        this.createdTime = createdTime != null ? createdTime : Instant.EPOCH;
        this.ignitionDataType = ignitionDataType;
        this.deadbandStyle = parseStyle(deadbandStyle);
    }

    private static InterpolationMode parseStyle(String style) {
        if (style == null) {
            return null;
        }
        try {
            return InterpolationMode.valueOf(style);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * How empty buckets of this tag are filled when the query leaves it to the historian
     * (FillMode.DERIVED), as Ignition's own historians do (QueryUtils.ColumnMapping.getFillMode):
     * LINEAR for an analog tag, PREV (hold) for a discrete one. String, Boolean and DateTime tags hold; else
     * the Deadband Style decides, and Auto or an unknown style goes by the tag's Ignition type: a
     * Float (or Float array) is analog. With neither known (another collector's measurement, an
     * asset property, a measurement older than the settings) the tag holds, as before the line:
     * only a tag known to be analog is drawn as one.
     */
    public FillMode derivedFill() {
        // Not a number: the SQL historian never interpolates one (createInterpolator), whatever
        // its Deadband Style.
        if (dataType == DataType.Boolean || dataType == DataType.String || dataType == DataType.DateTime) {
            return FillMode.PREV;
        }
        if (deadbandStyle != null && deadbandStyle != InterpolationMode.Auto) {
            return deadbandStyle == InterpolationMode.Discrete ? FillMode.PREV : FillMode.LINEAR;
        }
        if (ignitionDataType != null) {
            return ignitionDataType.replaceFirst("Array$", "").startsWith("Float") ? FillMode.LINEAR : FillMode.PREV;
        }
        return FillMode.PREV;
    }

    /**
     * The Deadband Style to tell Ignition, so its own interpolation (aggregates it computes from
     * raw points) follows the tag too; empty when nothing is known about the tag beyond its
     * Factry type, which Ignition already reads from the data type.
     */
    public Optional<InterpolationMode> deadbandStyle() {
        if (deadbandStyle == null && ignitionDataType == null) {
            return Optional.empty();
        }
        return Optional.of(derivedFill() == FillMode.LINEAR ? InterpolationMode.Analog : InterpolationMode.Discrete);
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
