package io.factry.historian.gateway;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.factry.historian.proto.AssetProperties;
import io.factry.historian.proto.Assets;
import io.factry.historian.proto.Collectors;
import io.factry.historian.proto.CreateMeasurement;
import io.factry.historian.proto.CreateMeasurementsReply;
import io.factry.historian.proto.CreateMeasurementsRequest;
import io.factry.historian.proto.CreatePointsReply;
import io.factry.historian.proto.GetAssetPropertiesRequest;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.Measurement;
import io.factry.historian.proto.MeasurementRequest;
import io.factry.historian.proto.Measurements;
import io.factry.historian.proto.Points;
import io.factry.historian.proto.QueryTimeseriesRequest;
import io.factry.historian.proto.QueryTimeseriesResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Stands in for Factry in unit tests: the gRPC channel is never used, because the calls the
 * module makes are overridden. The module writes as collector {@link #OWN}. Like Factry, the
 * keyword filter matches substrings and pages its answer, and a create with a name that already
 * exists does not return the existing measurement: it adds a Discovered duplicate, or, when the
 * existing one is in another time-series database, an Active measurement. Every change stamps the
 * measurement's updatedAt, one second later each time. Deleted measurements are only listed by
 * GetMeasurements, as in Factry. Call {@link #shutdown()} after the test.
 */
class FakeFactry extends FactryGrpcClient {
    static final String OWN = "collector";
    static final String OTHER = "other-collector";

    final List<Measurement> measurements = new ArrayList<>();
    final List<String> createdNames = new ArrayList<>();
    /** Every measurement Factry was asked to create, with its data type and settings. */
    final List<CreateMeasurement> created = new ArrayList<>();
    final List<Points> sent = new ArrayList<>();
    /** Number of single-name lookup calls (pages) made. */
    int lookups = 0;
    /** Number of measurement list calls without a keyword (pages of a full cache refresh). */
    int fullLoads = 0;
    /** Every GetMeasurements request. */
    final List<MeasurementRequest> measurementRequests = new ArrayList<>();
    /** Seconds of the last updatedAt given. */
    long clock = 1_000;
    /** How many of the next single-name lookups fail, and with which gRPC status. */
    int lookupFailures = 0;
    Status lookupFailure = Status.INVALID_ARGUMENT;
    /** Runs inside each create, after the measurements were added. */
    Runnable onCreate = () -> { };
    /** Whether existing measurements with the name of a new one are in another database. */
    boolean existingInOtherDatabase = false;
    /** The collector and status Factry gives a created measurement. */
    String createdCollector = OWN;
    String createdStatus = "Active";
    /** When set, creates fail with this gRPC status. */
    Status createFailure = null;
    Assets assets = Assets.getDefaultInstance();
    AssetProperties assetProperties = AssetProperties.getDefaultInstance();
    /** When set, asset property fetches fail with this gRPC status. */
    Status assetPropertiesFailure = null;
    /** The answer to every history query. */
    QueryTimeseriesResponse queryResponse = QueryTimeseriesResponse.getDefaultInstance();
    /** How many of the next history queries fail with {@link #queryFailure}. */
    int queryFailures = 0;
    Status queryFailure = Status.UNAVAILABLE;

    FakeFactry() {
        super("localhost", 1, OWN, "token", false, false, null);
    }

    void add(String name, String uuid) {
        add(name, uuid, "Active");
    }

    void add(String name, String uuid, String status) {
        add(name, uuid, OWN, status);
    }

    void add(String name, String uuid, String collectorUUID, String status) {
        measurements.add(Measurement.newBuilder().setName(name).setUuid(uuid)
                .setCollectorUUID(collectorUUID).setStatus(status).setUpdatedAt(tick()).build());
    }

    void setStatus(String uuid, String status) {
        measurements.replaceAll(m -> m.getUuid().equals(uuid)
                ? m.toBuilder().setStatus(status).setUpdatedAt(tick()).build() : m);
    }

    private Timestamp tick() {
        return Timestamp.newBuilder().setSeconds(++clock).build();
    }

    private static boolean isDeleted(Measurement m) {
        return "Deleted".equalsIgnoreCase(m.getStatus());
    }

    @Override
    public Measurements getMeasurements(MeasurementRequest request) {
        measurementRequests.add(request);
        String collector = request.getCollectorUUID().isEmpty() ? OWN : request.getCollectorUUID();
        List<Measurement> found = new ArrayList<>();
        for (Measurement m : measurements) {
            // Factry compares updatedAt strictly (checked live on 8.2.3).
            if (m.getCollectorUUID().equals(collector) && (!request.hasSince()
                    || Timestamps.compare(m.getUpdatedAt(), request.getSince()) > 0)) {
                found.add(m);
            }
        }
        return Measurements.newBuilder().addAllMeasurements(found).build();
    }

    @Override
    public Measurements getMeasurementsByFilterWithDeadline(GetMeasurementsByFilterRequest request) {
        lookups++;
        if (lookupFailures > 0) {
            lookupFailures--;
            throw new StatusRuntimeException(lookupFailure);
        }
        return getMeasurementsByFilter(request);
    }

    @Override
    public Measurements getMeasurementsByFilter(GetMeasurementsByFilterRequest request) {
        if (!request.hasKeyword()) {
            fullLoads++;
        }
        List<Measurement> found = new ArrayList<>();
        for (Measurement m : measurements) {
            if (!isDeleted(m) && (!request.hasKeyword() || m.getName().contains(request.getKeyword()))) {
                found.add(m);
            }
        }
        int from = (int) Math.min(request.getPagination().getOffset(), found.size());
        int to = request.getPagination().getLimit() > 0
                ? (int) Math.min(from + request.getPagination().getLimit(), found.size())
                : found.size();
        return Measurements.newBuilder()
                .addAllMeasurements(found.subList(from, to))
                .setTotal(found.size())
                .build();
    }

    @Override
    public CreateMeasurementsReply createMeasurements(CreateMeasurementsRequest request) {
        if (createFailure != null) {
            throw new StatusRuntimeException(createFailure);
        }
        for (CreateMeasurement m : request.getMeasurementsList()) {
            createdNames.add(m.getName());
            created.add(m);
            if (measurements.stream().noneMatch(e -> e.getName().equals(m.getName()) && !isDeleted(e))) {
                add(m.getName(), "new-" + m.getName(), createdCollector, createdStatus);
            } else if (existingInOtherDatabase) {
                add(m.getName(), "own-" + m.getName(), createdCollector, "Active");
            } else {
                add(m.getName(), "duplicate-" + m.getName(), createdCollector, "Discovered");
            }
        }
        onCreate.run();
        return CreateMeasurementsReply.getDefaultInstance();
    }

    @Override
    public CreatePointsReply createPoints(Points points) {
        sent.add(points);
        return CreatePointsReply.getDefaultInstance();
    }

    @Override
    public QueryTimeseriesResponse queryTimeseries(QueryTimeseriesRequest request) {
        if (queryFailures > 0) {
            queryFailures--;
            throw new StatusRuntimeException(queryFailure);
        }
        return queryResponse;
    }

    @Override
    public Collectors getCollectors() {
        return Collectors.getDefaultInstance();
    }

    @Override
    public Assets getAssets() {
        return assets;
    }

    @Override
    public AssetProperties getAssetProperties(GetAssetPropertiesRequest request) {
        if (assetPropertiesFailure != null) {
            throw new StatusRuntimeException(assetPropertiesFailure);
        }
        return assetProperties;
    }
}
