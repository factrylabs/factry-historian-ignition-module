package io.factry.historian.gateway;

import io.factry.historian.proto.Assets;
import io.factry.historian.proto.Collectors;
import io.factry.historian.proto.CreateMeasurement;
import io.factry.historian.proto.CreateMeasurementsReply;
import io.factry.historian.proto.CreateMeasurementsRequest;
import io.factry.historian.proto.CreatePointsReply;
import io.factry.historian.proto.GetMeasurementsByFilterRequest;
import io.factry.historian.proto.Measurement;
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
 * existing one is in another time-series database, an Active measurement. Call
 * {@link #shutdown()} after the test.
 */
class FakeFactry extends FactryGrpcClient {
    static final String OWN = "collector";
    static final String OTHER = "other-collector";

    final List<Measurement> measurements = new ArrayList<>();
    final List<String> createdNames = new ArrayList<>();
    final List<Points> sent = new ArrayList<>();
    /** Number of single-name lookup calls (pages) made. */
    int lookups = 0;
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
                .setCollectorUUID(collectorUUID).setStatus(status).build());
    }

    void setStatus(String uuid, String status) {
        measurements.replaceAll(m -> m.getUuid().equals(uuid) ? m.toBuilder().setStatus(status).build() : m);
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
        List<Measurement> found = new ArrayList<>();
        for (Measurement m : measurements) {
            if (!request.hasKeyword() || m.getName().contains(request.getKeyword())) {
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
            if (measurements.stream().noneMatch(e -> e.getName().equals(m.getName()))) {
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
        return QueryTimeseriesResponse.getDefaultInstance();
    }

    @Override
    public Collectors getCollectors() {
        return Collectors.getDefaultInstance();
    }

    @Override
    public Assets getAssets() {
        return Assets.getDefaultInstance();
    }
}
