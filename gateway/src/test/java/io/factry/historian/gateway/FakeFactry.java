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

import java.util.ArrayList;
import java.util.List;

/**
 * Stands in for Factry in unit tests: the gRPC channel is never used, because the calls the
 * module makes are overridden. Like Factry, the keyword filter matches substrings and pages its
 * answer, and a create with a name that already exists does not return the existing measurement.
 * Call {@link #shutdown()} after the test.
 */
class FakeFactry extends FactryGrpcClient {
    final List<Measurement> measurements = new ArrayList<>();
    final List<String> createdNames = new ArrayList<>();
    final List<Points> sent = new ArrayList<>();

    FakeFactry() {
        super("localhost", 1, "collector", "token", false, false, null);
    }

    void add(String name, String uuid) {
        add(name, uuid, "Active");
    }

    void add(String name, String uuid, String status) {
        measurements.add(Measurement.newBuilder().setName(name).setUuid(uuid).setStatus(status).build());
    }

    void setStatus(String uuid, String status) {
        measurements.replaceAll(m -> m.getUuid().equals(uuid) ? m.toBuilder().setStatus(status).build() : m);
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
        for (CreateMeasurement m : request.getMeasurementsList()) {
            createdNames.add(m.getName());
            if (measurements.stream().noneMatch(e -> e.getName().equals(m.getName()))) {
                add(m.getName(), "new-" + m.getName());
            }
        }
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
