package au.com.ps4impact.thingsboard_custom_rule_nodes

import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures
import groovy.json.JsonOutput
import org.jetbrains.annotations.Nullable
import org.thingsboard.rule.engine.api.TbContext
import org.thingsboard.rule.engine.api.TbNode
import org.thingsboard.rule.engine.api.TbNodeConfiguration
import org.thingsboard.rule.engine.api.TbNodeException
import org.thingsboard.rule.engine.api.util.TbNodeUtils
import org.thingsboard.server.common.data.DataConstants
import org.thingsboard.server.common.data.kv.AttributeKvEntry
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery
import org.thingsboard.server.common.data.kv.BasicTsKvEntry
import org.thingsboard.server.common.data.kv.DoubleDataEntry
import org.thingsboard.server.common.data.kv.TsKvEntry
import org.thingsboard.server.common.msg.TbMsg
import org.thingsboard.server.common.msg.session.SessionMsgType

import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import java.util.concurrent.Executors

/**
 * This node updates the mean value of recent telemetry measurements from a given sensor.
 * "Recent" means "within a given time period" such as five minutes.
 *
 * If the mean value now exceeds a given threshold, increment an attribute on the tenant reflecting
 * how many devices now exceed the threshold.
 *
 * If the mean value is now below the threshold, decrement the tenant attribute.
 *
 * Note that this requires tracking, for each device, whether the given sensor mean value
 * currently exceeds the threshold.
 *
 * Store this as a server-side attribute against the device.
 *
 * The threshold values will be stored as server attributes on the tenant.
 *
 * The obvious corollary to this behaviour is that if the user changes one or more threshold values, the number of
 * devices currently exceeding the threshold(s) should automatically reset. This is OUT OF SCOPE for this node.
 */
class UpdateMeanTelemetryMeasurementsNode implements TbNode {

    /** Message type for setting attributes */
    static final POST_ATTRIBUTES_REQUEST = SessionMsgType.POST_ATTRIBUTES_REQUEST.toString()

    /** Server scope for setting attributes */
    static final SERVER_SCOPE = DataConstants.SERVER_SCOPE.toString()

    /** Holds node configuration */
    private UpdateMeanTelemetryMeasurementsNodeConfiguration config

    private Executor executor

    /** Set config */
    void init(final TbContext ctx, final TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert configuration, UpdateMeanTelemetryMeasurementsNodeConfiguration
        this.executor = Executors.newSingleThreadExecutor()
    }

    /**
     * Calculate the mean of recent measurements for a given device and save as server-side device attribute.
     *
     * Determine whether the mean exceeds the threshold, and set this boolean flag as a server-side device attribute.
     * If the mean now exceeds the threshold, but did not exceed before, increment the server-side
     *     tenant attribute tracking exceeded count. (Exceeds flag defaults to, or starts out as, false).
     * Similarly, if the mean now no longer exceeds the threshold, decrement the count.
     *
     * If the threshold value isn't set as a tenant attribute, then the device should not show
     * as exceeding the threshold. This means the alert map will be empty until the threshold is set.
     */
    void onMsg(final TbContext ctx, final TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        final endTs = System.currentTimeMillis()
        final startTs = endTs - config.timePeriodSeconds * 1000

        // this query returns a list containing a single entry with the average of all timeseries data points
        // between the start and end timestamps inclusive
        final query = new BaseReadTsKvQuery(config.timeSeriesName, startTs, endTs)
        final measurements = ctx.timeseriesService.findAll(ctx.tenantId, msg.originator, [query]).get()
        final mean = defaultVal 0d, { measurements[0].doubleValue.get() }

        final entry = new BasicTsKvEntry(System.currentTimeMillis(), new DoubleDataEntry(config.timeSeriesMeanName, mean))
        ctx.telemetryService.saveAndNotify(ctx.tenantId, msg.originator, [entry], new FutureCallback<Void>() {
            void onSuccess(Void unused) {
                checkThresholds ctx, msg, mean
            }

            void onFailure(Throwable throwable) {
                ctx.tellFailure(msg, th)
            }
        })
    }

    private void checkThresholds(final TbContext ctx, final TbMsg msg, final double mean) {
        final exceedsThresholdFuture = ctx.attributesService.find(
                ctx.tenantId, msg.originator, SERVER_SCOPE, config.exceedsThresholdAttribute)

        final thresholdFuture = ctx.attributesService.find(
                ctx.tenantId, ctx.tenantId, SERVER_SCOPE, config.thresholdAttribute)

        Futures.allAsList(exceedsThresholdFuture, thresholdFuture)

        Futures.addCallback(exceedsThresholdAttribute, new FutureCallback<Optional<AttributeKvEntry>>() {
            @Override
            void onSuccess(@Nullable Optional<AttributeKvEntry> entry) {
                final exceedsThreshold = defaultVal false,
                        { entry.get().booleanValue.get() }
            }

            @Override
            void onFailure(Throwable e) { ctx.tellFailure msg, e }
        }, executor)

        Futures.allAsList()

        final thresholdAttribute = ctx.attributesService.find(
                ctx.tenantId, ctx.tenantId, SERVER_SCOPE, config.thresholdAttribute).get()

        // If threshold isn't defined, default to max value - that way device can't exceed it
        final threshold = defaultVal Double.MAX_VALUE, { thresholdAttribute.get().doubleValue.get() }

        // if mean exceeds threshold, but the device isn't currently shown as exceeding,
        // flag the device and bump the count
        if (mean > threshold) {
            if (! exceedsThreshold) {
                setDeviceExceedsThreshold(ctx, msg, true)
                adjustExceedsThresholdCount(ctx, msg, 1)
            }
        // if mean doesn't exceed threshold, but the device is currently shown as exceeding,
        // unflag the device and drop the count by 1
        } else if (exceedsThreshold) {
            setDeviceExceedsThreshold(ctx, msg, false)
            adjustExceedsThresholdCount(ctx, msg, -1)
        }
    }

    /** Set the boolean "exceeds threshold" attribute on a device via a new message */
    private void setDeviceExceedsThreshold(final TbContext ctx, final TbMsg msg, final boolean exceeds) {
        final newMsg = JsonOutput.toJson([(config.exceedsThresholdAttribute): exceeds])
        ctx.newMsg(msg.queueName, POST_ATTRIBUTES_REQUEST, msg.originator, msg.metaData, newMsg)
    }

    /**
     * Adjust the numeric "exceeds threshold count" attribute on a tenant by a given amount.
     *
     * Need to look up the current count value first. Default to zero if not set. Count can't go negative.
     *
     * Perform the adjustment via a new message.
     */
    private void adjustExceedsThresholdCount(final TbContext ctx, final TbMsg msg, final int amount) {
        if (amount == 0) {
            // no need to adjust - we're done
            return
        }

        final attributeEntry = ctx.attributesService.find(
                ctx.tenantId, ctx.tenantId, SERVER_SCOPE, config.exceedsThresholdCountAttribute).get()

        final currentCount = defaultVal 0, {
            attributeEntry.get().longValue.get()
        }

        long count = currentCount + amount
        if (count < 0) {
            count = 0
        }
        final newMsg = JsonOutput.toJson([(config.exceedsThresholdCountAttribute): count])
        ctx.newMsg(msg.queueName, POST_ATTRIBUTES_REQUEST, ctx.tenantId, msg.metaData, newMsg)
    }

    /**
     * Try to retrieve an underlying value from a chain of one or more Optionals.
     * If any retrieval fails b/c there's no such value, return a default.
     */
    private static <T> T defaultVal(final T defaultValue, final Closure optionalsChain) {
        try {
            optionalsChain.call()
        } catch (NoSuchElementException ignored) {
            return defaultValue
        }
    }

    @Override
    void destroy() {

    }
}
