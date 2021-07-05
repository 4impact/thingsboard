package au.com.ps4impact.thingsboard_custom_rule_nodes

import com.google.common.util.concurrent.FutureCallback
import org.thingsboard.rule.engine.api.RuleNode
import org.thingsboard.rule.engine.api.TbContext
import org.thingsboard.rule.engine.api.TbNode
import org.thingsboard.rule.engine.api.TbNodeConfiguration
import org.thingsboard.rule.engine.api.TbNodeException
import org.thingsboard.rule.engine.api.util.TbNodeUtils
import org.thingsboard.server.common.data.DataConstants
import org.thingsboard.server.common.data.id.EntityId
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery
import org.thingsboard.server.common.data.kv.BasicTsKvEntry
import org.thingsboard.server.common.data.kv.DoubleDataEntry
import org.thingsboard.server.common.data.plugin.ComponentType
import org.thingsboard.server.common.msg.TbMsg

import java.util.concurrent.ExecutionException

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
 * Use TB's async programming model to retrieve and set attribute and timeseries values. This means a series
 * of cascading method calls ending in a success or failure exit from this rule node.
 *
 * The obvious corollary to this behaviour is that if the user changes one or more threshold values, the number of
 * devices currently exceeding the threshold(s) should automatically reset. This is OUT OF SCOPE for this node.
 */
@RuleNode(
        type = ComponentType.ACTION,
        name = 'Update Mean Telemetry',
        configClazz = UpdateMeanTelemetryMeasurementsNodeConfiguration,
        nodeDescription = 'Updates mean telemetry value for a given timeseries',
        nodeDetails = 'Update mean telemetry value and keep device exceeds flag and tenant exceeds count in sync'
)
class UpdateMeanTelemetryMeasurementsNode implements TbNode {

    /** Server scope for setting attributes */
    static final SERVER_SCOPE = DataConstants.SERVER_SCOPE.toString()

    /** Holds node configuration */
    private UpdateMeanTelemetryMeasurementsNodeConfiguration config

    /** Set config */
    void init(final TbContext ctx, final TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert configuration, UpdateMeanTelemetryMeasurementsNodeConfiguration
    }

    /**
     * Calculate the mean of recent measurements for a given device and save as telemetry.
     *
     * Determine whether the mean exceeds the threshold, and set this boolean flag as a server-side device attribute.
     * If the mean now exceeds the threshold, but did not exceed before, increment the server-side
     *     tenant attribute tracking exceeded count. (Exceeds flag defaults to, or starts out as, false).
     * Similarly, if the mean now no longer exceeds the threshold, decrement the count.
     */
    void onMsg(final TbContext ctx, final TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        final endTs = System.currentTimeMillis()
        final startTs = endTs - config.timePeriodSeconds * 1000

        // this query will return a single result with the average of all timeseries entries
        // between the start and end timestamps inclusive
        final query = new BaseReadTsKvQuery(config.timeSeriesName, startTs, endTs)

        //TODO consider running this query without blocking
        final measurements = ctx.timeseriesService.findAll(ctx.tenantId, msg.originator, [query]).get()
        final mean = defaultVal 0d, { measurements[0].doubleValue.get() }

        final entry = new BasicTsKvEntry(System.currentTimeMillis(), new DoubleDataEntry(config.timeSeriesMeanName, mean))
        ctx.telemetryService.saveAndNotify(ctx.tenantId, msg.originator, [entry], new FutureCallback<Void>() {
            void onSuccess(Void unused) { checkThresholds ctx, msg, mean }
            void onFailure(Throwable e) { ctx.tellFailure msg, e }
        })
    }

    /**
     * Check whether the device mean measurement currently exceeds (or not) the threshold, and update device flag
     * and tenant exceeds count depending on current state. These updates are both server side attributes.
     *
     * If the exceeds flag isn't set as a device attribute, that means the device is not currently flagged as
     * exceeding, so default to false.
     *
     * If the threshold value isn't set as a tenant attribute, then the device should not show
     * as exceeding the threshold. This means the alert map will be empty until the threshold is set.
     *
     * Note that no action is required in these cases:
     *
     * - mean exceeds threshold and device is already flagged as exceeding
     * - mean does not exceed threshold and device is currently flagged as not exceeding
     */
    private void checkThresholds(final TbContext ctx, final TbMsg msg, final double mean) {
        //TODO consider running this query without blocking
        final exceedsThresholdAttribute = ctx.attributesService.find(
                ctx.tenantId, msg.originator, SERVER_SCOPE, config.exceedsThresholdAttribute).get()
        final exceedsThreshold = defaultVal false,
                { exceedsThresholdAttribute.get().booleanValue.get() }

        //TODO consider running this query without blocking
        final thresholdAttribute = ctx.attributesService.find(
                ctx.tenantId, ctx.tenantId, SERVER_SCOPE, config.thresholdAttribute).get()

        // If threshold isn't defined, default to max value - that way device can't exceed it
        final threshold = defaultVal Double.MAX_VALUE, { thresholdAttribute.get().doubleValue.get() }

        // if mean exceeds threshold, but device isn't currently shown as exceeding, flag device and bump the count
        if (mean > threshold) {
            if (! exceedsThreshold) {
                saveAttribute(ctx, msg, msg.originator, config.exceedsThresholdAttribute, true,
                        { adjustExceedsThresholdCount ctx, msg, 1 })
            }

        // if mean doesn't exceed threshold, but the device is currently shown as exceeding,
        // unflag the device and drop the count by 1
        } else if (exceedsThreshold) {
            saveAttribute(ctx, msg, msg.originator, config.exceedsThresholdAttribute, false,
                    { adjustExceedsThresholdCount ctx, msg, -1 })
        }
    }

    /**
     * Adjust the numeric "exceeds threshold count" attribute on a tenant by a given amount.
     *
     * Need to look up the current count value first. Default to zero if not set. Count can't go negative.
     *
     * This is the last step in the async cascade.
     */
    private void adjustExceedsThresholdCount(final TbContext ctx, final TbMsg msg, final int amount) {
        if (amount == 0) {
            // no need to adjust - we're done
            ctx.tellSuccess msg
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

        saveAttribute(ctx, msg, ctx.tenantId, config.exceedsThresholdCountAttribute, count)
    }

    /**
     * Save an attribute to an entity in server scope. On success call the given success callback, defaulting to
     * sending a success message through the rule chain. (Note this means this will be the last step in the
     * async cascade).
     *
     * On failure send a failure message through the rule chain (thus ending the cascade).
     */
    private static void saveAttribute(final TbContext ctx, final TbMsg msg, final EntityId entityId, final String key,
                                      final val, final Closure successCallback = null) {
        ctx.telemetryService.saveAttrAndNotify(ctx.tenantId, entityId, SERVER_SCOPE, key, val, new FutureCallback<Void>() {
            void onSuccess(Void unused) { successCallback ? successCallback.call() : ctx.tellSuccess(msg) }
            void onFailure(Throwable e) { ctx.tellFailure msg, e }
        })
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
