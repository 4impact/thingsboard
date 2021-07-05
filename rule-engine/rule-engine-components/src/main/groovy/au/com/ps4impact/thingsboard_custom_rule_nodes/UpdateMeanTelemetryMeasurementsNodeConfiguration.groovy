package au.com.ps4impact.thingsboard_custom_rule_nodes

import org.thingsboard.rule.engine.api.NodeConfiguration

/**
 * Configuration for UpdateMeanTelemetryMeasurementsNode.
 *
 * Properties specified through configuration:
 *
 * timeSeriesName - name of sensor attribute whose mean to calculate
 * timeSeriesMeanName - name of calculated mean value
 * timePeriodSeconds - number of seconds of telemetry readings history to include in mean calculation
 * thresholdAttribute - name of server-side tenant attribute holding the threshold value
 * exceedsThresholdAttribute- name of server-side device attribute indicating whether a particular
 *     device currently exceeds the threshold
 * exceedsThresholdCountAttribute - name of server-side tenant attribute tracking the number of sensors
 *     whose mean value currently exceeds the threshold
 */
class UpdateMeanTelemetryMeasurementsNodeConfiguration
        implements NodeConfiguration<UpdateMeanTelemetryMeasurementsNodeConfiguration> {

    /** Name of device timeseries holding the sensor measurements whose mean to calculate */
    String timeSeriesName

    /** Name of server-side device attribute holding the calculated mean */
    String timeSeriesMeanName

    /** Number of seconds of measurement history to include in the mean calculation */
    int timePeriodSeconds

    /** Name of server-side tenant attribute holding the threshold value */
    String thresholdAttribute

    /** Name of server-side device attribute indicating whether a given device currently exceeds the threshold */
    String exceedsThresholdAttribute

    /** Name of server-side tenant attribute indicating how many devices currently exceed the threshold */
    String exceedsThresholdCountAttribute

    /** By default set all config properties to illustrative example values */
    UpdateMeanTelemetryMeasurementsNodeConfiguration defaultConfiguration() {
        new UpdateMeanTelemetryMeasurementsNodeConfiguration(
            timeSeriesName: 'so2',
            timeSeriesMeanName: 'so2Mean',
                timePeriodSeconds: 300,
            thresholdAttribute: 'so2Threshold',
            exceedsThresholdAttribute: 'so2MeanExceeded',
            exceedsThresholdCountAttribute: 'so2MeanExceededCount'
        )
    }
}
