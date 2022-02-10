const { isObject, percentOf, round } = require('../../utils/utils');

/**
 *
 */
class StatsAggregator {
    /**
     * Calculate aggregates associated with the peer connection tracks
     *
     * @param {Object} extractedData - Data associated with a single peer connection.
     * @returns
     */
    _calculateTrackAggregates(pcData) {
        let totalPacketsSent = 0;
        let totalSentPacketsLost = 0;
        let totalPacketsReceived = 0;
        let totalReceivedPacketsLost = 0;

        const tracks = Object.keys(pcData).filter(
            pcDataEntry => isObject(pcData[pcDataEntry]) && pcData[pcDataEntry].hasOwnProperty('mediaType')
        );

        // packetsLost and packetsSent are sent as totals for each point in time they were collected, thus
        // the last value in the array is going to be the total lost/sent for a track.
        // We then add them together to get the totals for the peer connection.
        tracks.forEach(trackSsrc => {
            const { packetsSentLost = [], packetsSent = [],
                packetsReceivedLost = [], packetsReceived = [] } = pcData[trackSsrc];

            if (packetsSentLost.length && packetsSent.length) {
                totalSentPacketsLost += packetsSentLost.at(-1);
                totalPacketsSent += packetsSent.at(-1);
            }

            if (packetsReceivedLost.length && packetsReceived.length) {
                totalReceivedPacketsLost += packetsReceivedLost.at(-1);
                totalPacketsReceived += packetsReceived.at(-1);
            }
        });

        let sentPacketsLostPct = 0;

        if (totalPacketsSent
            && totalSentPacketsLost > 0
            && totalSentPacketsLost < totalPacketsSent) {
            sentPacketsLostPct = percentOf(totalSentPacketsLost, totalPacketsSent) || 0;
        }
        let receivedPacketsLostPct = 0;

        if (totalPacketsReceived
            && totalReceivedPacketsLost > 0
            && totalReceivedPacketsLost < totalPacketsReceived) {
            receivedPacketsLostPct = percentOf(totalReceivedPacketsLost, totalPacketsReceived) || 0;
        }

        return {
            totalSentPacketsLost,
            totalPacketsSent,
            sentPacketsLostPct,
            totalReceivedPacketsLost,
            totalPacketsReceived,
            receivedPacketsLostPct
        };
    }

    /**
     * Calculate aggregates associated with the transport report of a peer connection.
     *
     * @param {*} pcData - Data associated with a single peer connection.
     */
    _calculateTransportAggregates(pcData) {
        const { transport = {} } = pcData;
        const { rtts = [] } = transport;


        // Simply calculate the average rtt for this peer connection, more calculations can be added as needed.
        return {
            meanRtt: round(rtts.reduce((a, b) => a + b, 0) / (rtts.length || 1), 2)
        };
    }

    /**
     *
     * @param {*} extractedData - Data extracted by the QualityStatsCollector.
     */
    calculateAggregates(extractedData) {
        const resultMap = {};

        // Go through each peer connection and compute aggregates.
        Object.keys(extractedData).forEach(pc => {
            resultMap[pc] = { isP2P: extractedData[pc].isP2P,
                dtlsErrors: extractedData[pc].dtlsErrors,
                dtlsFailure: extractedData[pc].dtlsFailure };

            const pcTrackResults = this._calculateTrackAggregates(extractedData[pc]);
            const pcTransportResults = this._calculateTransportAggregates(extractedData[pc]);

            resultMap[pc].trackAggregates = pcTrackResults;
            resultMap[pc].transportAggregates = pcTransportResults;
        });

        return resultMap;
    }
}

module.exports = StatsAggregator;