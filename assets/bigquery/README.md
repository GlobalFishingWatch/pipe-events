# Incremental fishing detection

## Challenges and solutions

Conceptual challenges:
1. Fishing events can cross multiple days.
2. Fishing events are defined by all messages within the event. We apply filters that depend on all messages existing (e.g. average speed).
3. Fishing events could theoretically go back all the way to 2012 because there's no hard limit that would cut off events if a vessel was continuously fishing. In fact the longest event is currently > 100 days but this could be much higher.
4. We apply the mutable field `overlapping_and_short`.
5. The definition of fishing events depends on the best_vessel_class which is mutable over time and dictates whether to use `nnet_score` or `night_loitering` for each vessel.

Solutions:
1. Because of challenge #1 any incremental solution requires a merge step.
2. Because of challenge #2 we need to keep all messages available as long as an event isn't "closed" yet because the speed threshold may or may not be exceeded as we add more messages.
3. Because of challenge #3 we potentially need to keep a very long history of messages.
4. Because of challenge #4 we need to calculate fishing events by segment (which fortunately has been the default definition).
5. Because of challenge #5 we need to develop 2 parallel pipelines: 1. calculating fishing events based on `nnet_score`; 2. calculating fishing events based on `night_loitering`. In the daily merge step we then look up best_vessel_class and choose the right fishing event for each vessel.

Challenges based on solutions:
1. Solutions #2 and #3 combined mean that we have to implement another incremental mechanism. We cannot rescan the message history for all fishing events every day. Instead, we need to incrementally upsert new and open fishing events. This cannot happen in the first incremental load, because `research_messages` is partitioned by message timestamp. Instead, we first generate an incremental table that contains messages and is partitioned by fishing event end date, which is a cheap way of keeping track of "open" fishing events (which must be on the most recent event end date because the maximum gap within a fishing event is 2 hours).


## Processing steps
### Original setup 
Previously, fishing events was split into two steps:
1. Calculate fishing events based on `research_messages`, add `vessel_id` and all other identity information and remove `overlapping_and_short` segments. Based on the latest best_vessel_class fishing events were either based on `nnet_score` (`best_vessel_class != 'squid_jigger'`) or `night_loitering (`best_vessel_class = 'squid_jigger'`).
2. Add authorization and regions information.

### Incremental setup
1. The former first step is split into 3 stages:
    1. Calculate fishing events by segment for any given time period (backfill or yearly/monthly/daily increment are possible). Annotate the fishing events on messages so that we have all messages for each fishing event. We call the output `temp_fishing_event_messages_temp`. The suffix `_temp` shows that this table does not need to be stored permanently but is only used in the next step (therefore BQ temporary tables could be used). For incremental loads we pad the previous day, so we can merge fishing events in the next step.

    The code for this is exactly the first half of the original first step.

    2. Merge `fishing_event_messages_temp` with the existing `fishing_event_messages_merged` table. The merge step retrieves `fishing_event_messages_merged` from the last two hours of the previous day and attempts to merge messages by `seg_id` and `timestamp` with the latest incremental `fishing_event_messages_temp`. If fishing events overlap they get merged, which means updating the `event_end` of the "open" fishing event messages, and updating the `event_start` of the new fishing event messages.

    This step is entirely new. The most similar existing concept in pipe3 to this table is `raw_port_events`. Technically, at this point we only store "potential" events and not all of them will eventually pass all filters.

    3. Generate final fishing events which are based on all messages within an event. Apply filters, add `vessel_id` and identity fields, and remove `overlapping_and_short` segments.
