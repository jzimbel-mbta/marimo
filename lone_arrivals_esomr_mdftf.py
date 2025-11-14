import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    from datetime import date, datetime, timedelta
    import polars as pl
    import urllib
    import os

    import pytz
    from pytz import timezone
    LOCAL_TZ = timezone("America/New_York")
    return LOCAL_TZ, date, datetime, mo, os, pl, pytz, timedelta, urllib


@app.cell
def _(LOCAL_TZ, datetime, mo, pytz, timedelta):
    yesterday_local = pytz.utc.localize(datetime.now()).astimezone(LOCAL_TZ).date() - timedelta(days=1)

    inputs = mo.md('''
    # Setup

    {date_range}

    {stops}
    ''').batch(
        date_range=mo.ui.date_range(label='date range', stop=yesterday_local, value=(yesterday_local, yesterday_local)),
        stops=mo.ui.multiselect(label='stops', options={'East Somerville': 'place-esomr', 'Medford/Tufts': 'place-mdftf'})
    )

    inputs
    return (inputs,)


@app.cell
def _(inputs, mo):
    START_DATE, END_DATE = inputs.value['date_range']
    STOPS = inputs.value['stops']

    mo.md('''
    /// attention | Please select at least one stop.
    ''') if len(STOPS) == 0 else None
    return END_DATE, START_DATE, STOPS


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- Initial setup

        install httpfs; load httpfs;

        -- Will contain all stop events at the specified STOPS
        create table if not exists all_arrivals as
        	from 'https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/2025-05-21-subway-on-time-performance-v1.parquet'
            limit 0;

        create or replace view lone_arrivals as
            from all_arrivals
            where stop_count = 1;

        describe lone_arrivals;
        """,
        output=False
    )
    return all_arrivals, lone_arrivals


@app.cell
def _(END_DATE, START_DATE, STOPS, all_arrivals, mo, timedelta):
    def load_service_date_query(d, stops):
        return f"""
        insert into all_arrivals
            from 'https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{d}-subway-on-time-performance-v1.parquet'
            where parent_station in {stops};
        """

    def service_dates(start_date, end_date):
        d = start_date
        while d != end_date:
            yield d
            d = d + timedelta(days=1)
        yield d

    mo.sql("truncate all_arrivals;")

    dates = service_dates(START_DATE, END_DATE)

    if len(STOPS) > 0:
        for d in mo.status.progress_bar(
            list(service_dates(START_DATE, END_DATE)),
            title="Loading arrival events",
            completion_title="Loaded arrival events"
        ):
            mo.sql(load_service_date_query(d, STOPS))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---

    ## All lone arrival events from selected service date range
    """
    )
    return


@app.cell
def _(END_DATE, START_DATE, STOPS, lone_arrivals, mo):
    # Forces cell to re-run if any inputs change
    _ = (START_DATE, END_DATE, STOPS)

    _df = mo.sql(f"from lone_arrivals;")
    return


@app.cell
def _(END_DATE, START_DATE, STOPS, lone_arrivals, mo):
    # Forces cell to re-run if any inputs change
    _ = (START_DATE, END_DATE, STOPS)

    lone_arrival_trip_ids = mo.sql(
        f"""
        select trip_id from lone_arrivals;
        """,
        output=False
    )
    return (lone_arrival_trip_ids,)


@app.cell
def _(END_DATE, LOCAL_TZ, START_DATE, datetime, lone_arrival_trip_ids, urllib):
    trip_ids = "(" + ",".join(f'"{trip_id}"' for trip_id in lone_arrival_trip_ids["trip_id"]) + ")"
    splunk_search = f"""index="rtr-prod" created_added_gtfs_trip_for_glides new_gtfs_trip_id IN {trip_ids}
    | eval vehicle_uid=substr(vehicle_id, 3)
    | eval date=strftime(_time, "%Y-%m-%d")
    | eval time=strftime(_time, "%H:%M:%S")
    | eval window_start=strftime(_time - 300, "%H:%M:%S")
    | eval window_end=strftime(_time + 300, "%H:%M:%S")
    | table date, time, window_start, window_end, vehicle_uid"""

    start_time_unix = int(
            LOCAL_TZ.localize(
            datetime(START_DATE.year, START_DATE.month, START_DATE.day)
        ).timestamp()
    )
    end_time_unix = int(
        LOCAL_TZ.localize(
            datetime(END_DATE.year, END_DATE.month, END_DATE.day, 23, 59, 59)
        ).timestamp()
    )

    search_url = f"https://mbta.splunkcloud.com/en-US/app/search/search?earliest={start_time_unix}&latest={end_time_unix}&q=search%20{urllib.parse.quote(splunk_search)}&display.page.search.mode=smart&dispatch.sample_ratio=1&workload_pool=&display.page.search.tab=statistics&display.general.type=statistics"
    return (search_url,)


@app.cell
def _(mo, os):
    files = mo.watch.directory(os.getcwd())
    return (files,)


@app.cell
def _(files, mo, os, search_url):
    file_options = [os.path.basename(f) for f in files.glob('*.csv')]
    inputs2 = mo.md(f'''
    ### **Now, download [lone arrival vehicle details]({search_url}) from splunk before continuing.**

    /// admonition | Note
    The link may not work if you've selected a large date range.  
    In that case, you may want to [Cumulative daily stats](#cumulative-daily-stats) instead.
    ///

    Export the table as csv and save to the same directory as this notebook, then select it below.

    {{filename}}
    ''').batch(
        filename=mo.ui.dropdown(options=file_options, label='choose a csv:')
    )

    inputs2
    return (inputs2,)


@app.cell
def _(inputs2, mo):
    filename = inputs2.value['filename']

    if filename is None:
        md = '/// attention | Please select a file.'
    else:
        md = 'Lone arrival vehicle details'

    mo.md(md)
    return (filename,)


@app.cell
def _(filename, mo, null):
    lone_arrival_vehicles = None

    if filename is not None:
        lone_arrival_vehicles = mo.sql(
            f"""
            from '{filename}';
            """,
            output=False
    )
    return (lone_arrival_vehicles,)


@app.cell
def _(LOCAL_TZ, datetime, lone_arrival_vehicles, pl, pytz, urllib):
    def get_iso_timestamp(d, t):
        naive_dt = datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)
        localized_dt = LOCAL_TZ.localize(naive_dt)
        iso_format = localized_dt.astimezone(pytz.utc).isoformat()

        return iso_format[:19] + 'Z'


    def get_dashboard_url(lone_arrival_vehicle):
        earliest = get_iso_timestamp(lone_arrival_vehicle['date'], lone_arrival_vehicle['window_start'])
        latest = get_iso_timestamp(lone_arrival_vehicle['date'], lone_arrival_vehicle['window_end'])
        return f'https://mbta.splunkcloud.com/en-US/app/search/transit_datautility_ocs_viz?tab=layout_1&form.global_time.earliest={urllib.parse.quote(earliest)}&form.global_time.latest={urllib.parse.quote(latest)}&form.cfg_rtr_index=rtr-prod&form.usercfg_export_time.earliest=%24global_time%24&form.usercfg_export_time.latest=now&form.usercfg_train_uid={lone_arrival_vehicle['vehicle_uid']}'

    df = None
    if lone_arrival_vehicles is not None:
        df = pl.DataFrame([{'map dashboard link': get_dashboard_url(row), **row} for row in lone_arrival_vehicles.rows(named=True)]).sort(('date', 'time'))
    df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---

    ## Cumulative daily stats
    """
    )
    return


@app.cell
def _(END_DATE, START_DATE, STOPS, all_arrivals, lone_arrivals, mo):
    # Forces cell to re-run if any inputs change
    _ = (START_DATE, END_DATE, STOPS)

    lone_arrival_daily_stats = mo.sql(
        f"""
        create view if not exists lone_arrival_daily_stats as
            with default_stats as (
                select distinct
                    service_date
                    ,parent_station
                    ,0 as total
                    ,map_from_entries([('ADDED', 0), ('SCHEDULED', 0), ('_OTHER', 0)]) as trip_type_counts
                from all_arrivals
            ),
            existing_lone_arrival_stats as (
                select
                	service_date
                	,parent_station
                	,count(*) as total
                	,histogram_exact(case
                    	when trip_id similar to 'ADDED-.+' then 'ADDED'
                		when trip_id similar to '\\d+' then 'SCHEDULED'
                    	-- else 'OTHER: ' || coalesce(trip_id, 'NULL')
                        else 'OTHER'
                    end, ['ADDED', 'SCHEDULED', '_OTHER']) as trip_type_counts
                from lone_arrivals
                group by service_date, parent_station
                order by service_date, parent_station
            )
            select
            	ds.service_date
            	,ds.parent_station
            	,coalesce(la.total, ds.total) as total
            	,coalesce(la.trip_type_counts, ds.trip_type_counts) as trip_type_counts
            from default_stats ds
            left join existing_lone_arrival_stats la
            	on ds.service_date = la.service_date
            	and ds.parent_station = la.parent_station
            group by all
            order by ds.service_date, ds.parent_station;


        select
            service_date
            ,parent_station
            ,total
            ,trip_type_counts['ADDED'] as added_count
            ,trip_type_counts['SCHEDULED'] as scheduled_count
            ,trip_type_counts['_OTHER'] as other_count
        from lone_arrival_daily_stats;
        """
    )
    return (lone_arrival_daily_stats,)


@app.cell
def _(END_DATE, START_DATE, STOPS, lone_arrival_daily_stats, mo):
    # Forces cell to re-run if any inputs change
    _ = (START_DATE, END_DATE, STOPS)

    deltas_result = mo.sql(
        f"""
        with esomr_lags as (
            select
            	parent_station
            	,service_date
                ,lag(total, 1, 0) over two_days as prev_total
            	,total - prev_total as delta
            from lone_arrival_daily_stats
            where parent_station = 'place-esomr'
            window two_days as (order by service_date rows between 1 preceding and current row)
        )
        ,mdftf_lags as (
            select
                parent_station
                ,service_date
                ,lag(total, 1, 0) over two_days as prev_total
            	,total - prev_total as delta
            from lone_arrival_daily_stats
            where parent_station = 'place-mdftf'
            window two_days as (order by service_date rows between 1 preceding and current row)
        )
        ,lags as (
            from esomr_lags union from mdftf_lags
        )
        select
            s.service_date
            ,s.parent_station
            ,s.total
            ,l.prev_total
            ,l.delta
        from lone_arrival_daily_stats s
        join lags l
        	on s.parent_station = l.parent_station
            and s.service_date = l.service_date
        order by s.service_date, s.parent_station;
        """,
        output=False
    )
    return (deltas_result,)


@app.cell
def _(deltas_result):
    # Group results by station
    # deltas = deltas_result.sort(["parent_station", "service_date"])

    # Keep results collated
    deltas = deltas_result
    return (deltas,)


@app.cell
def _(mo):
    mo.md(
        r"""
    /// admonition | Note
    The following 2 tables may contain results only when the selected date range spans 2 or more service dates.
    ///

    ### Dates with large **increases** in lone arrival events
    """
    )
    return


@app.cell
def _(deltas, pl):
    large_increases = deltas.filter(pl.col('delta') > 500)

    large_increases
    return (large_increases,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Dates with large **decreases** in lone arrival events""")
    return


@app.cell
def _(deltas, pl):
    large_decreases = deltas.filter(pl.col('delta') < -500)

    large_decreases
    return (large_decreases,)


@app.cell
def _(large_decreases, large_increases):
    def int_to_datestamp(service_date):
        s = str(service_date)  
        return f"{s[0:4]}-{s[4:6]}-{s[6:]}"

    print("increases:", ", ".join([int_to_datestamp(n) for n in large_increases["service_date"]]))
    print("decreases:", ", ".join([int_to_datestamp(n) for n in large_decreases["service_date"]]))
    return (int_to_datestamp,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Daily lone arrival event counts by station""")
    return


@app.cell
def _(date, deltas, int_to_datestamp, lone_arrival_daily_stats, pl):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12.0, 4.8))

    def plot_daily_stats(df, color):
        ax.plot(df['service_date'].map_elements(lambda d: date.fromisoformat(int_to_datestamp(d))), df['total'], color=color)

    def plot_derivatives(df, color):
        ax.plot(df['service_date'].map_elements(lambda d: date.fromisoformat(int_to_datestamp(d))), df['delta'], color=color)

    esomr_stats = lone_arrival_daily_stats.filter(pl.col('parent_station') == 'place-esomr')
    mdftf_stats = lone_arrival_daily_stats.filter(pl.col('parent_station') == 'place-mdftf')

    esomr_deltas = deltas.filter(pl.col('parent_station') == 'place-esomr')
    mdftf_deltas = deltas.filter(pl.col('parent_station') == 'place-mdftf')

    if mdftf_stats.height > 0:
        plot_daily_stats(mdftf_stats, 'tab:red')
    # plot_derivatives(mdftf_deltas, 'tab:orange')

    if esomr_stats.height > 0:
        plot_daily_stats(esomr_stats, 'tab:blue')
    # plot_derivatives(esomr_deltas, 'tab:purple')

    ax.set_ylim(bottom=0)
    fig
    return


@app.cell
def _(mo):
    mo.md(
        f"""
    /// details | Notes
    ## Lone arrivals existed at East Somerville before date given in report

    Before 5/28, East Somerville consistently had around 15-20 lone arrivals per service day, as far back as 1/1. (And presumably further; I stopped checking after that.)

    The number of lone arrivals at East Somerville did explode from 5/28 onward, similarly to Medford/Tufts, though. 5/28 has over 2k lone arrivals at East Somerville.

    ## Lone arrivals continue through the present

    For 10/16, there were 287 lone arrivals at Medford/Tufts and 101 at East Somerville.

    Compare with the spikes around 5/28 and 7/8, which were in the thousands.

    Prior to 5/28, there were 0-2 lone arrivals per service day at Medford/Tufts and 15-20 at East Somerville.
    ///

    /// details | Follow-up

    Split arrivals by stop and see what the vehicle w/ lone arrival was doing afterward at East Somerville. (Using rtr logs)

    For East Somerville, cause may be vehicles moving around in the yard.

    If it's this ^ then we probably need to ask OCS to modify some geofences.

    Plot GPS locations of pings when these lone arrival events happen, to get a better sense of whether it's a geofence issue.
    ^ How to get these? Ask in #tid-glides! There might be a hidden Glides admin view that lets us see this

    Also worth doing the same investigation for Medford/Tufts

    ---

    Also try to get a sense of whether rtr is making added trips in these cases. And why, if it turns out to be the case.

    Sample splunk query for when rtr creates added trips for glides
    https://mbta.splunkcloud.com/en-US/app/search/search?q=search%20index%3D%22rtr-prod%22%20created_added_gtfs_trip_for_glides&display.page.search.mode=verbose&dispatch.sample_ratio=1&workload_pool=&earliest=1759118400.000&latest=1759204800&display.page.search.tab=events&display.general.type=events&display.visualizations.charting.chart=line&display.visualizations.charting.chart.overlayFields=&sid=1762200862.130700

    ---

    One potential cause is that we started incorporating events for vehicles with special AVI codes that previously caused them to be ignored - now we treat them as nonrevenue trips
    https://github.com/mbta/rtr/blob/8730efd1e125a6d820c959b27f45f0c9eabdf3ff/lib/rtr/patterns/pattern.ex#L89
    https://github.com/mbta/rtr/pull/1440
    https://github.com/mbta/rtr/pull/1440/files#diff-5f1d544cf92ad1905c4ebbfac3522862dbde12e01c5649dadd3f12f7ea363530L95

    ---

    Are OpsAnalytics filtering between rev and nonrev predictions? There's no mention of it in their report, they might need to start filtering.
    Do some investigation as to how many lone arrival events are for nonrev trips

    ---

    You can download all results from a splunk query and then pull those logs into duckdb for querying
    ///
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    /// details | A rough annotated timeline

    These are dates where lone arrival count at a station changed significantly from that of the previous day.

    Groups of consecutive dates, and instances where both stations had a large change on the same day, are separated by blank lines.



    ```
    service_date,parent_station,total,prev_total,delta
    DEPLOY - https://github.com/mbta/rtr/compare/873cc140..3a6ed6ac
    UP
    2025-05-28,place-esomr,2146,16,2130
    2025-05-28,place-mdftf,660,0,660

    DOWN
    2025-05-29,place-esomr,1376,2146,-770
    2025-05-29,place-mdftf,151,660,-509

    UP
    2025-05-30,place-esomr,2208,1376,832

    DOWN
    2025-05-31,place-esomr,1340,2208,-868
    2025-06-01,place-esomr,91,1340,-1249

    DEPLOY Jun 1 - https://github.com/mbta/rtr/compare/3a6ed6ac..40285c5a
    DEPLOY Jun 2 - https://github.com/mbta/rtr/compare/40285c5a..2b73bb53
    UP
    2025-06-01,place-mdftf,730,172,558
    2025-06-02,place-esomr,1137,91,1046

    DEPLOY - linked above
    DOWN
    2025-06-02,place-mdftf,24,730,-706

    DEPLOY - https://github.com/mbta/rtr/compare/2b73bb53..d8961935
    DEPLOY - https://github.com/mbta/rtr/compare/d8961935..93ead9e9
    UP
    2025-06-04,place-esomr,2803,1604,1199
    2025-06-04,place-mdftf,5293,42,5251

    DEPLOY - https://github.com/mbta/rtr/compare/93ead9e9..e544eb75
    DOWN
    2025-06-05,place-esomr,26,2803,-2777
    2025-06-05,place-mdftf,24,5293,-5269

    DEPLOY 7/14 - https://github.com/mbta/rtr/compare/9c4de06f..460b4b68
    DEPLOY(rolled back) 7/15 - https://github.com/mbta/rtr/compare/460b4b68..83a56608
    UP
    2025-07-15,place-mdftf,777,266,511

    UP
    2025-08-04,place-mdftf,758,248,510

    DEPLOY - https://github.com/mbta/rtr/compare/aa5e9af3..b0a2886b
    DOWN
    2025-08-20,place-mdftf,122,753,-631

    DEPLOY - https://github.com/mbta/rtr/compare/5da64709..3bb06ad9
    UP
    2025-09-03,place-mdftf,1480,92,1388

    UP
    2025-09-08,place-mdftf,1854,1189,665

    DOWN
    2025-09-12,place-mdftf,329,1438,-1109

    UP
    2025-10-03,place-mdftf,1008,466,542

    DOWN
    2025-10-06,place-mdftf,279,1008,-729

    UP
    2025-10-25,place-mdftf,1397,650,747

    DOWN
    2025-10-30,place-mdftf,1058,1582,-524
    ```
    ///
    """
    )
    return


if __name__ == "__main__":
    app.run()
