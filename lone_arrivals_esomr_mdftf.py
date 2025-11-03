import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    from datetime import date, timedelta
    import polars as pl
    return date, mo, pl, timedelta


@app.cell
def _(date):
    # OpsAnalytics report gives start date as 5/28, this samples an extra week of data before it.
    START_DATE = date.fromisoformat("2025-05-21")
    END_DATE = date.fromisoformat("2025-10-31")
    STOPS = ["place-esomr", "place-mdftf"]
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
        """
    )
    return all_arrivals, lone_arrivals


@app.cell
def _(END_DATE, START_DATE, STOPS, all_arrivals, date, mo, timedelta):
    def load_service_date_query(d: date):
        return f"""
        insert into all_arrivals
            from 'https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{d}-subway-on-time-performance-v1.parquet'
            where parent_station in {STOPS};
        """

    def service_dates(start_date, end_date):
        d = start_date
        while d != end_date:
            yield d
            d = d + timedelta(days=1)
        yield d

    mo.sql("truncate all_arrivals;")

    for d in service_dates(START_DATE, END_DATE):
        mo.sql(load_service_date_query(d))
    return


@app.cell
def _(all_arrivals, lone_arrivals, mo):
    lone_arrival_daily_stats = mo.sql(
        f"""
        create table lone_arrival_daily_stats as
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


        select * from lone_arrival_daily_stats;
        """
    )
    return (lone_arrival_daily_stats,)


@app.cell
def _(lone_arrival_daily_stats, mo):
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
        """
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
def _(deltas, pl):
    large_increases = deltas.filter(pl.col('delta') > 500)

    large_increases
    return (large_increases,)


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


@app.cell
def _(date, deltas, int_to_datestamp, lone_arrival_daily_stats, pl):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()

    def plot_daily_stats(df, color):
        ax.plot(df['service_date'].map_elements(lambda d: date.fromisoformat(int_to_datestamp(d))), df['total'], color=color)

    def plot_derivatives(df, color):
        ax.plot(df['service_date'].map_elements(lambda d: date.fromisoformat(int_to_datestamp(d))), df['delta'], color=color)

    esomr_stats = lone_arrival_daily_stats.filter(pl.col('parent_station') == 'place-esomr')
    mdftf_stats = lone_arrival_daily_stats.filter(pl.col('parent_station') == 'place-mdftf')

    esomr_deltas = deltas.filter(pl.col('parent_station') == 'place-esomr')
    mdftf_deltas = deltas.filter(pl.col('parent_station') == 'place-mdftf')

    plot_daily_stats(mdftf_stats, 'tab:red')
    # plot_derivatives(mdftf_deltas, 'tab:orange')

    plot_daily_stats(esomr_stats, 'tab:blue')
    # plot_derivatives(esomr_deltas, 'tab:purple')

    plt.show()
    return


@app.cell
def _(mo):
    mo.md(
        f"""
    # Notes

    ## Lone arrivals existed at East Somerville before date given in report

    Before 5/28, East Somerville consistently had around 15-20 lone arrivals per service day, as far back as 1/1. (And presumably further; I stopped checking after that.)

    The number of lone arrivals at East Somerville did explode from 5/28 onward, similarly to Medford/Tufts, though. 5/28 has over 2k lone arrivals at East Somerville.

    ## Lone arrivals continue through the present

    For 10/16, there were 287 lone arrivals at Medford/Tufts and 101 at East Somerville.

    Compare with the spikes around 5/28 and 7/8, which were in the thousands.

    Prior to 5/28, there were 0-2 lone arrivals per service day at Medford/Tufts and 15-20 at East Somerville.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""

    # A rough annotated timeline

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
    """
    )
    return


if __name__ == "__main__":
    app.run()
