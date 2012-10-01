/* call_graph/call_graph--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION UPDATE" to load this file. \quit

CREATE OR REPLACE FUNCTION call_graph_version() RETURNS text AS $$ SELECT text '1.3'; $$ LANGUAGE sql;

ALTER TABLE TableUsage ADD PRIMARY KEY (CallGraphID, relid);

-- first, add the columns with DEFAULT 0 so the existing rows' columns all get
-- assigned to 0
ALTER TABLE TableUsage ADD COLUMN seq_scan bigint NOT NULL DEFAULT 0;
ALTER TABLE TableUsage ADD COLUMN seq_tup_read bigint NOT NULL DEFAULT 0;
ALTER TABLE TableUsage ADD COLUMN idx_scan bigint NOT NULL DEFAULT 0;
ALTER TABLE TableUsage ADD COLUMN idx_tup_read bigint NOT NULL DEFAULT 0;

ALTER TABLE TableAccessBuffer ADD COLUMN seq_scan bigint NOT NULL DEFAULT 0;
ALTER TABLE TableAccessBuffer ADD COLUMN seq_tup_read bigint NOT NULL DEFAULT 0;
ALTER TABLE TableAccessBuffer ADD COLUMN idx_scan bigint NOT NULL DEFAULT 0;
ALTER TABLE TableAccessBuffer ADD COLUMN idx_tup_read bigint NOT NULL DEFAULT 0;

-- .. and then remove the default
ALTER TABLE TableUsage ALTER COLUMN seq_scan DROP DEFAULT;
ALTER TABLE TableUsage ALTER COLUMN seq_tup_read DROP DEFAULT;
ALTER TABLE TableUsage ALTER COLUMN idx_scan DROP DEFAULT;
ALTER TABLE TableUsage ALTER COLUMN idx_tup_read DROP DEFAULT;

ALTER TABLE TableAccessBuffer ALTER COLUMN seq_scan DROP DEFAULT;
ALTER TABLE TableAccessBuffer ALTER COLUMN seq_tup_read DROP DEFAULT;
ALTER TABLE TableAccessBuffer ALTER COLUMN idx_scan DROP DEFAULT;
ALTER TABLE TableAccessBuffer ALTER COLUMN idx_tup_read DROP DEFAULT;

-- replace ProcessCallGraphBuffers()
CREATE OR REPLACE FUNCTION call_graph.ProcessCallGraphBuffers(_MaxBufferCount bigint)
 RETURNS SETOF bigint
 LANGUAGE plpgsql
 SET search_path TO @extschema@
AS $function$
DECLARE
_MinBufferID bigint;
_CallGraphID bigint;
_GraphExists bool;
_NumGraphs int;
_ record;

BEGIN

_NumGraphs := 0;

-- The first thing we need to do is to identify the callgraph each CallGraphBufferID represents.  We currently do this by
-- calculating the MD5 hash of the binary representation of an array that contains ROW(caller, callee) values ordered by
-- (caller, callee).  This hash can then be used to uniquely identify each call graph easily and efficiently.
--
-- In the below query, the subquery aggregates the data for each CallGraphBufferID, calculating the hash representation of
-- the edges.  At the same time, it pulls some additional data from the row where Caller = 0 (there should only ever be one
-- such row per CallGraphBufferID, so the choice of aggregate function shouldn't matter; I chose max) which is then stored
-- in the CallGraphs table.  Also note that we are grouping by (CallGraphBufferID, TopLevelFunction), which is effectively
-- the same as grouping by only CallGraphBufferID; there should NEVER be more than one TopLevelFunction for a
-- CallGraphBufferID.
--
-- After the subquery is done, we aggregate the data again, this time for each (TopLevelFunction, EdgesHash) pair.  This
-- way we can do the processing one callgraph at a time, rather than a CallGraphBufferID at a time.

_MinBufferID = (SELECT min(CallGraphBufferID) FROM call_graph.CallGraphBuffer);

FOR _ IN
SELECT
    TopLevelFunction,
    EdgesHash,
    array_agg(CallGraphBufferID) AS CallGraphBufferIDs,
    SUM(Calls)     AS Calls,
    SUM(TotalTime) AS TotalTime,
    SUM(SelfTime)  AS SelfTime,
    MIN(CallStamp) AS FirstCall,
    MAX(CallStamp) AS LatestCall
FROM (
    SELECT
        CallGraphBufferID,
        TopLevelFunction,
        md5(array_send(array_agg(ROW(Caller, Callee) ORDER BY Caller, Callee))) AS EdgesHash,
        MAX(CASE WHEN Caller = 0 THEN Calls     END) AS Calls,
        MAX(CASE WHEN Caller = 0 THEN TotalTime END) AS TotalTime,
        MAX(CASE WHEN Caller = 0 THEN SelfTime  END) AS SelfTime,
        MAX(Datestamp) AS CallStamp
    FROM
	(
		SELECT
			CallGraphBufferID, TopLevelFunction, Caller, Callee, Calls, TotalTime, SelfTime, DateStamp
		FROM
			CallGraphBuffer
		WHERE
			CallGraphBufferID >= _MinBufferID AND CallGraphBufferID <= _MinBufferID + _MaxBufferCount
	) AS Buffers
    GROUP BY CallGraphBufferID, TopLevelFunction
) AS GroupedBuffers
GROUP BY TopLevelFunction, EdgesHash
LOOP
    UPDATE CallGraphs SET
        Calls     = Calls     + _.Calls,
        TotalTime = TotalTime + _.TotalTime,
        SelfTime  = SelfTime  + _.SelfTime,
        LastCall  = _.LatestCall
    WHERE TopLevelFunction = _.TopLevelFunction
    AND EdgesHash          = _.EdgesHash
    RETURNING CallGraphID INTO _CallGraphID;
    IF FOUND THEN
        _GraphExists := TRUE;
    ELSE
        INSERT INTO CallGraphs (TopLevelFunction, EdgesHash, Calls, TotalTime, SelfTime, FirstCall, LastCall)
        VALUES (_.TopLevelFunction, _.EdgesHash, _.Calls, _.TotalTime, _.SelfTime, _.FirstCall, _.LatestCall)
        RETURNING CallGraphID INTO _CallGraphID;
        _GraphExists := FALSE;
    END IF;

    -- If the graph existed, all of the edges should exist too, and we can simply UPDATE them.  If it didn't,
    -- we need to add the edges.
    --
    -- Note that although we're doing multiple CallGraphBufferIDs at a time, we're only working on a single
    -- call graph, so we can safely aggregate the data in CallGraphBufferSum to avoid doing multiple UPDATEs.

    IF _GraphExists THEN
        UPDATE Edges SET
            Calls     = Edges.Calls     + CallGraphBufferSum.Calls,
            TotalTime = Edges.TotalTime + CallGraphBufferSum.TotalTime,
            SelfTime  = Edges.SelfTime  + CallGraphBufferSum.SelfTime
        FROM (
            SELECT
                Caller,
                Callee,
                SUM(Calls)     AS Calls,
                SUM(TotalTime) AS TotalTime,
                SUM(SelfTime)  AS SelfTime
			FROM CallGraphBuffer
            WHERE CallGraphBufferID = ANY(_.CallGraphBufferIDs)
            GROUP BY Caller, Callee
        ) AS CallGraphBufferSum
        WHERE Edges.CallGraphID = _CallGraphID
        AND Edges.Caller = CallGraphBufferSum.Caller
        AND Edges.Callee = CallGraphBufferSum.Callee;
    ELSE
            INSERT INTO Edges (CallGraphID, Caller, Callee, Calls, TotalTime, SelfTime)
            SELECT _CallGraphID, Caller, Callee, SUM(Calls), SUM(TotalTime), SUM(SelfTime)
            FROM CallGraphBuffer
            WHERE CallGraphBufferID = ANY(_.CallGraphBufferIDs)
            GROUP BY Caller, Callee;
    END IF;

    DELETE FROM CallGraphBuffer WHERE CallGraphBufferID = ANY(_.CallGraphBufferIDs);

	WITH Buffers AS (
		DELETE FROM
			TableAccessBuffer
		WHERE
			CallGraphBufferID = ANY(_.CallGraphBufferIDs)
		RETURNING
			relid, seq_scan, seq_tup_read, idx_scan, idx_tup_read
	),
	GroupedBuffers AS (
		SELECT
			relid, sum(seq_scan) AS seq_scan, sum(seq_tup_read) AS seq_tup_read,
				   sum(idx_scan) AS idx_scan, sum(idx_tup_read) AS idx_tup_read
		FROM
			Buffers
		GROUP BY
			relid
	),
	UpdateExisting AS (
		UPDATE
			TableUsage tu
		SET
			seq_scan = tu.seq_scan + buf.seq_scan,
			seq_tup_read = tu.seq_tup_read + buf.seq_tup_read,
			idx_scan = tu.idx_scan + buf.idx_scan,
			idx_tup_read = tu.idx_tup_read + buf.idx_tup_read
		FROM
			GroupedBuffers buf
		WHERE
			tu.CallGraphID = _CallGraphID AND
			tu.relid = buf.relid
	)
	INSERT INTO
		TableUsage (CallGraphID, relid, seq_scan, seq_tup_read, idx_scan, idx_tup_read)
	SELECT
		_CallGraphID, relid, seq_scan, seq_tup_read, idx_scan, idx_tup_Read
	FROM
		GroupedBuffers buf
	WHERE
		NOT EXISTS
		(SELECT * FROM TableUsage tu
		 WHERE tu.CallGraphID = _CallGraphID AND tu.relid = buf.relid);

	RETURN NEXT _CallGraphID;
END LOOP;

RETURN;

END;
$function$
;
