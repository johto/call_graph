/* call_graph/call_graph--2.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION call_graph" to load this file. \quit

CREATE USER call_graph;
ALTER SCHEMA call_graph OWNER TO call_graph;

SET ROLE call_graph;

CREATE TABLE Functions (
	FunctionID serial NOT NULL,
	Nspname text NOT NULL,
	Signature text NOT NULL,

	UNIQUE (Nspname, Signature),
	PRIMARY KEY (FunctionID)
);

CREATE SEQUENCE seqCallGraphBuffer;
CREATE UNLOGGED TABLE CallGraphBuffer (
	CallGraphBufferID bigint NOT NULL,
	CallerNspname text,
	CallerSignature text,
	CalleeNspname text NOT NULL,
	CalleeSignature text NOT NULL,
	Calls bigint NOT NULL,
	TotalTime double precision NOT NULL,
	SelfTime double precision NOT NULL,
	Datestamp timestamptz NOT NULL DEFAULT now()
);

CREATE UNLOGGED TABLE CallGraphBufferMeta (
	CallGraphBufferID bigint NOT NULL,
	-- top level function info
	Nspname text NOT NULL,
	Signature text NOT NULL,
	-- caller info
	CallerRolname text NOT NULL
);

--CREATE INDEX CallGraphBuffer_CallGraphBufferID_TopLevelFunction_Index ON CallGraphBuffer(CallGraphBufferID, TopLevelFunction);

CREATE TABLE CallGraphs (
	CallGraphID bigserial NOT NULL,
	TopLevelFunction int NOT NULL REFERENCES Functions(FunctionID),
	EdgesHash text NOT NULL,
	Calls bigint NOT NULL,
	TotalTime double precision NOT NULL,
	SelfTime double precision NOT NULL,
	FirstCall timestamptz NOT NULL,
	LastCall timestamptz NOT NULL,
	PRIMARY KEY (CallGraphID),
	UNIQUE (TopLevelFunction, EdgesHash)
);

CREATE TABLE Edges (
	EdgeID bigserial NOT NULL,
	CallGraphID int NOT NULL REFERENCES CallGraphs(CallGraphID),
	Caller int NOT NULL REFERENCES Functions(FunctionID),
	Callee int NOT NULL REFERENCES Functions(FunctionID),
	Calls bigint NOT NULL,
	TotalTime double precision NOT NULL,
	SelfTime double precision NOT NULL,
	PRIMARY KEY (EdgeID),
	UNIQUE (CallGraphID, Caller, Callee)
);

CREATE TABLE DailyStats (
	CallGraphID bigserial NOT NULL,
	Date date NOT NULL,
	Calls bigint NOT NULL,
	TotalTime double precision NOT NULL,
	SelfTime double precision NOT NULL,
	FirstCall timestamptz NOT NULL,
	LastCall timestamptz NOT NULL,
	PRIMARY KEY (CallGraphID, Date)
);

CREATE TABLE HourlyStats (
	CallGraphID bigserial NOT NULL,
	Datestamp timestamptz NOT NULL CHECK (date_trunc('hour', Datestamp) = Datestamp),
	Calls bigint NOT NULL,
	TotalTime double precision NOT NULL,
	SelfTime double precision NOT NULL,
	FirstCall timestamptz NOT NULL,
	LastCall timestamptz NOT NULL,
	PRIMARY KEY (CallGraphID, Datestamp)
);

CREATE FUNCTION UpsertFunction(_Nspname text, _Signature text)
 RETURNS int
 LANGUAGE sql
AS $function$
-- Assumes exclusive access
WITH Sel AS (
	SELECT FunctionID
	FROM call_graph.Functions
	WHERE Nspname = $1 AND Signature = $2
), Ins AS (
	INSERT INTO call_graph.Functions (Nspname, Signature)
	SELECT $1, $2
	WHERE NOT EXISTS (SELECT 1 FROM Sel)
	RETURNING FunctionID
)
SELECT FunctionID FROM Sel
	UNION
SELECT FunctionID FROM Ins
;
$function$
;

CREATE FUNCTION UpsertCallGraphBuffer(OUT TopLevelFunction int, OUT Caller int, OUT Callee int, _Buffer call_graph.CallGraphBuffer)
 RETURNS RECORD
 SECURITY DEFINER
 LANGUAGE plpgsql
AS $function$
BEGIN

SELECT UpsertFunction(Nspname, Signature) INTO TopLevelFunction
	FROM CallGraphBufferMeta
	WHERE CallGraphBufferMeta.CallGraphBufferID = (_Buffer).CallGraphBufferID;
IF NOT FOUND THEN
	RAISE EXCEPTION 'could not find meta information for CallGraphBufferID %', (_Buffer).CallGraphBufferID;
END IF;

Caller := UpsertFunction((_Buffer).CallerNspname, (_Buffer).CallerSignature);
Callee := UpsertFunction((_Buffer).CalleeNspname, (_Buffer).CalleeSignature);

END
$function$
;


CREATE FUNCTION ProcessCallGraphBuffers(_MaxBufferCount bigint)
 RETURNS SETOF bigint
 SECURITY DEFINER
 LANGUAGE plpgsql
 SET search_path TO call_graph
AS $function$
DECLARE
_MinBufferID bigint;
_CallGraphID bigint;
_GraphExists bool;
_NumGraphs int;
_ record;
BEGIN

_NumGraphs := 0;

-- The first thing we need to do is to identify the callgraph each
-- CallGraphBufferID represents.  We currently do this by calculating the MD5
-- hash of the binary representation of an array that contains ROW(caller,
-- callee) values ordered by (caller, callee).  This hash can then be used to
-- uniquely identify each call graph easily and efficiently.
--
-- In the below query, the subquery aggregates the data for each
-- CallGraphBufferID, calculating the hash representation of the edges.  At the
-- same time, it pulls some additional data from the row where Caller is NULL
-- (there should only ever be one such row per CallGraphBufferID, so the choice
-- of aggregate function shouldn't matter; I chose max) which is then stored in
-- the CallGraphs table.  Also note that we are grouping by (CallGraphBufferID,
-- TopLevelFunction), which is effectively the same as grouping by only
-- CallGraphBufferID; there should NEVER be more than one TopLevelFunction for
-- a CallGraphBufferID.
--
-- After the subquery is done, we aggregate the data again, this time for each
-- (TopLevelFunction, EdgesHash) pair.  This way we can do the processing one
-- callgraph at a time, rather than a CallGraphBufferID at a time.

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
        MAX(CASE WHEN Caller IS NULL THEN Calls     END) AS Calls,
        MAX(CASE WHEN Caller IS NULL THEN TotalTime END) AS TotalTime,
        MAX(CASE WHEN Caller IS NULL THEN SelfTime  END) AS SelfTime,
        MAX(Datestamp) AS CallStamp
    FROM
	(
		SELECT
			CallGraphBufferID, (Upsert).TopLevelFunction,
			(Upsert).Caller, (Upsert).Callee,
			Calls, TotalTime, SelfTime, Datestamp
		FROM
		(
			SELECT
				CallGraphBufferID, Upsert_CallGraphBuffer(CallGraphBuffer) AS Upsert,
				TotalTime, SelfTime, Datestamp
			FROM CallGraphBuffer
			WHERE
				CallGraphBufferID >= _MinBufferID AND
				CallGraphBufferID <= _MinBufferID + _MaxBufferCount
			OFFSET 0
		) CallGraphBuffer
	) AS CallGraphBuffer
    GROUP BY CallGraphBufferID, TopLevelFunction
) AS GroupedBuffers
GROUP BY TopLevelFunction, EdgesHash
LOOP
    UPDATE CallGraphs SET
        Calls     = Calls     + _.Calls,
        TotalTime = TotalTime + _.TotalTime,
        SelfTime  = SelfTime  + _.SelfTime,
		FirstCall = LEAST(FirstCall, _.FirstCall),
        LastCall  = GREATEST(LastCall, _.LatestCall)
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

    -- If the graph existed, all of the edges should exist too, and we can
	-- simply UPDATE them.  If it didn't, we need to add the edges.
    --
    -- Note that although we're doing multiple CallGraphBufferIDs at a time,
	-- we're only working on a single call graph, so we can safely aggregate
	-- the data in CallGraphBufferSum to avoid doing multiple UPDATEs.

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
			relid, seq_scan, seq_tup_read, idx_scan, idx_tup_read,
				   n_tup_ins, n_tup_upd, n_tup_del
	),
	GroupedBuffers AS (
		SELECT
			relid, sum(seq_scan) AS seq_scan, sum(seq_tup_read) AS seq_tup_read,
				   sum(idx_scan) AS idx_scan, sum(idx_tup_read) AS idx_tup_read,
				   sum(n_tup_ins) AS n_tup_ins, sum(n_tup_upd) AS n_tup_upd,
				   sum(n_tup_del) AS n_tup_del
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
			idx_tup_read = tu.idx_tup_read + buf.idx_tup_read,
			n_tup_ins = tu.n_tup_ins + buf.n_tup_ins,
			n_tup_upd = tu.n_tup_upd + buf.n_tup_upd,
			n_tup_del = tu.n_tup_del + buf.n_tup_del
		FROM
			GroupedBuffers buf
		WHERE
			tu.CallGraphID = _CallGraphID AND
			tu.relid = buf.relid
	)
	INSERT INTO
		TableUsage (CallGraphID, relid, seq_scan, seq_tup_read, idx_scan, idx_tup_read,
										n_tup_ins, n_tup_upd, n_tup_del)
	SELECT
		_CallGraphID, relid, seq_scan, seq_tup_read, idx_scan, idx_tup_read,
							 n_tup_ins, n_tup_upd, n_tup_del
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

RESET ROLE;
