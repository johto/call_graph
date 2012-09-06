/* call_graph/call_graph--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION call_graph" to load this file. \quit

CREATE FUNCTION call_graph_version() RETURNS text AS $$ SELECT text '1.0'; $$ LANGUAGE sql;

CREATE SEQUENCE seqCallGraphBuffer;
CREATE UNLOGGED TABLE CallGraphBuffer(
CallGraphBufferID bigint NOT NULL,
TopLevelFunction oid NOT NULL CHECK (TopLevelFunction <> '0'),
Caller oid NOT NULL,
Callee oid NOT NULL,
Calls bigint NOT NULL,
TotalTime double precision NOT NULL,
SelfTime double precision NOT NULL,
Datestamp timestamptz NOT NULL default now()
);

-- make sure all users are allowed to track data
GRANT USAGE ON SEQUENCE seqCallGraphBuffer TO PUBLIC;
GRANT INSERT ON TABLE CallGraphBuffer TO PUBLIC;

CREATE INDEX CallGraphBuffer_CallGraphBufferID_Index ON CallGraphBuffer(CallGraphBufferID);

CREATE TABLE CallGraphs(
CallGraphID bigserial NOT NULL,
TopLevelFunction oid NOT NULL,
EdgesHash bytea NOT NULL,
Calls bigint NOT NULL,
TotalTime double precision NOT NULL,
SelfTime double precision NOT NULL,
FirstCall timestamptz NOT NULL,
LastCall timestamptz NOT NULL,
PRIMARY KEY (CallGraphID),
UNIQUE (TopLevelFunction, EdgesHash)
);

CREATE TABLE Edges(
EdgeID bigserial NOT NULL,
CallGraphID int NOT NULL REFERENCES CallGraphs(CallGraphID),
Caller Oid NOT NULL,
Callee Oid NOT NULL,
Calls bigint NOT NULL,
TotalTime double precision NOT NULL,
SelfTime double precision NOT NULL,
PRIMARY KEY (EdgeID),
UNIQUE (CallGraphID, Caller, Callee)
);

CREATE OR REPLACE FUNCTION ProcessCallGraphBuffers()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
_CallGraphID bigint;
_GraphExists bool;
_NumGraphs int;
_ record;

BEGIN

-- Make sure we don't generate false information
SET LOCAL call_graph.enable TO FALSE;

_NumGraphs := 0;

-- The first thing we need to do is to identify the callgraph each CallGraphBufferID represents.  We currently do this by
-- calculating the SHA1 hash of the binary representation of an array that contains ROW(caller, callee) values ordered by
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
-- way we can do the processing a callgraph at a time, rather than a CallGraphBufferID at a time.

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
        digest(array_send(array_agg(row(caller, callee) ORDER BY caller, callee)), 'sha1') AS EdgesHash,
        MAX(CASE WHEN Caller = 0 THEN Calls     END) AS Calls,
        MAX(CASE WHEN Caller = 0 THEN TotalTime END) AS TotalTime,
        MAX(CASE WHEN Caller = 0 THEN SelfTime  END) AS SelfTime,
        MAX(Datestamp) AS CallStamp
    FROM CallGraphBuffer
    GROUP BY CallGraphBufferID, TopLevelFunction
) AS CallGraphBufferGrouped
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
    -- call graph, so we can safely aggregate the data to avoid doing multiple UPDATEs.

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
    _NumGraphs := _NumGraphs + 1;
END LOOP;

RETURN _NumGraphs;

END;
$function$
;
