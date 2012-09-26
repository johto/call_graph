#!/usr/bin/perl

use strict;
use warnings;
use DBI;
use DBD::Pg;

# Debug the dot file format (writes .dot files in the graphs/ directory instead
# of rendering graphs).
my $dot_debug = 0;


# XXX At some point it might make sense to use a real parser (Config::IniFiles
# for example), but right now this works just as well and avoids introducing
# new dependencies.
sub parse_config_file
{
	my ($filename, $params) = @_;

	my $parser = q{^\s*($|(;.*)|((\w+)\s*=([^;]+)(;.*)?))\s*$};

	open(CONFFILE, $filename) or die("Could not open config file $filename");
	while (my $line = <CONFFILE>)
	{
		chomp($line);
		die("Syntax error on line $. in $filename\n") if ($line !~ $parser);

		next if (!defined $4);
		
		$params->{$4} = $5;
	}

	# The user can specify a list of functions which are then separated from the
	# actual graph they would otherwise be in, creating separate subgraphs.
	if (defined $params->{SubGraphs})
	{
		my @subgraph_list = split(",", $params->{SubGraphs});
		# trim() the elements
		my @trimmed = map { local $_ = $_; s/^\s+|\s+$//g; $_ } @subgraph_list;
		$params->{SubGraphs} = \@trimmed;
	}
	else
	{
		$params->{SubGraphs} = [];
	}

	return $params;
}


if (@ARGV != 3)
{
	print "Usage: ./create_graphs.pl graphdir configfile dbname\n";
	exit;
}

my $graphdir = $ARGV[0];
my $config_file = $ARGV[1];
my $dbname = $ARGV[2];
my $htmlfile = $graphdir."/index.html";

# default params
my $params =
{
	OidLookupTable		=>		"\"pg_proc\"",

	EdgeColor			=>		"'black'",
	EdgeStyle			=>		"'solid'",
	EdgePenWidth		=>		1.0,

	NodeLabel			=>		"FunctionName",
	NodeShape			=>		"'ellipse'",
	NodeHref			=>		"NULL",
	NodeColor			=>		"'black'",
	NodeStyle			=>		"'solid'",
	NodePenWidth		=>		1.0
};

$params = parse_config_file($config_file, $params);

my $sqlquery = 
<<"SQL";
WITH RECURSIVE
SubGraphParams (TopLevelFunction, Callee, SubGraphID) AS (
	-- XXX This is a bit ugly.  Maybe we should just take (oid,oid) pairs as parameters?

	SELECT
		-- There might be multiple functions with the same name, so simply pick the one
		-- which has the smaller oid.
		min(tlf.oid), min(ef.oid), row_number() OVER () AS SubGraphID
	FROM
	(
		SELECT
			split_part(val, '.', 1),
			split_part(val, '.', 2)
		FROM
			unnest(\$1::text[]) SubGraphInput (val)
	) SubGraphs (TopLevelFunction, EntryFunction)
	JOIN
		$params->{OidLookupTable} tlf
			ON (tlf.proname = SubGraphs.TopLevelFunction)
	JOIN
		$params->{OidLookupTable} ef
			ON (ef.proname = SubGraphs.EntryFunction)
	GROUP BY tlf.proname, ef.proname
),
SubGraphEdgeWorkTable (EdgeID, CallGraphID, SubGraphID, Callee, SeenEdges, ShouldStop) AS (
	-- Recursively make a list of edges that are part of any subgraphs.  We need to keep a list of
	-- edges we've already visited to avoid looping infinitely in case there are loops in the call
	-- graphs.
	--
	-- Also we need to make sure that we don't claim that an edge is part of two different
	-- subgraphs; that's handled using ShouldStop.

	SELECT
		EdgeID, Edges.CallGraphID, SubGraphParams.SubGraphID, Edges.Callee, ARRAY[EdgeID],
		(CallGraphs.TopLevelFunction, Edges.Callee) IN (SELECT TopLevelFunction, Callee FROM SubGraphParams)
	FROM
		call_graph.Edges
	JOIN
		call_graph.CallGraphs
			ON (CallGraphs.CallGraphID = Edges.CallGraphID)
	JOIN
		SubGraphParams ON (SubGraphParams.TopLevelFunction = CallGraphs.TopLevelFunction AND SubGraphParams.Callee = Edges.Caller)
	WHERE
		(CallGraphs.TopLevelFunction, Edges.Caller) IN (SELECT TopLevelFunction, Callee FROM SubGraphParams)

	UNION ALL

	SELECT
		Edges.EdgeID, Edges.CallGraphID, SubGraphEdgeWorkTable.SubGraphID, Edges.Callee, SeenEdges || Edges.EdgeID,
		-- stop traversing this path if we hit an edge belonging to another subgraph
		(CallGraphs.TopLevelFunction, Edges.Callee) IN (SELECT TopLevelFunction, Callee FROM SubGraphParams) OR
		-- make sure we don't loop forever if there are cycles
		Edges.EdgeID = ANY(SeenEdges)
	FROM
		SubGraphEdgeWorkTable
	JOIN
		call_graph.Edges
			ON (SubGraphEdgeWorkTable.Callee = Edges.Caller AND SubGraphEdgeWorkTable.CallGraphID = Edges.CallGraphID) 
	JOIN
		call_graph.CallGraphs
			ON (CallGraphs.CallGraphID = Edges.CallGraphID)
	WHERE
		NOT ShouldStop
),
Edges AS (
	-- Create a processed list of edges, marking which subgraph they are part of (if any)
	SELECT
		CASE WHEN SubGraphEdges.EdgeID IS NULL THEN 'e' ELSE 's' END AS EdgeType,
		COALESCE('s'||SubGraphEdges.SubGraphID, TopLevelFunction::text) AS GraphID,
		TopLevelFunction,
		Edges.EdgeID, Edges.CallGraphID, Caller, Callee, Edges.Calls, Edges.TotalTime, Edges.SelfTime
	FROM
		call_graph.Edges
	JOIN
		call_graph.CallGraphs USING (CallGraphID)
	LEFT JOIN
	(
		SELECT
			EdgeID, SubGraphID
		FROM
			SubGraphEdgeWorkTable
		GROUP BY
			EdgeID, SubGraphID
	) SubGraphEdges
		ON (SubGraphEdges.EdgeID = Edges.EdgeID)
)
SELECT
	GraphID,
	'edge'::text AS ElementType,
	EdgeFrom,
	EdgeTo,
	NULL::text AS NodeID,
	NULL::text AS NodeLabel,
	NULL::text AS NodeShape,
	NULL::text AS NodeHref,
	$params->{EdgeColor} AS Color,
	$params->{EdgeStyle} AS Style,
	$params->{EdgePenWidth} AS PenWidth
FROM
(
	SELECT
		GraphID AS GraphID,
		GraphID||'e'||caller AS EdgeFrom,
		GraphID||'e'||callee AS EdgeTo,
		EdgeNumCalls,
		max(EdgeNumCalls) OVER w AS MaxEdgeNumCalls,
		AvgSelfTime, TotalSelfTime,
		max(AvgSelfTime) OVER w AS MaxAvgSelfTime,
		max(TotalSelfTime) OVER w AS MaxTotalSelfTime,
		AvgTotalTime, TotalTotalTime,
		max(AvgTotalTime) OVER w AS MaxAvgTotalTime,
		max(TotalTotalTime) OVER w AS MaxTotalTotalTime,
		NumPresent,
		NumGraphs
	FROM
	(
		SELECT
			GraphID, Caller, Callee, sum(Edges.Calls) AS EdgeNumCalls,
			sum(Edges.SelfTime) / sum(Edges.Calls) AS AvgSelfTime, sum(Edges.SelfTime) AS TotalSelfTime,
			sum(Edges.TotalTime) / sum(Edges.Calls) AS AvgTotalTime, sum(Edges.TotalTime) AS TotalTotalTime,
			count(*) AS NumPresent, 
			(SELECT count(*) FROM call_graph.CallGraphs cg3 WHERE cg3.TopLevelFunction = Edges.TopLevelFunction) AS NumGraphs
		FROM
			Edges
		WHERE
			Caller <> 0
		GROUP BY
			GraphID, TopLevelFunction, caller, callee
	) ss

	WINDOW w AS (PARTITION BY GraphID)
) ss2
	
-- add labels for the nodes
UNION ALL
SELECT
	GraphID,
	'node' AS ElementType,
	NULL AS EdgeFrom,
	NULL AS EdgeTo,
	NodeID,
	$params->{NodeLabel} AS NodeLabel,
	$params->{NodeShape} AS NodeShape,
	$params->{NodeHref} AS NodeHref,
	$params->{NodeColor} AS Color,
	$params->{NodeStyle} AS Style,
	$params->{NodePenWidth} AS PenWidth
FROM
(
	SELECT
		GraphID,
		GraphID||'e'||Callee AS NodeID,
		proclookup.proname AS FunctionName,
		proclookup.oid AS FunctionOid,
		(TopLevelFunction, Callee) IN (SELECT TopLevelFunction, Callee FROM SubGraphParams) AS NodeIsSubGraphEntryFunction,
		NodeIsGraphEntryFunction
	FROM
	(
		SELECT
			GraphID::text,
			Callee,
			TopLevelFunction,
			Caller = 0 AS NodeIsGraphEntryFunction
		FROM
			Edges
		WHERE
			-- Don't display top level functions with no children.  We can skip this check for subgraphs
			-- since if there are any subgraph edges in the Edges CTE, we can assume them to be visible.
			EdgeType = 's' OR
			EXISTS (SELECT * FROM Edges e2 WHERE e2.caller = Edges.TopLevelFunction AND e2.GraphID = Edges.GraphID)
		
		UNION ALL

		-- also need labels for the entry points into subgraphs
		SELECT
			's'||SubGraphID AS GraphID,
			Callee,
			TopLevelFunction,
			TRUE AS NodeIsGraphEntryFunction
		FROM
			SubGraphParams
	) Edges
	JOIN
		$params->{OidLookupTable} proclookup
			ON (proclookup.oid = Edges.Callee)
	GROUP BY
		GraphID, TopLevelFunction, Callee, proclookup.proname, proclookup.oid, NodeIsGraphEntryFunction
) ss

ORDER BY
	GraphID

SQL
;


my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "", {RaiseError => 1, PrintError => 0});

my $sth = $dbh->prepare($sqlquery);
$sth->execute($params->{SubGraphs});

if ($sth->rows <= 0)
{
	# shouldn't happen
	die "the SQL query returned no rows -- maybe you forgot to run ProcessCallGraphBuffers()?";
}

my $graph = undef;

# Go through all the dot formatted lines one at a time.  Here we can assume that
# the result set is ordered by GraphID, so we can write an entire file at once.
#
# The loop below is a bit complex to avoid duplicating the code for opening and
# closing a file handle.
#

my $graphs = {};
while (1)
{
	my $row = $sth->fetchrow_hashref;

	# If the previous row was the last row in the result set or the last row of that
	# particular graph, close the pipe and make sure the "dot" command succeeded.
	if (!defined($row) ||
		(defined $graph && $graph ne $row->{graphid}))
	{
		print DOT "}\n";
		close(DOT);

		# Check the exit code of the program
		die('dot failed') if $? != 0;

		$graph = undef;

		# Also, if there are no more rows, we're done
		last if !defined $row;
	}

	# Start writing into a new graph if we're not currently writing to any
	if (!defined $graph)
	{
		$graph = $row->{graphid};
		if (defined $graphs->{$graph})
		{
			# Shouldn't happen unless someone changes this script
			die "graph list not ordered by GraphID";
		}

		$graphs->{$graph} = { size => 0, name => "graph $graph" };

		# If $dot_debug is set, write .dot files.  if not, pipe the output to dot
		if ($dot_debug)
		{
			my $filename = "$graphdir/$graph.dot";
			open(DOT, ">", $filename) or die "could not open file $filename";
		}
		else
		{
			open(DOT, "| dot -Tsvg -o $graphdir/$graph.svg") or die "could not fork";
		}

		print DOT "digraph graph1 {\n";
	}

	$graphs->{$graph}->{size}++;

	my $data;
	if ($row->{elementtype} eq 'edge')
	{
		$data = "edge[color = \"$row->{color}\", penwidth=$row->{penwidth}]; \"$row->{edgefrom}\" -> \"$row->{edgeto}\" [style=\"$row->{style}\"];";
	}
	elsif ($row->{elementtype} eq 'node')
	{
		my $optional_url = defined $row->{nodehref} ? "URL=\"$row->{nodehref}\"" : "";
		$data = "\"$row->{nodeid}\" [label=\"$row->{nodelabel}\", shape=$row->{nodeshape}, style=$row->{style} $optional_url];";
	}
	else
	{
		print "unknown element type $row->{elementtype}\n";
		exit;
	}

	print DOT $data."\n";
}

$dbh->disconnect();
$dbh = undef;


# Generate an HTML file

open(HTML, '>', $htmlfile) or die("could not open $htmlfile");
print HTML "<!DOCTYPE html>";
print HTML "<html>";
print HTML "<head><title>graphs</title></head>";
print HTML "<table border=\"1\" style=\"border: 1px solid gray; border-collapse: collapse\">";
my $i = 0;

# Order the graphs based on complexity; more complex graphs first
foreach my $key (sort { $graphs->{$b}->{size} <=> $graphs->{$a}->{size} } keys %{$graphs})
{
	my $value = $graphs->{$key};

	if ($i % 3 == 0)
	{
		print HTML "</tr>" if $i > 0;
		print HTML "<tr>";
	}

	print HTML "<td><a href=\"".$key.".svg\"><img width=\"300\" height=\"200\" src=\"".$key.".svg\" /></a></td>";

	++$i;
}
print HTML "</tr></table>";
print HTML "</html>";
close(HTML);
