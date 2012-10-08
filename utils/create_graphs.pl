#!/usr/bin/perl -w

use strict;
use warnings;
use DBI;
use DBD::Pg;

require PerFunctionGraphs;
require TableUsageGraphs;

# Debug the dot file format (writes .dot files in the graphs/ directory instead
# of rendering graphs).
my $dot_debug = 0;

sub table_usage_sort
{
	my ($a, $b, $tables) = @_;

	my $t;

	$t = $tables->{$a};
	my $arw = $t->{n_tup_ins} > 0 || $t->{n_tup_upd} > 0 || $t->{n_tup_del} > 0;
	$t = $tables->{$b};
	my $brw = $t->{n_tup_ins} > 0 || $t->{n_tup_upd} > 0 || $t->{n_tup_del} > 0;

	# read-write first
	return $brw <=> $arw || $a cmp $b;
}

sub generate_html_index_worker
{
	my ($htmlfile, $graphs, $subgraphs, $subgraph_parent, $table_usage_graphs) = @_;

	open(HTML, '>', $htmlfile) or die "could not open $htmlfile";
	print HTML "<!DOCTYPE html>\n";
	print HTML "<html>\n";
	if (defined $subgraph_parent)
	{
		my $parent_graphid = 't'.$subgraph_parent;
		my $parent_name = $graphs->{$parent_graphid}->{entryfunctionname};
		print HTML "<head><title>Subgraphs for $parent_name</title></head>\n";
	}
	else
	{
		print HTML "<head><title>Call graphs</title></head>\n";
	}

	print HTML "<table border=\"1\" style=\"border: 1px solid gray; border-collapse: collapse\">\n";

	# Order the graphs based on complexity; more complex graphs first
	foreach my $key (sort { $graphs->{$b}->{size} <=> $graphs->{$a}->{size} } keys %{$graphs})
	{
		my $value = $graphs->{$key};

		# there might be subgraphs with no calls at all
		next if $value->{totalcalls} == 0;

		next if !defined $subgraph_parent && $value->{issubgraph};
		next if defined $subgraph_parent && (!defined $value->{parentgraphentryfunctionoid} || $value->{parentgraphentryfunctionoid} != $subgraph_parent);

		my $subgraph_information = "";
		if (defined $subgraphs && exists $subgraphs->{$value->{entryfunctionoid}})
		{
			my $num_subgraphs = scalar @{$subgraphs->{$value->{entryfunctionoid}}};
			$subgraph_information = "(<a href=\"$value->{entryfunctionoid}.html\">$num_subgraphs subgraphs</a>)";
		}

		print HTML "<tr>\n";
		print HTML "<td rowspan=3 valign=\"top\"><a href=\"$key.svg\"><img width=\"500\" height=\"320\" src=\"".$key.".svg\" /></a></td>\n";
		print HTML "<td colspan=5><font size=\"+2\">$value->{'entryfunctionname'} $subgraph_information</font></td></tr>\n";
		print HTML "<tr><td>$value->{'totalcalls'} calls</td><td>$value->{'totaltime'} ms total</td><td>$value->{'avgtime'} ms average</td>\n";
		print HTML "<td>First call<br />$value->{'firstcall'}</td><td>Last call<br />$value->{'lastcall'}</td></tr>\n";

		if (defined $table_usage_graphs)
		{
			# see if we have table usage information available
			if (exists $table_usage_graphs->{$value->{entryfunctionoid}} &&
				scalar keys %{$table_usage_graphs->{$value->{entryfunctionoid}}->{tables}} > 0)
			{
				my $tables = $table_usage_graphs->{$value->{entryfunctionoid}}->{tables};

				print HTML "<tr><td colspan=5><table border=1 style=\"border: 1px solid lightgray; border-collapse: collapse\">\n";

				my $previousrw = undef;
				foreach my $tablekey (sort { table_usage_sort($a, $b, $tables) } keys %{$tables})
				{
					my $table = $tables->{$tablekey};

					my $currentrw = $table->{n_tup_ins} > 0 ||
									$table->{n_tup_upd} > 0 ||
									$table->{n_tup_del} > 0;
					if (!defined $previousrw || $previousrw != $currentrw)
					{
						$previousrw = $currentrw;
						my $label = $currentrw ? "read-write tables" : "read-only tables";
						print HTML "<tr><th>$label</th><th>seq_scan</th><th>seq_tup_read</th><th>idx_scan</th><th>idx_tup_read</th>\n";
						print HTML "<th>n_tup_ins</th><th>n_tup_upd</th><th>n_tup_del</th></tr>\n";
					}

					print HTML "<tr><td><a href=\"tableusage/r$tablekey.svg\">$table->{relname}</a></td><td>$table->{seq_scan}</td><td>$table->{seq_tup_read}</td>\n";
					print HTML "<td>$table->{idx_scan}</td><td>$table->{idx_tup_read}</td>\n";
					print HTML "<td>$table->{n_tup_ins}</td><td>$table->{n_tup_upd}</td><td>$table->{n_tup_del}</td></tr>\n";
				}
				print HTML "</table><br /><a href=\"tableusage/tlf$value->{entryfunctionoid}.svg\">View table information</a></td></tr>\n";
			}
			else
			{
				print HTML "<tr><td colspan=5>No table information available</td></tr>\n";
			}
		}
		else
		{
			print HTML "<tr><td colspan=5>&nbsp;</td></tr>\n";
		}

		print HTML "</tr>\n";
	}

	print HTML "</tr></table>\n";
	print HTML "</html>\n";
	close(HTML);
}

sub generate_html_index
{
	my ($htmlfile, $graphs, $subgraphs, $table_usage_graphs) = @_;

	generate_html_index_worker($htmlfile, $graphs, $subgraphs, undef, $table_usage_graphs);
}

sub generate_subgraph_html_index
{
	my ($htmlfile, $graphs, $subgraph_parent, $table_usage_graphs) = @_;

	generate_html_index_worker($htmlfile, $graphs, undef, $subgraph_parent, undef);
}

# small function for trimming configuration parameter input
sub trim
{
	my $var = shift @_;
	$var =~ s/^\s+|\s+$//g;
	return $var;
}

sub parse_boolean
{
	my ($params, $param_name, $default_value) = @_;

	$params->{$param_name} = $default_value if !defined $params->{$param_name};

	my $value = trim($params->{$param_name});

	if ($value eq 'yes' || $value eq '1' || $value eq 'true')
		{ $params->{$param_name} = 1; }
	elsif ($value eq 'no' || $value eq '0' || $value eq 'false')
		{ $params->{$param_name} = 0; } 
	else
		{ die "Unrecognized input \"$value\" for $param_name"; }
}

# XXX At some point it might make sense to use a real parser (Config::IniFiles
# for example), but right now this works just as well and avoids introducing
# new dependencies.
sub parse_config_file
{
	my ($filename, $params) = @_;

	my $parser = q{^\s*($|(;.*)|((\w+)\s*=([^;]+)(;.*)?))\s*$};

	open(CONFFILE, $filename) or die "Could not open config file $filename";
	while (my $line = <CONFFILE>)
	{
		chomp($line);
		die "Syntax error on line $. in $filename\n" if ($line !~ $parser);

		next if (!defined $4);
		
		# remember to check for SubGraphs separately
		die "Unrecognized configuration parameter \"$4\"\n" if ($4 ne 'SubGraphs' && !exists $params->{$4});
		$params->{$4} = $5;
	}


	# XXX Here we have to special case some configuration parameters that have a
	# special data type.  It's not yet too ugly, but slowly going that way..

	parse_boolean($params, "GeneratePerFunctionGraphs", "true");
	parse_boolean($params, "GenerateTableUsageGraphs", "false");

	# The user can specify a list of functions which are then separated from the
	# actual graph they would otherwise be in, creating separate subgraphs.
	if (defined $params->{SubGraphs})
	{
		my @subgraph_list = split(",", $params->{SubGraphs});
		# trim() the elements
		my @trimmed = map { trim($_) } @subgraph_list;
		$params->{SubGraphs} = \@trimmed;
	}
	else
	{
		$params->{SubGraphs} = [];
	}
}

sub create_directory
{
	my $dir = shift @_;
	die "could not create directory $dir: $!" if !-d $dir && !mkdir $dir;
}


#
# MAIN PROGRAM STARTS HERE
#


# check that GraphViz is installed
`which dot`;
die "Could not execute shell command: $!\n" if $? == -1;
die "\"dot\" not found in your PATH.  This tool requires an existing installation of GraphViz.\n" if $? != 0;


if (@ARGV != 3)
{
	print "Usage: ./create_graphs.pl graphdir configfile dbname\n";
	exit;
}

my $graphdir = $ARGV[0];
my $per_function_graphdir = $graphdir . "/perfunction";
my $table_usage_graphdir = $graphdir . "/tableusage";

my $config_file = $ARGV[1];
my $dbname = $ARGV[2];

# default params
my %params =
(
	# set the params with special data types to "undef" and let parse_config_file()
	# deal with them
	SubGraphs					=>		undef,
	GeneratePerFunctionGraphs	=>		undef,
	GenerateTableUsageGraphs	=>		undef,

	PgProcReplacement			=>		"\"pg_proc\"",
	PgConstraintReplacement		=>		"\"pg_constraint\"",
	PgClassReplacement			=>		"\"pg_class\"",
	PgNamespaceReplacement		=>		"\"pg_namespace\"",

	EdgeColor					=>		"'black'",
	EdgeStyle					=>		"'solid'",
	EdgePenWidth				=>		1.0,

	NodeLabel					=>		"FunctionName",
	NodeShape					=>		"'ellipse'",
	NodeHref					=>		"NULL",
	NodeColor					=>		"'black'",
	NodeStyle					=>		"'solid'",
	NodePenWidth				=>		1.0
);

parse_config_file($config_file, \%params);

create_directory($graphdir);
create_directory($per_function_graphdir) if ($params{GeneratePerFunctionGraphs});
create_directory($table_usage_graphdir) if ($params{GenerateTableUsageGraphs});

my $system_catalogs = { pg_proc			=> $params{PgProcReplacement},
						pg_constraint	=> $params{PgConstraintReplacement},
						pg_class		=> $params{PgClassReplacement},
						pg_namespace	=> $params{PgNamespaceReplacement} };

my $subgraph_params_query = 
<<"SQL";
CREATE TEMPORARY TABLE SubGraphParams
	(TopLevelFunction, EntryFunction, SubGraphID)
ON COMMIT DROP
AS
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
	$system_catalogs->{pg_proc} tlf
		ON (tlf.proname = SubGraphs.TopLevelFunction)
JOIN
	$system_catalogs->{pg_proc} ef
		ON (ef.proname = SubGraphs.EntryFunction)
GROUP BY tlf.proname, ef.proname
;
SQL
;

my $edges_query =
<<"SQL";
CREATE TEMPORARY TABLE Edges
(
	EdgeType,
	GraphID,
	TopLevelFunction,
	EdgeID,
	CallGraphID,
	Caller,
	Callee,
	Calls,
	TotalTime,
	SelfTime
)
ON COMMIT DROP
AS
WITH RECURSIVE
SubGraphEdgeWorkTable (EdgeID, CallGraphID, SubGraphID, Callee, SeenEdges, ShouldStop) AS (
	-- Recursively make a list of edges that are part of any subgraphs.  We need to keep a list of
	-- edges we've already visited to avoid looping infinitely in case there are loops in the call
	-- graphs.
	--
	-- Also we need to make sure that we don't claim that an edge is part of two different
	-- subgraphs; that's handled using ShouldStop.

	SELECT
		EdgeID, Edges.CallGraphID, SubGraphParams.SubGraphID, Edges.Callee, ARRAY[EdgeID],
		(CallGraphs.TopLevelFunction, Edges.Callee) IN (SELECT TopLevelFunction, EntryFunction FROM SubGraphParams)
	FROM
		call_graph.Edges
	JOIN
		call_graph.CallGraphs
			ON (CallGraphs.CallGraphID = Edges.CallGraphID)
	JOIN
		SubGraphParams ON (SubGraphParams.TopLevelFunction = CallGraphs.TopLevelFunction AND SubGraphParams.EntryFunction = Edges.Caller)
	WHERE
		(CallGraphs.TopLevelFunction, Edges.Caller) IN (SELECT TopLevelFunction, EntryFunction FROM SubGraphParams)

	UNION ALL

	SELECT
		Edges.EdgeID, Edges.CallGraphID, SubGraphEdgeWorkTable.SubGraphID, Edges.Callee, SeenEdges || Edges.EdgeID,
		-- stop traversing this path if we hit an edge belonging to another subgraph
		(CallGraphs.TopLevelFunction, Edges.Callee) IN (SELECT TopLevelFunction, EntryFunction FROM SubGraphParams) OR
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
)
-- Create a processed list of edges, marking which subgraph they are part of (if any)
SELECT
	CASE WHEN SubGraphEdges.EdgeID IS NULL THEN 'e' ELSE 's' END AS EdgeType,
	COALESCE('s'||SubGraphEdges.SubGraphID, 't'||TopLevelFunction::text) AS GraphID,
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
SQL
;

my $edgedata_query = 
<<"SQL";
SELECT
	GraphID,
	'edge'::text AS ElementType,
	EdgeFrom,
	EdgeTo,
	NULL::text AS NodeID,
	NULL::text AS NodeLabel,
	NULL::text AS NodeShape,
	NULL::text AS NodeHref,
	$params{EdgeColor} AS Color,
	$params{EdgeStyle} AS Style,
	$params{EdgePenWidth} AS PenWidth
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
	$params{NodeLabel} AS NodeLabel,
	$params{NodeShape} AS NodeShape,
	$params{NodeHref} AS NodeHref,
	$params{NodeColor} AS Color,
	$params{NodeStyle} AS Style,
	$params{NodePenWidth} AS PenWidth
FROM
(
	SELECT
		GraphID,
		GraphID||'e'||Callee AS NodeID,
		proclookup.proname AS FunctionName,
		proclookup.oid AS FunctionOid,
		(TopLevelFunction, Callee) IN (SELECT TopLevelFunction, EntryFunction FROM SubGraphParams) AS NodeIsSubGraphEntryFunction,
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
			EntryFunction,
			TopLevelFunction,
			TRUE AS NodeIsGraphEntryFunction
		FROM
			SubGraphParams
	) Edges
	JOIN
		$system_catalogs->{pg_proc} proclookup
			ON (proclookup.oid = Edges.Callee)
	GROUP BY
		GraphID, TopLevelFunction, Callee, proclookup.proname, proclookup.oid, NodeIsGraphEntryFunction
) ss

ORDER BY
	GraphID, NodeID, EdgeFrom, EdgeTo
SQL
;

my $statistics_query =
<<"SQL";
SELECT
	GraphID, EntryFunctionOid, EntryFunctionName, IsSubGraph,
	ParentGraphEntryFunctionOid, ParentGraphEntryFunctionName,
	to_char(FirstCall, 'YYYY-MM-DD HH24:MI:SS') AS FirstCall,
	to_char(LastCall, 'YYYY-MM-DD HH24:MI:SS') AS LastCall, TotalCalls, TotalTime, AvgTime
FROM
(
	SELECT
		GraphID, EntryFunctionOid, EntryFunctionName, IsSubGraph,
		ParentGraphEntryFunctionOid, ParentGraphEntryFunctionName,
		min(FirstCall) AS FirstCall, max(LastCall) AS LastCall,
		sum(Calls) AS TotalCalls, round(sum(TotalTime)::numeric, 2) AS TotalTime,
		round((sum(TotalTime) / sum(Calls))::numeric, 2) AS AvgTime
	FROM
	(
	    SELECT
			GraphID, proclookup.oid AS EntryFunctionOid, proclookup.proname AS EntryFunctionName,
			FALSE AS IsSubGraph, NULL AS ParentGraphEntryFunctionOid,
			NULL AS ParentGraphEntryFunctionName,
			Edges.Calls, Edges.TotalTime, FirstCall, LastCall
		FROM
			Edges
		JOIN
			$system_catalogs->{pg_proc} proclookup
				ON (proclookup.oid = Edges.Callee)
		JOIN
			call_graph.CallGraphs cg
				ON (cg.CallGraphID = Edges.CallGraphID)
		WHERE
			Caller = 0

		UNION ALL

		SELECT
			's' || sgp.SubGraphID AS GraphID,
			proclookup.oid AS EntryFunctionOid, proclookup.proname AS EntryFunctionName,
			TRUE AS IsSubGraph, sgp.TopLevelFunction AS ParentGraphEntryFunctionOid,
			parentproclookup.proname AS ParentGraphEntryFunctionName,
			Calls, TotalTime,
			-- not available for subgraphs
			NULL AS FirstCall, NULL AS LastCall
		FROM
			Edges e
		JOIN
			SubGraphParams sgp
				ON (e.TopLevelFunction = sgp.TopLevelFunction and e.Callee = sgp.EntryFunction)
		JOIN
			$system_catalogs->{pg_proc} proclookup
				ON (proclookup.oid = sgp.EntryFunction)
		JOIN
			$system_catalogs->{pg_proc} parentproclookup
				ON (parentproclookup.oid = sgp.TopLevelFunction)
	) EntryEdges
	GROUP BY
		GraphID, EntryFunctionOid, EntryFunctionName,
		IsSubGraph, ParentGraphEntryFunctionOid,
		ParentGraphEntryFunctionName
) ss
SQL
;




my $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "", {RaiseError => 1, PrintError => 0});

$dbh->begin_work();

if ($params{GeneratePerFunctionGraphs})
{
	PerFunctionGraphs::generate_per_function_graphs($per_function_graphdir, $dbh, $system_catalogs);
}

my $table_usage_graphs = undef;
if ($params{GenerateTableUsageGraphs})
{
	$table_usage_graphs = TableUsageGraphs::generate_table_usage_graphs($table_usage_graphdir, $dbh, $system_catalogs);
}

my $sth = $dbh->prepare($subgraph_params_query);
$sth->execute($params{SubGraphs});

$sth = $dbh->prepare($edges_query);
$sth->execute();

$sth = $dbh->prepare($edgedata_query);
$sth->execute();

if ($sth->rows <= 0)
{
	# shouldn't happen
	die "the SQL query returned no rows -- maybe you forgot to run ProcessCallGraphBuffers()?";
}

# Go through all the dot formatted lines one at a time.  Here we can assume that
# the result set is ordered by GraphID, so we can write an entire file at once.
#
# The loop below is a bit complex to avoid duplicating the code for opening and
# closing a file handle.
#

my $graphs = {};
for (my $graph = undef, my $pipe;;)
{
	my $row = $sth->fetchrow_hashref;

	# If the previous row was the last row in the result set or the last row of that
	# particular graph, close the pipe and make sure the "dot" command succeeded.
	if (!defined($row) ||
		(defined $graph && $graph ne $row->{graphid}))
	{
		print $pipe "}\n";
		close($pipe);

		# Check the exit code of the program
		die "dot failed" if $? != 0;

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

		$graphs->{$graph} = { size => 0,
							  totalcalls => 0,
							  totaltime => 0,
							  avgtime => 0,
							  entryfunctionname => 'unknown',
							  firstcall => 'unknown',
							  lastcall => 'unknown' };

		# If $dot_debug is set, write .dot files.  if not, pipe the output to dot
		if ($dot_debug)
		{
			my $filename = "$graphdir/$graph.dot";
			open($pipe, ">", $filename) or die "could not open file $filename";
		}
		else
		{
			open($pipe, "| dot -Tsvg -o $graphdir/$graph.svg") or die "could not fork";
		}

		print $pipe "digraph graph1 {\n";
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
		die "unknown element type $row->{elementtype}\n";
	}

	print $pipe $data."\n";
}

$sth = $dbh->prepare($statistics_query);
$sth->execute();

while (my $row = $sth->fetchrow_hashref)
{
	my $graph = $row->{'graphid'};

	# We skip graphs that have nothing more than the top level function, so
	# this is not an error condition.
	next if (!defined $graphs->{$graph});

	$graphs->{$graph}->{'totalcalls'} = $row->{'totalcalls'};
	$graphs->{$graph}->{'totaltime'} = $row->{'totaltime'};
	$graphs->{$graph}->{'avgtime'} = $row->{'avgtime'};
	$graphs->{$graph}->{'entryfunctionname'} = $row->{'entryfunctionname'};
	$graphs->{$graph}->{'entryfunctionoid'} = $row->{'entryfunctionoid'};
	$graphs->{$graph}->{'issubgraph'} = $row->{'issubgraph'};
	$graphs->{$graph}->{'parentgraphentryfunctionoid'} = $row->{'parentgraphentryfunctionoid'};
	$graphs->{$graph}->{'parentgraphentryfunctionname'} = $row->{'parentgraphentryfuntionname'};

	# use the default value set previously if the values are not known
	$graphs->{$graph}->{'firstcall'} = $row->{'firstcall'} if defined $row->{'firstcall'};
	$graphs->{$graph}->{'lastcall'} = $row->{'lastcall'} if defined $row->{'lastcall'};
}

$dbh->commit();
$dbh->disconnect();
$dbh = undef;


# Create a list of subgraphs, but only include the ones with calls > 0
my $subgraphs = {};
foreach my $graphid (keys %{$graphs})
{
	my $graph = $graphs->{$graphid};
	next if (!$graph->{issubgraph});	
	next if ($graph->{totalcalls} == 0);

	my $parent = $graph->{parentgraphentryfunctionoid};
	$subgraphs->{$parent} = [] if (!exists $subgraphs->{$parent});
	
	push @{$subgraphs->{$parent}}, $graphid;
}

# Generate the HTML index

generate_html_index("$graphdir/index.html", $graphs, $subgraphs, $table_usage_graphs);

foreach my $subgraph_parent (keys %{$subgraphs})
{
	generate_subgraph_html_index("$graphdir/$subgraph_parent.html", $graphs, $subgraph_parent, $table_usage_graphs);
}
